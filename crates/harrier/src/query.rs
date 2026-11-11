//! Asks running game servers who is online (ADR 0009). Query protocols are network protocols,
//! not games: the panel says which one a server speaks.

use anyhow::{Context, bail, ensure};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const TIMEOUT: Duration = Duration::from_secs(3);
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

#[derive(Debug, PartialEq)]
pub struct Players {
    pub online: u32,
    pub max: u32,
    pub names: Vec<String>,
}

/// Minecraft: Java Edition Server List Ping on `127.0.0.1:<port>`
/// (<https://minecraft.wiki/w/Java_Edition_protocol/Server_List_Ping>).
pub async fn minecraft(port: u16) -> anyhow::Result<Players> {
    tokio::time::timeout(TIMEOUT, minecraft_ping(port))
        .await
        .context("no answer")?
}

async fn minecraft_ping(port: u16) -> anyhow::Result<Players> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;

    // Handshake: packet 0, protocol version -1 ("any"), address, port, next state 1 (status).
    let mut handshake = vec![0x00];
    write_varint(u32::MAX, &mut handshake);
    write_varint(9, &mut handshake);
    handshake.extend(b"localhost");
    handshake.extend(port.to_be_bytes());
    write_varint(1, &mut handshake);

    let mut request = Vec::new();
    write_varint(handshake.len() as u32, &mut request);
    request.extend(handshake);
    request.extend([0x01, 0x00]); // status request: length 1, packet 0
    stream.write_all(&request).await?;

    let length = read_varint(&mut stream).await? as usize;
    ensure!(length <= MAX_RESPONSE_BYTES, "status reply too large");
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await?;

    let mut body = body.as_slice();
    ensure!(read_varint(&mut body).await? == 0, "unexpected packet");
    let json_length = read_varint(&mut body).await? as usize;
    parse_minecraft_status(body.get(..json_length).context("truncated status reply")?)
}

fn parse_minecraft_status(json: &[u8]) -> anyhow::Result<Players> {
    let status: serde_json::Value = serde_json::from_slice(json)?;
    let players = &status["players"];
    let count = |key: &str| {
        players[key]
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .with_context(|| format!("players.{key} missing"))
    };
    // Servers can fill the sample with arbitrary text (or hide players as "Anonymous Player");
    // only real player names are kept.
    let names = players["sample"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|p| p["name"].as_str())
        .filter(|n| {
            (3..=16).contains(&n.len()) && n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        })
        .take(32)
        .map(String::from)
        .collect();
    Ok(Players {
        online: count("online")?,
        max: count("max")?,
        names,
    })
}

pub(crate) fn write_varint(mut value: u32, out: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

pub(crate) async fn read_varint(input: &mut (impl AsyncRead + Unpin)) -> anyhow::Result<u32> {
    let mut value = 0u32;
    for i in 0..5 {
        let byte = input.read_u8().await?;
        value |= u32::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    bail!("varint too long")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn varints_round_trip() {
        for value in [0, 1, 127, 128, 25565, u32::MAX] {
            let mut bytes = Vec::new();
            write_varint(value, &mut bytes);
            assert_eq!(read_varint(&mut bytes.as_slice()).await.unwrap(), value);
        }
    }

    #[test]
    fn status_keeps_only_real_player_names() {
        let players = parse_minecraft_status(
            br#"{"players":{"online":3,"max":20,"sample":[
                {"name":"Anil","id":"1"},{"name":"\u00a7cHello world","id":"2"},{"name":"lea_2","id":"3"}
            ]}}"#,
        )
        .unwrap();
        assert_eq!(
            players,
            Players {
                online: 3,
                max: 20,
                names: vec!["Anil".into(), "lea_2".into()]
            }
        );
    }

    /// A fake Minecraft server answering one status request, like a real one.
    #[tokio::test]
    async fn pings_a_server() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            let _ = socket.read(&mut buf).await.unwrap();
            let json = br#"{"version":{"name":"1.21"},"players":{"online":1,"max":10,"sample":[{"name":"Anil","id":"x"}]}}"#;
            let mut body = vec![0x00];
            write_varint(json.len() as u32, &mut body);
            body.extend(json);
            let mut reply = Vec::new();
            write_varint(body.len() as u32, &mut reply);
            reply.extend(body);
            socket.write_all(&reply).await.unwrap();
        });
        let players = minecraft(port).await.unwrap();
        assert_eq!((players.online, players.max), (1, 10));
        assert_eq!(players.names, vec!["Anil".to_string()]);
    }
}
