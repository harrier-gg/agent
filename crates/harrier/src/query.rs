//! Asks running game servers who is online (ADR 0009). Query protocols are network protocols,
//! not games: the panel says which one a server speaks.

use anyhow::{Context, bail, ensure};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};

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

/// Steam's A2S_INFO and A2S_PLAYER on `127.0.0.1:<port>`
/// (<https://developer.valvesoftware.com/wiki/Server_queries>). The count comes from A2S_INFO;
/// names are left out if A2S_PLAYER fails or sends empty ones (Valheim does).
pub async fn a2s(port: u16) -> anyhow::Result<Players> {
    tokio::time::timeout(TIMEOUT, async {
        let socket = UdpSocket::bind("127.0.0.1:0").await?;
        socket.connect(("127.0.0.1", port)).await?;
        let info = a2s_exchange(&socket, 0x54, b"Source Engine Query\0", None, 0x49).await?;
        let (online, max) = parse_a2s_info(&info)?;
        let names = match a2s_exchange(&socket, 0x55, b"", Some([0xff; 4]), 0x44).await {
            Ok(reply) => parse_a2s_players(&reply)?,
            Err(_) => Vec::new(),
        };
        Ok(Players { online, max, names })
    })
    .await
    .context("no answer")?
}

/// Sends a request and answers one challenge (`0x41`) by sending it again with the challenge.
/// Returns the reply after its type byte, which must be `expect`.
async fn a2s_exchange(
    socket: &UdpSocket,
    kind: u8,
    payload: &[u8],
    mut challenge: Option<[u8; 4]>,
    expect: u8,
) -> anyhow::Result<Vec<u8>> {
    for _ in 0..2 {
        let mut request = vec![0xff, 0xff, 0xff, 0xff, kind];
        request.extend(payload);
        request.extend(challenge.iter().flatten());
        socket.send(&request).await?;
        let mut reply = vec![0; 65_535];
        let length = socket.recv(&mut reply).await?;
        reply.truncate(length);
        // ponytail: split replies (0xFFFFFFFE) are not joined; they only occur beyond ~1400 bytes,
        // far more players than these servers hold.
        let body = reply
            .strip_prefix(&[0xff, 0xff, 0xff, 0xff])
            .context("not a single-packet reply")?;
        match body.split_first() {
            Some((0x41, rest)) => {
                challenge = Some(rest.get(..4).context("short challenge")?.try_into()?);
            }
            Some((&kind, rest)) if kind == expect => return Ok(rest.to_vec()),
            _ => bail!("unexpected reply"),
        }
    }
    bail!("challenge not accepted")
}

/// A2S_INFO reply: protocol, name, map, folder, game, app ID, players, max players, …
fn parse_a2s_info(mut reply: &[u8]) -> anyhow::Result<(u32, u32)> {
    take(&mut reply, 1)?;
    for _ in 0..4 {
        c_string(&mut reply)?;
    }
    take(&mut reply, 2)?;
    let counts = take(&mut reply, 2)?;
    Ok((counts[0].into(), counts[1].into()))
}

/// A2S_PLAYER reply: count, then index, name, score and duration per player.
fn parse_a2s_players(mut reply: &[u8]) -> anyhow::Result<Vec<String>> {
    let count = take(&mut reply, 1)?[0];
    let mut names = Vec::new();
    for _ in 0..count {
        take(&mut reply, 1)?;
        let name = c_string(&mut reply)?;
        take(&mut reply, 8)?;
        if !name.is_empty() {
            names.push(name);
        }
    }
    Ok(names)
}

fn take<'a>(input: &mut &'a [u8], n: usize) -> anyhow::Result<&'a [u8]> {
    ensure!(input.len() >= n, "truncated reply");
    let (head, rest) = input.split_at(n);
    *input = rest;
    Ok(head)
}

fn c_string(input: &mut &[u8]) -> anyhow::Result<String> {
    let end = input
        .iter()
        .position(|&b| b == 0)
        .context("truncated reply")?;
    let text = String::from_utf8_lossy(&input[..end]).into_owned();
    *input = &input[end + 1..];
    Ok(text)
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

    /// A fake Steam server that wants a challenge for both requests, with packets as Valheim
    /// sends them: two players online, one of them without a name.
    #[tokio::test]
    async fn asks_a_steam_server() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = server.local_addr().unwrap().port();
        tokio::spawn(async move {
            let header = [0xff, 0xff, 0xff, 0xff];
            let challenge = [0x01, 0x02, 0x03, 0x04];
            let mut info = header.to_vec();
            info.extend(b"\x49\x11My world\0\0valheim\0Valheim\0\0\0\x02\x0a\0dl\0\x01");
            let mut players = header.to_vec();
            players.extend(b"\x44\x02");
            players.extend(b"\0Anil\0\0\0\0\0\0\0\x80\x3f");
            players.extend(b"\x01\0\0\0\0\0\0\0\x80\x3f");
            let mut buf = [0u8; 1400];
            loop {
                let (n, from) = server.recv_from(&mut buf).await.unwrap();
                let request = &buf[..n];
                let reply = if request.ends_with(&challenge) {
                    if request[4] == 0x54 { &info } else { &players }
                } else {
                    let mut c = header.to_vec();
                    c.push(0x41);
                    c.extend(challenge);
                    server.send_to(&c, from).await.unwrap();
                    continue;
                };
                server.send_to(reply, from).await.unwrap();
            }
        });
        let players = a2s(port).await.unwrap();
        assert_eq!(
            players,
            Players {
                online: 2,
                max: 10,
                names: vec!["Anil".into()]
            }
        );
    }
}
