//! Answers on a sleeping server's game port (ADR 0013): the server list shows that it sleeps,
//! and a login attempt wakes it. Wake protocols are network protocols, not games.

use crate::query::{read_varint, write_varint};
use anyhow::{bail, ensure};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

/// Every connection gets this long to say what it wants.
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
/// Handshake, status request and ping are tiny; anything bigger is not a Minecraft client.
const MAX_PACKET_BYTES: u32 = 1024;
/// Slow or idle connections are dropped, so they cannot pile up.
const MAX_CONNECTIONS: usize = 32;

/// Serves Minecraft: Java Edition clients until one tries to join, then returns.
pub async fn minecraft(listener: &TcpListener, status_text: &str, join_text: &str) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { continue };
                if connections.len() >= MAX_CONNECTIONS {
                    continue; // dropping the stream closes it
                }
                let (status, join) = (status_text.to_owned(), join_text.to_owned());
                connections.spawn(async move {
                    tokio::time::timeout(CONNECTION_TIMEOUT, answer(stream, &status, &join))
                        .await
                        .is_ok_and(|joined| joined.unwrap_or(false))
                });
            }
            Some(joined) = connections.join_next() => {
                if joined.unwrap_or(false) {
                    return;
                }
            }
        }
    }
}

/// Answers one connection. Returns whether the client tried to join.
async fn answer(mut stream: TcpStream, status_text: &str, join_text: &str) -> anyhow::Result<bool> {
    // Handshake: protocol version, address, port, next state (1 status, 2 login, 3 transfer).
    let handshake = read_packet(&mut stream).await?;
    let mut body = handshake.as_slice();
    ensure!(read_varint(&mut body).await? == 0, "not a handshake");
    let protocol = read_varint(&mut body).await?;
    let address = read_varint(&mut body).await? as usize;
    ensure!(
        address <= 255 && body.len() >= address + 2,
        "invalid handshake"
    );
    body = &body[address + 2..];
    let next = read_varint(&mut body).await?;

    match next {
        1 => {
            // Status request (packet 0), then usually a ping (packet 1) to measure latency.
            read_packet(&mut stream).await?;
            let status = serde_json::json!({
                // The client's own protocol, so the list does not show "incompatible version".
                "version": { "name": "Sleeping", "protocol": protocol },
                "players": { "max": 0, "online": 0 },
                "description": { "text": status_text },
            });
            send_string(&mut stream, 0x00, &status.to_string()).await?;
            if let Ok(ping) = read_packet(&mut stream).await {
                let mut pong = Vec::new();
                write_varint(ping.len() as u32, &mut pong);
                pong.extend(ping); // packet 1 with the same 8 bytes
                stream.write_all(&pong).await?;
            }
            Ok(false)
        }
        2 | 3 => {
            // Login: the client waits for an answer; disconnect it with a message (packet 0).
            let reason = serde_json::json!({ "text": join_text });
            send_string(&mut stream, 0x00, &reason.to_string()).await?;
            Ok(true)
        }
        _ => bail!("unknown next state {next}"),
    }
}

async fn read_packet(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    let length = read_varint(stream).await?;
    ensure!(
        (1..=MAX_PACKET_BYTES).contains(&length),
        "packet size {length}"
    );
    let mut packet = vec![0; length as usize];
    stream.read_exact(&mut packet).await?;
    Ok(packet)
}

/// Writes a packet that consists of one string, like status replies and disconnect reasons.
async fn send_string(stream: &mut TcpStream, id: u32, text: &str) -> anyhow::Result<()> {
    let mut body = Vec::new();
    write_varint(id, &mut body);
    write_varint(text.len() as u32, &mut body);
    body.extend(text.as_bytes());
    let mut packet = Vec::new();
    write_varint(body.len() as u32, &mut packet);
    packet.extend(body);
    stream.write_all(&packet).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn listener() -> (TcpListener, u16) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        (listener, port)
    }

    /// A handshake as a client sends it, with the given next state.
    fn handshake(port: u16, next: u32) -> Vec<u8> {
        let mut body = vec![0x00];
        write_varint(774, &mut body);
        write_varint(9, &mut body);
        body.extend(b"localhost");
        body.extend(port.to_be_bytes());
        write_varint(next, &mut body);
        let mut packet = Vec::new();
        write_varint(body.len() as u32, &mut packet);
        packet.extend(body);
        packet
    }

    #[tokio::test]
    async fn the_server_list_shows_the_sleep_text_and_only_a_login_wakes() {
        let (listener, port) = listener().await;
        let serve =
            tokio::spawn(async move { minecraft(&listener, "Sleeping", "Waking up").await });

        // The existing Server List Ping client reads our status reply.
        let players = crate::query::minecraft(port).await.unwrap();
        assert_eq!((players.online, players.max), (0, 0));
        let mut raw = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        raw.write_all(&handshake(port, 1)).await.unwrap();
        raw.write_all(&[0x01, 0x00]).await.unwrap();
        let mut reply = Vec::new();
        let _ = tokio::time::timeout(Duration::from_millis(300), raw.read_to_end(&mut reply)).await;
        assert!(String::from_utf8_lossy(&reply).contains(r#""text":"Sleeping""#));

        // Garbage and pings do not wake it.
        let mut junk = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        junk.write_all(&[0xfe, 0x01]).await.unwrap();
        drop(junk);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!serve.is_finished());

        // A login attempt gets the join text and wakes the server.
        let mut join = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        join.write_all(&handshake(port, 2)).await.unwrap();
        let mut reply = Vec::new();
        join.read_to_end(&mut reply).await.unwrap();
        assert!(String::from_utf8_lossy(&reply).contains("Waking up"));
        tokio::time::timeout(Duration::from_secs(1), serve)
            .await
            .expect("a login wakes the server")
            .unwrap();
    }
}
