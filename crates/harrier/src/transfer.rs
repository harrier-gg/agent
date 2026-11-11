//! Transfer channel for uploads and downloads (ADR 0015). For each transfer the agent opens a
//! second WebSocket to `<panel>/agent/transfer/<id>`, authenticated with its token like the main
//! connection, and streams binary frames. It closes normally when done, or with code 1011 and a
//! reason the panel shows.

use crate::files;
use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

type Channel = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Frames of 64 KiB: large enough to be fast, small enough to stay below every message limit.
const CHUNK: usize = 64 * 1024;
/// The gateway sends this text frame after the last bytes of an upload.
const UPLOAD_DONE: &str = "done";

/// `ws://host/agent/ws` → `ws://host/agent/transfer/<id>`.
pub fn url(panel: &str, transfer_id: &str) -> String {
    let base = panel.trim_end_matches('/');
    let base = base.strip_suffix("/ws").unwrap_or(base);
    format!("{base}/transfer/{transfer_id}")
}

async fn connect(url: &str, token: &str) -> anyhow::Result<Channel> {
    let mut request = url.into_client_request()?;
    let mut bearer = HeaderValue::from_str(&format!("Bearer {token}"))?;
    bearer.set_sensitive(true);
    request.headers_mut().insert(AUTHORIZATION, bearer);
    let (channel, _) = connect_async(request)
        .await
        .with_context(|| format!("Opening the transfer channel {url}"))?;
    Ok(channel)
}

/// Ends the transfer successfully. The code must be explicit: a close without one reaches the
/// panel as 1005 ("no status"), not as success.
async fn succeed(channel: &mut Channel) -> anyhow::Result<()> {
    let frame = CloseFrame {
        code: CloseCode::Normal,
        reason: "".into(),
    };
    channel.close(Some(frame)).await?;
    Ok(())
}

/// Ends the transfer with an error the panel shows (close reasons are at most 123 bytes).
async fn fail(channel: &mut Channel, reason: &str) {
    let mut end = reason.len().min(120);
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    let frame = CloseFrame {
        code: CloseCode::Error,
        reason: reason[..end].to_string().into(),
    };
    let _ = channel.close(Some(frame)).await;
}

/// Streams a file of the server to the panel.
pub async fn download(url: &str, token: &str, root: PathBuf, path: String) -> anyhow::Result<()> {
    let mut channel = connect(url, token).await?;
    let opened =
        tokio::task::spawn_blocking(move || files::open_for_download(&root, &path)).await?;
    let file = match opened {
        Ok((file, _size)) => file,
        Err(reason) => {
            fail(&mut channel, &reason).await;
            return Ok(());
        }
    };
    let mut file = tokio::fs::File::from_std(file);
    let mut buffer = vec![0; CHUNK];
    loop {
        let read = match file.read(&mut buffer).await {
            Ok(read) => read,
            Err(e) => {
                fail(&mut channel, &format!("Reading: {e}")).await;
                return Ok(());
            }
        };
        if read == 0 {
            break;
        }
        channel
            .send(Message::binary(buffer[..read].to_vec()))
            .await?;
    }
    succeed(&mut channel).await?;
    Ok(())
}

/// Receives a file from the panel and puts it at `path` once it is complete.
pub async fn upload(url: &str, token: &str, root: PathBuf, path: String) -> anyhow::Result<()> {
    let mut channel = connect(url, token).await?;
    let started = tokio::task::spawn_blocking(move || files::start_upload(&root, &path)).await?;
    let upload = match started {
        Ok(upload) => upload,
        Err(reason) => {
            fail(&mut channel, &reason).await;
            return Ok(());
        }
    };
    let mut file = tokio::fs::File::from_std(upload.file.try_clone()?);
    let mut complete = false;
    while let Some(message) = channel.next().await {
        match message {
            Ok(Message::Binary(bytes)) => {
                if let Err(e) = file.write_all(&bytes).await {
                    upload.abort();
                    fail(&mut channel, &format!("Writing: {e}")).await;
                    return Ok(());
                }
            }
            Ok(Message::Text(text)) if text.as_str() == UPLOAD_DONE => {
                complete = true;
                break;
            }
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(_) => {}
        }
    }
    if !complete {
        upload.abort(); // the browser went away; nothing half-written stays
        return Ok(());
    }
    file.flush().await?;
    drop(file);
    match tokio::task::spawn_blocking(move || upload.finish()).await? {
        Ok(()) => succeed(&mut channel).await?,
        Err(reason) => fail(&mut channel, &reason).await,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_transfer_url_sits_next_to_the_agent_connection() {
        assert_eq!(
            url("ws://localhost:8080/agent/ws", "abc"),
            "ws://localhost:8080/agent/transfer/abc"
        );
        assert_eq!(
            url("wss://panel.example/agent/ws/", "abc"),
            "wss://panel.example/agent/transfer/abc"
        );
    }
}
