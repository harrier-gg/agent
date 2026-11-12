//! Downloads files from the internet into a server's data directory, e.g. mods (ADR 0017). HTTPS
//! only; the file is moved into place only if size and hash match, otherwise nothing changes.

use crate::files;
use std::io::{Read, Write};
use std::path::Path;

/// Larger files are refused: mods and plugins are megabytes, not gigabytes.
const MAX_BYTES: u64 = 512 * 1024 * 1024;

/// What the file must be.
pub struct Expected<'a> {
    pub size: Option<u64>,
    pub sha512: Option<&'a str>,
    pub sha1: Option<&'a str>,
    pub sha256: Option<&'a str>,
}

/// Downloads `url` to `path` inside `root`; returns the size.
pub fn download(root: &Path, path: &str, url: &str, expected: &Expected) -> Result<u64, String> {
    if !url.starts_with("https://") {
        return Err("Only HTTPS downloads are allowed".into());
    }
    if expected.sha512.is_none() && expected.sha1.is_none() && expected.sha256.is_none() {
        return Err("A download needs a hash".into());
    }
    let limit = expected.size.unwrap_or(MAX_BYTES).min(MAX_BYTES);
    let response = ureq::get(url)
        .header(
            "user-agent",
            concat!("harrier-agent/", env!("CARGO_PKG_VERSION")),
        )
        .call()
        .map_err(|e| format!("Downloading {url}: {e}"))?;
    let mut body = response.into_body().into_reader();

    let mut upload = files::start_upload(root, path)?;
    // Two crates with different `Digest` trait versions: each through its own.
    let mut sha512 = <sha2::Sha512 as sha2::Digest>::new();
    let mut sha1 = <sha1::Sha1 as sha1::Digest>::new();
    let mut sha256 = <sha2::Sha256 as sha2::Digest>::new();
    let mut size = 0u64;
    let mut buffer = vec![0; 64 * 1024];
    let copied = loop {
        let read = match body.read(&mut buffer) {
            Ok(0) => break Ok(()),
            Ok(read) => read,
            Err(e) => break Err(format!("Downloading {url}: {e}")),
        };
        size += read as u64;
        if size > limit {
            break Err(format!("{path} is larger than expected"));
        }
        let chunk = &buffer[..read];
        sha2::Digest::update(&mut sha512, chunk);
        sha1::Digest::update(&mut sha1, chunk);
        sha2::Digest::update(&mut sha256, chunk);
        if let Err(e) = upload.file.write_all(chunk) {
            break Err(format!("Writing {path}: {e}"));
        }
    };
    let checked = copied.and_then(|()| {
        if expected.size.is_some_and(|s| s != size) {
            return Err(format!(
                "{path} has {size} bytes, expected {}",
                expected.size.unwrap_or(0)
            ));
        }
        let matches = |expected: Option<&str>, actual: String| {
            expected.is_none_or(|e| e.eq_ignore_ascii_case(&actual))
        };
        if !matches(expected.sha512, hex::encode(sha2::Digest::finalize(sha512)))
            || !matches(expected.sha1, hex::encode(sha1::Digest::finalize(sha1)))
            || !matches(expected.sha256, hex::encode(sha2::Digest::finalize(sha256)))
        {
            return Err(format!(
                "{path} does not match its hash; it was not installed"
            ));
        }
        Ok(())
    });
    match checked {
        Ok(()) => upload.finish().map(|()| size),
        Err(e) => {
            upload.abort();
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_plain_http_and_unhashed_downloads() {
        let root = std::env::temp_dir().join(format!("harrier-download-{}", std::process::id()));
        let any = Expected {
            size: None,
            sha512: Some("00"),
            sha1: None,
            sha256: None,
        };
        assert!(
            download(&root, "mods/x.jar", "http://example.com/x.jar", &any)
                .unwrap_err()
                .contains("HTTPS")
        );
        let none = Expected {
            size: None,
            sha512: None,
            sha1: None,
            sha256: None,
        };
        assert!(
            download(&root, "mods/x.jar", "https://example.com/x.jar", &none)
                .unwrap_err()
                .contains("hash")
        );
        assert!(!root.join("mods/x.jar").exists());
    }
}
