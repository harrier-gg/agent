//! Reads and writes text files inside a server's data directory (ADR 0011). The agent checks
//! every path itself, so even a compromised panel cannot reach files outside that directory.
//!
//! All access goes through a `cap_std::fs::Dir` opened on that directory: every path component
//! is opened relative to its parent, so `..` and symlinks can never lead outside, not even if the
//! game swaps a directory for a symlink while the agent is working (no check-then-open race).

use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Component, Path};

/// Fits one agent message (256 KiB in the gateway) even with JSON escaping.
const MAX_BYTES: u64 = 128 * 1024;

/// Accepts only plain relative paths (`config/paper.yml`), so errors are clear before cap-std
/// would refuse them anyway.
fn relative(path: &str) -> Result<&Path, String> {
    let relative = Path::new(path);
    if path.is_empty()
        || !relative
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
    {
        return Err(format!("Invalid path \"{path}\""));
    }
    Ok(relative)
}

fn open_root(root: &Path) -> std::io::Result<Dir> {
    Dir::open_ambient_dir(root, ambient_authority())
}

/// The file's text, or `None` if it does not exist.
pub fn read(root: &Path, path: &str) -> Result<Option<String>, String> {
    let relative = relative(path)?;
    let opened = open_root(root).and_then(|dir| dir.open(relative));
    let file = match opened {
        Ok(file) => file,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("Opening \"{path}\": {e}")),
    };
    let meta = file.metadata().map_err(|e| e.to_string())?;
    if !meta.is_file() {
        return Err(format!("\"{path}\" is not a file"));
    }
    if meta.len() > MAX_BYTES {
        return Err(format!("\"{path}\" is larger than 128 KiB"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| format!("Reading \"{path}\": {e}"))?;
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| format!("\"{path}\" is not UTF-8 text"))
}

/// Replaces the file: writes a temporary file next to it and renames it, so the game never sees
/// a half-written file. Missing directories are created (the server may not have started yet).
pub fn write(root: &Path, path: &str, content: &str) -> Result<(), String> {
    if content.len() as u64 > MAX_BYTES {
        return Err("The content is larger than 128 KiB".into());
    }
    let relative = relative(path)?;
    let failed = |e: std::io::Error| format!("Writing \"{path}\": {e}");
    // The root itself comes from the agent's data directory and a checked server ID.
    std::fs::create_dir_all(root).map_err(failed)?;
    let dir = open_root(root).map_err(failed)?;
    let parent = relative.parent().unwrap_or(Path::new(""));
    if !parent.as_os_str().is_empty() {
        dir.create_dir_all(parent).map_err(failed)?;
    }
    if dir.metadata(relative).is_ok_and(|m| m.is_dir()) {
        return Err(format!("\"{path}\" is a directory"));
    }
    let name = relative.file_name().unwrap_or_default().to_string_lossy();
    let temp = parent.join(format!(".{name}.harrier-tmp"));
    let _ = dir.remove_file(&temp);
    let mut file = dir
        .open_with(&temp, OpenOptions::new().write(true).create_new(true))
        .map_err(failed)?;
    // rename replaces a symlink at the target itself; it never writes through it.
    file.write_all(content.as_bytes())
        .and_then(|()| file.sync_all())
        .and_then(|()| dir.rename(&temp, &dir, relative))
        .map_err(|e| {
            let _ = dir.remove_file(&temp);
            failed(e)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_root() -> PathBuf {
        let root = std::env::temp_dir().join(format!("harrier-files-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn reads_and_writes_inside_the_directory_only() {
        let root = temp_root();
        assert_eq!(read(&root, "server.properties"), Ok(None));
        write(&root, "server.properties", "motd=Hi\n").unwrap();
        write(&root, "config/paper.yml", "a: 1\n").unwrap();
        assert_eq!(
            read(&root, "server.properties").unwrap().as_deref(),
            Some("motd=Hi\n")
        );
        assert_eq!(
            read(&root, "config/paper.yml").unwrap().as_deref(),
            Some("a: 1\n")
        );

        for bad in [
            "",
            "../secret",
            "/etc/passwd",
            "config/../../x",
            "./server.properties",
        ] {
            assert!(read(&root, bad).is_err(), "{bad} must be rejected");
            assert!(write(&root, bad, "x").is_err(), "{bad} must be rejected");
        }

        // A symlink to a file outside, or to a directory outside, is never followed.
        let outside = root.with_extension("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("secret"), "x").unwrap();
        std::os::unix::fs::symlink(outside.join("secret"), root.join("link")).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("dir")).unwrap();
        assert!(read(&root, "link").is_err());
        // Writing replaces the link with a regular file inside; the target stays untouched.
        write(&root, "link", "y").unwrap();
        assert!(
            !std::fs::symlink_metadata(root.join("link"))
                .unwrap()
                .is_symlink()
        );
        assert!(read(&root, "dir/secret").is_err());
        assert!(write(&root, "dir/new", "y").is_err());
        assert_eq!(
            std::fs::read_to_string(outside.join("secret")).unwrap(),
            "x"
        );

        std::fs::write(root.join("binary"), [0xff, 0xfe]).unwrap();
        assert!(read(&root, "binary").is_err());
        assert!(write(&root, "big", &"x".repeat(MAX_BYTES as usize + 1)).is_err());

        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&outside);
    }
}
