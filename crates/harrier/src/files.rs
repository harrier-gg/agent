//! Reads and writes text files inside a server's data directory (ADR 0011). The agent checks
//! every path itself, so even a compromised panel cannot reach files outside that directory.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Component, Path, PathBuf};

/// Fits one agent message (256 KiB in the gateway) even with JSON escaping.
const MAX_BYTES: u64 = 128 * 1024;

/// Turns a relative path into one inside `root`. Rejects absolute paths, `..`, and symlinks in
/// any existing part of the path; `O_NOFOLLOW` covers the file itself when it is opened.
// ponytail: checking the directories before opening leaves a short race for nested paths (a
// game process could swap a directory for a symlink in between); the file manager (M6) should
// open each component relative to its parent (openat / cap-std).
fn resolve(root: &Path, path: &str) -> Result<PathBuf, String> {
    let relative = Path::new(path);
    if path.is_empty()
        || !relative
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
    {
        return Err(format!("Invalid path \"{path}\""));
    }
    let mut full = root.to_path_buf();
    for component in relative.components() {
        full.push(component);
        if std::fs::symlink_metadata(&full).is_ok_and(|m| m.file_type().is_symlink()) {
            return Err(format!("\"{path}\" is a symlink"));
        }
    }
    Ok(full)
}

/// The file's text, or `None` if it does not exist.
pub fn read(root: &Path, path: &str) -> Result<Option<String>, String> {
    let full = resolve(root, path)?;
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&full)
    {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
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
    let full = resolve(root, path)?;
    let parent = full.parent().unwrap_or(root);
    std::fs::create_dir_all(parent).map_err(|e| format!("Creating the directory: {e}"))?;
    if full.is_dir() {
        return Err(format!("\"{path}\" is a directory"));
    }
    let name = full.file_name().unwrap_or_default().to_string_lossy();
    let temp = parent.join(format!(".{name}.harrier-tmp"));
    let _ = std::fs::remove_file(&temp);
    let mut file: File = OpenOptions::new()
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temp)
        .map_err(|e| format!("Writing \"{path}\": {e}"))?;
    file.write_all(content.as_bytes())
        .and_then(|()| file.sync_all())
        .and_then(|()| std::fs::rename(&temp, &full))
        .map_err(|e| {
            let _ = std::fs::remove_file(&temp);
            format!("Writing \"{path}\": {e}")
        })
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(write(&root, "link", "y").is_err());
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
