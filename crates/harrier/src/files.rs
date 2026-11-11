//! Files inside a server's data directory: text files (ADR 0011) and the file manager (ADR 0015).
//! The agent checks every path itself, so even a compromised panel cannot reach files outside
//! that directory.
//!
//! All access goes through a `cap_std::fs::Dir` opened on that directory: every path component
//! is opened relative to its parent, so `..` and symlinks can never lead outside, not even if the
//! game swaps a directory for a symlink while the agent is working (no check-then-open race).

use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};
use harrier_protocol::{EntryKind, FileEntry};
use std::io::{ErrorKind, Read, Write};
use std::path::{Component, Path, PathBuf};

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

/// Suffix of files being written; they are hidden from listings.
const TEMP_SUFFIX: &str = ".harrier-tmp";

/// A directory's entries, directories first, then by name. `""` lists the data directory itself,
/// which may not exist before the first start (then it is empty).
pub fn list(root: &Path, path: &str) -> Result<Vec<FileEntry>, String> {
    let failed = |e: std::io::Error| format!("Listing \"{path}\": {e}");
    let dir = match open_root(root) {
        Ok(dir) => dir,
        Err(e) if e.kind() == ErrorKind::NotFound && path.is_empty() => return Ok(Vec::new()),
        Err(e) => return Err(failed(e)),
    };
    let dir = if path.is_empty() {
        dir
    } else {
        dir.open_dir(relative(path)?).map_err(failed)?
    };
    let mut entries = Vec::new();
    for entry in dir.entries().map_err(failed)? {
        let entry = entry.map_err(failed)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(TEMP_SUFFIX) {
            continue;
        }
        // Not followed: a symlink is listed as what it is.
        let meta = entry.metadata().map_err(failed)?;
        let kind = if meta.is_dir() {
            EntryKind::Dir
        } else if meta.is_file() {
            EntryKind::File
        } else {
            EntryKind::Other
        };
        let modified = meta
            .modified()
            .ok()
            .and_then(|t| t.into_std().duration_since(std::time::UNIX_EPOCH).ok())
            .and_then(|d| i64::try_from(d.as_secs()).ok());
        entries.push(FileEntry {
            name,
            kind,
            size: if kind == EntryKind::File {
                meta.len()
            } else {
                0
            },
            modified,
        });
    }
    entries.sort_by(|a, b| {
        (a.kind != EntryKind::Dir, &a.name).cmp(&(b.kind != EntryKind::Dir, &b.name))
    });
    Ok(entries)
}

/// Deletes a file, or a directory with everything in it. A symlink is removed, never followed.
pub fn remove(root: &Path, path: &str) -> Result<(), String> {
    let relative = relative(path)?;
    let failed = |e: std::io::Error| format!("Deleting \"{path}\": {e}");
    let dir = open_root(root).map_err(failed)?;
    if dir.symlink_metadata(relative).map_err(failed)?.is_dir() {
        dir.remove_dir_all(relative).map_err(failed)
    } else {
        dir.remove_file(relative).map_err(failed)
    }
}

/// Creates a directory and missing parents.
pub fn make_dir(root: &Path, path: &str) -> Result<(), String> {
    let relative = relative(path)?;
    let failed = |e: std::io::Error| format!("Creating \"{path}\": {e}");
    std::fs::create_dir_all(root).map_err(failed)?;
    open_root(root)
        .and_then(|dir| dir.create_dir_all(relative))
        .map_err(failed)
}

/// Renames or moves; never replaces an existing file or directory.
pub fn rename(root: &Path, from: &str, to: &str) -> Result<(), String> {
    let (source, target) = (relative(from)?, relative(to)?);
    let failed = |e: std::io::Error| format!("Moving \"{from}\": {e}");
    let dir = open_root(root).map_err(failed)?;
    if dir.symlink_metadata(target).is_ok() {
        return Err(format!("\"{to}\" already exists"));
    }
    dir.rename(source, &dir, target).map_err(failed)
}

/// Opens a regular file for a download; returns it and its size.
pub fn open_for_download(root: &Path, path: &str) -> Result<(std::fs::File, u64), String> {
    let relative = relative(path)?;
    let failed = |e: std::io::Error| format!("Opening \"{path}\": {e}");
    let file = open_root(root)
        .and_then(|dir| dir.open(relative))
        .map_err(failed)?;
    let meta = file.metadata().map_err(failed)?;
    if !meta.is_file() {
        return Err(format!("\"{path}\" is not a file"));
    }
    Ok((file.into_std(), meta.len()))
}

/// An upload being written to a temporary file next to its target (ADR 0015).
pub struct Upload {
    dir: Dir,
    temp: PathBuf,
    target: PathBuf,
    /// Where the received bytes go.
    pub file: std::fs::File,
}

impl Upload {
    /// Moves the complete file into place, replacing an older one.
    pub fn finish(self) -> Result<(), String> {
        self.file
            .sync_all()
            .and_then(|()| self.dir.rename(&self.temp, &self.dir, &self.target))
            .map_err(|e| {
                let _ = self.dir.remove_file(&self.temp);
                format!("Saving \"{}\": {e}", self.target.display())
            })
    }

    /// Throws the partial file away.
    pub fn abort(self) {
        let _ = self.dir.remove_file(&self.temp);
    }
}

/// Starts an upload to `path`; missing directories are created.
pub fn start_upload(root: &Path, path: &str) -> Result<Upload, String> {
    let target = relative(path)?.to_path_buf();
    let failed = |e: std::io::Error| format!("Writing \"{path}\": {e}");
    std::fs::create_dir_all(root).map_err(failed)?;
    let dir = open_root(root).map_err(failed)?;
    let parent = target.parent().unwrap_or(Path::new("")).to_path_buf();
    if !parent.as_os_str().is_empty() {
        dir.create_dir_all(&parent).map_err(failed)?;
    }
    if dir.metadata(&target).is_ok_and(|m| m.is_dir()) {
        return Err(format!("\"{path}\" is a directory"));
    }
    let name = target.file_name().unwrap_or_default().to_string_lossy();
    let temp = parent.join(format!(".{name}.upload{TEMP_SUFFIX}"));
    let _ = dir.remove_file(&temp);
    let file = dir
        .open_with(&temp, OpenOptions::new().write(true).create_new(true))
        .map_err(failed)?
        .into_std();
    Ok(Upload {
        dir,
        temp,
        target,
        file,
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

    #[test]
    fn the_file_manager_lists_moves_deletes_and_uploads_inside_only() {
        let root = std::env::temp_dir().join(format!("harrier-fm-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        assert_eq!(list(&root, ""), Ok(Vec::new()), "before the first start");

        write(&root, "b.txt", "b").unwrap();
        make_dir(&root, "world/region").unwrap();
        std::fs::write(root.join(".x.harrier-tmp"), "partial").unwrap();
        std::os::unix::fs::symlink("/etc", root.join("link")).unwrap();
        let listed: Vec<_> = list(&root, "")
            .unwrap()
            .into_iter()
            .map(|e| (e.name, e.kind, e.size))
            .collect();
        assert_eq!(
            listed,
            [
                ("world".to_string(), EntryKind::Dir, 0),
                ("b.txt".to_string(), EntryKind::File, 1),
                ("link".to_string(), EntryKind::Other, 0),
            ],
            "directories first, temporary files hidden, symlinks not followed"
        );
        assert!(
            list(&root, "link").is_err(),
            "a symlink is not opened as a directory"
        );

        assert!(rename(&root, "b.txt", "world").is_err(), "never replaces");
        rename(&root, "b.txt", "world/b.txt").unwrap();
        assert!(rename(&root, "world/b.txt", "../b.txt").is_err());

        // An upload only appears when it is complete.
        let mut upload = start_upload(&root, "plugins/x.jar").unwrap();
        upload.file.write_all(b"jar").unwrap();
        assert!(read(&root, "plugins/x.jar").unwrap().is_none());
        upload.finish().unwrap();
        assert_eq!(open_for_download(&root, "plugins/x.jar").unwrap().1, 3);
        let mut broken = start_upload(&root, "plugins/y.jar").unwrap();
        broken.file.write_all(b"half").unwrap();
        broken.abort();
        assert!(
            list(&root, "plugins")
                .unwrap()
                .iter()
                .all(|e| e.name == "x.jar")
        );

        remove(&root, "link").unwrap();
        assert!(
            std::path::Path::new("/etc").exists(),
            "the link's target stays"
        );
        remove(&root, "world").unwrap();
        assert!(remove(&root, "").is_err() && remove(&root, "..").is_err());
        assert_eq!(list(&root, "").unwrap().len(), 1);
        let _ = std::fs::remove_dir_all(&root);
    }
}
