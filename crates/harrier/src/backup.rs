//! Backups of a server's data directory as `tar.gz` archives on the node (ADR 0016). They live in
//! `<data dir>/.harrier-backups/<server>/`, outside every server directory, so neither the game
//! nor the file manager can change them.

use cap_std::ambient_authority;
use cap_std::fs::Dir;
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use harrier_protocol::{EntryKind, FileEntry};
use std::fs::File;
use std::path::{Component, Path, PathBuf};

const BACKUPS_DIR: &str = ".harrier-backups";
const SUFFIX: &str = ".tar.gz";
/// Backups made before a restore are kept apart from the schedule's, at most this many.
const BEFORE_RESTORE: &str = "-before-restore";
const KEEP_BEFORE_RESTORE: usize = 3;

/// Where a server's backups are.
pub fn dir(data_dir: &Path, server_id: &str) -> PathBuf {
    data_dir.join(BACKUPS_DIR).join(server_id)
}

/// A backup name as the agent creates it: `<unix seconds>[-before-restore].tar.gz`.
fn checked(name: &str) -> Result<&str, String> {
    let stem = name.strip_suffix(SUFFIX).unwrap_or("");
    let stem = stem.strip_suffix(BEFORE_RESTORE).unwrap_or(stem);
    if stem.is_empty() || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("\"{name}\" is not a backup"));
    }
    Ok(name)
}

/// Whether a top-level entry is left out: its exact name, or `*.jar` for a file ending.
fn excluded(name: &str, patterns: &[String]) -> bool {
    patterns.iter().any(|p| {
        p.strip_prefix('*')
            .map_or(p == name, |end| name.ends_with(end))
    })
}

/// A new name; seconds since 1970, so names sort by age.
pub fn new_name(before_restore: bool) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let tag = if before_restore { BEFORE_RESTORE } else { "" };
    format!("{now}{tag}{SUFFIX}")
}

/// Archives `server_dir` as `name`; returns the archive's size. Written to a temporary file
/// first, so an interrupted backup never looks complete.
pub fn create(
    server_dir: &Path,
    backups: &Path,
    name: &str,
    exclude: &[String],
) -> Result<u64, String> {
    let failed = |e: std::io::Error| format!("Backing up: {e}");
    std::fs::create_dir_all(backups).map_err(failed)?;
    let partial = backups.join(format!(".{name}.partial"));
    let target = backups.join(checked(name)?);
    let result = (|| {
        let encoder = GzEncoder::new(File::create(&partial)?, Compression::fast());
        let mut archive = tar::Builder::new(encoder);
        if server_dir.exists() {
            // Through cap-std: a symlink the game creates can never pull outside files in.
            let root = Dir::open_ambient_dir(server_dir, ambient_authority())?;
            append_dir(&mut archive, &root, Path::new(""), exclude)?;
        }
        archive.into_inner()?.finish()?.sync_all()?;
        std::fs::rename(&partial, &target)?;
        Ok(std::fs::metadata(&target)?.len())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&partial);
    }
    result.map_err(failed)
}

/// Adds regular files and directories below `dir`; symlinks and special files are left out.
/// `exclude` applies to the top level only (`prefix` empty).
fn append_dir(
    archive: &mut tar::Builder<GzEncoder<File>>,
    dir: &Dir,
    prefix: &Path,
    exclude: &[String],
) -> std::io::Result<()> {
    for entry in dir.entries()? {
        let entry = entry?;
        let name = entry.file_name();
        if excluded(&name.to_string_lossy(), exclude) {
            continue;
        }
        let path = prefix.join(&name);
        let kind = entry.file_type()?;
        if kind.is_dir() {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Directory);
            header.set_mode(0o755);
            header.set_size(0);
            archive.append_data(&mut header, &path, std::io::empty())?;
            append_dir(archive, &dir.open_dir(&name)?, &path, &[])?;
        } else if kind.is_file() {
            let file = dir.open(&name)?;
            let meta = file.metadata()?;
            let mut header = tar::Header::new_gnu();
            header.set_size(meta.len());
            header.set_mode(0o644);
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.into_std().duration_since(std::time::UNIX_EPOCH).ok());
            header.set_mtime(mtime.map_or(0, |d| d.as_secs()));
            // The game may still append to a log while it is copied: take exactly `len` bytes.
            archive.append_data(
                &mut header,
                &path,
                std::io::Read::take(file.into_std(), meta.len()),
            )?;
        }
    }
    Ok(())
}

/// The backups, newest first.
pub fn list(backups: &Path) -> Result<Vec<FileEntry>, String> {
    let mut entries: Vec<FileEntry> = match std::fs::read_dir(backups) {
        Ok(dir) => dir
            .filter_map(Result::ok)
            .filter(|e| checked(&e.file_name().to_string_lossy()).is_ok())
            .filter_map(|e| {
                let meta = e.metadata().ok()?;
                let modified = meta
                    .modified()
                    .ok()?
                    .duration_since(std::time::UNIX_EPOCH)
                    .ok()?;
                Some(FileEntry {
                    name: e.file_name().to_string_lossy().into_owned(),
                    kind: EntryKind::File,
                    size: meta.len(),
                    modified: i64::try_from(modified.as_secs()).ok(),
                })
            })
            .collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(format!("Listing backups: {e}")),
    };
    // Names start with the time they were made.
    let made = |e: &FileEntry| {
        e.name
            .split('-')
            .next()
            .and_then(|n| n.strip_suffix(SUFFIX).or(Some(n))?.parse::<u64>().ok())
    };
    entries.sort_by_key(|e| std::cmp::Reverse(made(e)));
    Ok(entries)
}

/// Deletes the oldest backups beyond `keep` (the schedule's) and beyond 3 made before restores.
pub fn prune(backups: &Path, keep: u32) -> Result<(), String> {
    let all = list(backups)?;
    let (before, regular): (Vec<_>, Vec<_>) =
        all.iter().partition(|e| e.name.contains(BEFORE_RESTORE));
    let old = regular
        .iter()
        .skip(keep as usize)
        .chain(before.iter().skip(KEEP_BEFORE_RESTORE));
    for entry in old {
        std::fs::remove_file(backups.join(&entry.name))
            .map_err(|e| format!("Deleting {}: {e}", entry.name))?;
    }
    Ok(())
}

pub fn remove(backups: &Path, name: &str) -> Result<(), String> {
    std::fs::remove_file(backups.join(checked(name)?)).map_err(|e| format!("Deleting {name}: {e}"))
}

/// Replaces `server_dir` with the backup's content. Unpacks next to it first and swaps only when
/// the archive was read completely; entries that would leave the directory are skipped.
/// Top-level entries matching `keep` move over from the current state (the backup left them out).
pub fn restore(
    server_dir: &Path,
    backups: &Path,
    name: &str,
    keep: &[String],
) -> Result<(), String> {
    let failed = |e: std::io::Error| format!("Restoring {name}: {e}");
    let archive = File::open(backups.join(checked(name)?)).map_err(failed)?;
    let fresh = server_dir.with_extension("restore");
    let old = server_dir.with_extension("old");
    let _ = std::fs::remove_dir_all(&fresh);
    std::fs::create_dir_all(&fresh).map_err(failed)?;
    let unpacked = (|| {
        let mut archive = tar::Archive::new(GzDecoder::new(archive));
        for entry in archive.entries()? {
            let mut entry = entry?;
            let kind = entry.header().entry_type();
            let path = entry.path()?.into_owned();
            let inside = path.components().all(|c| matches!(c, Component::Normal(_)));
            if inside && (kind.is_file() || kind.is_dir()) {
                entry.unpack_in(&fresh)?;
            }
        }
        Ok(())
    })();
    if let Err(e) = unpacked {
        let _ = std::fs::remove_dir_all(&fresh);
        return Err(failed(e));
    }
    let _ = std::fs::remove_dir_all(&old);
    if let Ok(current) = std::fs::read_dir(server_dir) {
        for entry in current.filter_map(Result::ok) {
            let name = entry.file_name();
            let target = fresh.join(&name);
            if excluded(&name.to_string_lossy(), keep) && !target.exists() {
                std::fs::rename(entry.path(), target).map_err(failed)?;
            }
        }
    }
    if server_dir.exists() {
        std::fs::rename(server_dir, &old).map_err(failed)?;
    }
    std::fs::rename(&fresh, server_dir).map_err(failed)?;
    let _ = std::fs::remove_dir_all(&old);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backups_round_trip_and_leave_out_what_lies_outside() {
        let root = std::env::temp_dir().join(format!("harrier-backup-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let server = root.join("server");
        let backups = dir(&root, "server");
        std::fs::create_dir_all(server.join("world/region")).unwrap();
        std::fs::write(server.join("world/region/r.0.0.mca"), vec![7u8; 100_000]).unwrap();
        std::fs::write(server.join("server.properties"), "motd=Hi\n").unwrap();
        std::fs::write(root.join("secret"), "outside").unwrap();
        std::os::unix::fs::symlink(root.join("secret"), server.join("link")).unwrap();

        let name = "100.tar.gz";
        std::fs::create_dir_all(server.join("libraries")).unwrap();
        std::fs::write(server.join("libraries/big.jar"), "lib").unwrap();
        std::fs::write(server.join("minecraft_server.jar"), "jar").unwrap();
        std::fs::create_dir_all(server.join("plugins")).unwrap();
        std::fs::write(server.join("plugins/kept.jar"), "plugin").unwrap();
        let exclude = ["libraries".to_string(), "*.jar".to_string()];
        assert!(create(&server, &backups, name, &exclude).unwrap() > 0);
        let names: Vec<String> =
            tar::Archive::new(GzDecoder::new(File::open(backups.join(name)).unwrap()))
                .entries()
                .unwrap()
                .map(|e| e.unwrap().path().unwrap().display().to_string())
                .collect();
        assert!(
            names.contains(&"plugins/kept.jar".to_string()),
            "only top-level entries are excluded"
        );
        assert!(
            !names
                .iter()
                .any(|n| n.starts_with("libraries") || n == "minecraft_server.jar")
        );
        std::fs::write(server.join("server.properties"), "motd=Broken\n").unwrap();
        std::fs::remove_dir_all(server.join("world")).unwrap();

        restore(&server, &backups, name, &exclude).unwrap();
        assert_eq!(
            std::fs::read_to_string(server.join("minecraft_server.jar")).unwrap(),
            "jar",
            "kept from the current state"
        );
        assert!(server.join("libraries/big.jar").exists());
        assert_eq!(
            std::fs::read_to_string(server.join("server.properties")).unwrap(),
            "motd=Hi\n"
        );
        assert_eq!(
            std::fs::read(server.join("world/region/r.0.0.mca"))
                .unwrap()
                .len(),
            100_000
        );
        assert!(!server.join("link").exists(), "symlinks are not backed up");

        assert!(restore(&server, &backups, "../secret", &[]).is_err());
        assert!(remove(&backups, "x.tar.gz").is_err());

        // Pruning keeps the newest regular ones and up to 3 from before restores.
        for n in ["101.tar.gz", "102.tar.gz", "103-before-restore.tar.gz"] {
            create(&server, &backups, n, &[]).unwrap();
        }
        prune(&backups, 2).unwrap();
        let names: Vec<_> = list(&backups)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert_eq!(
            names,
            ["103-before-restore.tar.gz", "102.tar.gz", "101.tar.gz"]
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
