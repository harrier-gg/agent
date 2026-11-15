//! Downloads files from the internet into a server's data directory, e.g. mods (ADR 0017). HTTPS
//! only; the file is moved into place only if size and hash match, otherwise nothing changes.
//! With `extract` the file is a zip that is unpacked into a folder, replacing the old folder only
//! once everything is unpacked (Thunderstore, ADR 0023).

use crate::files;
use cap_std::fs::{Dir, OpenOptions};
use std::io::{ErrorKind, Read, Seek, Write};
use std::path::Path;

/// Larger files are refused: mods and plugins are megabytes, not gigabytes.
const MAX_BYTES: u64 = 512 * 1024 * 1024;
/// What a zip may unpack to; more is a zip bomb.
// ponytail: smaller in tests so the limit test does not write a gigabyte.
const MAX_UNPACKED: u64 = if cfg!(test) { 1 << 20 } else { 1 << 30 };
const MAX_ENTRIES: usize = 10_000;

/// What the file must be.
pub struct Expected<'a> {
    pub size: Option<u64>,
    pub sha512: Option<&'a str>,
    pub sha1: Option<&'a str>,
    pub sha256: Option<&'a str>,
}

/// Downloads `url` to `path` inside `root` (with `extract`: unpacks it into the folder `path`);
/// returns the downloaded size.
pub fn download(
    root: &Path,
    path: &str,
    url: &str,
    expected: &Expected,
    extract: bool,
) -> Result<u64, String> {
    if !url.starts_with("https://") {
        return Err("Only HTTPS downloads are allowed".into());
    }
    if expected.sha512.is_none() && expected.sha1.is_none() && expected.sha256.is_none() {
        return Err("A download needs a hash".into());
    }
    let response = ureq::get(url)
        .header(
            "user-agent",
            concat!("harrier-agent/", env!("CARGO_PKG_VERSION")),
        )
        .call()
        .map_err(|e| format!("Downloading {url}: {e}"))?;
    install(
        root,
        path,
        &mut response.into_body().into_reader(),
        expected,
        extract,
    )
}

/// Writes `body` to a temporary file and puts it in place only if size and hash match.
fn install(
    root: &Path,
    path: &str,
    body: &mut impl Read,
    expected: &Expected,
    extract: bool,
) -> Result<u64, String> {
    // A zip lives next to its folder only until it is unpacked.
    let target = if extract {
        files::relative(path)?;
        format!("{path}.zip")
    } else {
        path.to_owned()
    };
    let limit = expected.size.unwrap_or(MAX_BYTES).min(MAX_BYTES);
    let mut upload = files::start_upload(root, &target)?;
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
            Err(e) => break Err(format!("Downloading {path}: {e}")),
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
        Ok(()) if extract => {
            let unpacked = unpack(root, path, &mut upload.file);
            upload.abort();
            unpacked.map(|()| size)
        }
        Ok(()) => upload.finish().map(|()| size),
        Err(e) => {
            upload.abort();
            Err(e)
        }
    }
}

/// Unpacks the zip into a temporary folder next to the folder `path`, then swaps it in. Until the
/// swap the old folder stays untouched, so a broken or oversized zip changes nothing.
fn unpack(root: &Path, path: &str, zip: &mut std::fs::File) -> Result<(), String> {
    let target = files::relative(path)?;
    let failed = |e: std::io::Error| format!("Unpacking into \"{path}\": {e}");
    let dir = files::open_root(root).map_err(failed)?;
    let parent = target.parent().unwrap_or(Path::new(""));
    let name = target.file_name().unwrap_or_default().to_string_lossy();
    let temp = parent.join(format!(".{name}.unpack{}", files::TEMP_SUFFIX));
    let old = parent.join(format!(".{name}.old{}", files::TEMP_SUFFIX));
    let _ = dir.remove_dir_all(&temp);
    let _ = dir.remove_dir_all(&old);
    dir.create_dir(&temp).map_err(failed)?;
    let swapped = dir
        .open_dir(&temp)
        .map_err(failed)
        .and_then(|into| unpack_into(&into, zip, path))
        .and_then(|()| match dir.symlink_metadata(target) {
            Err(e) if e.kind() == ErrorKind::NotFound => {
                dir.rename(&temp, &dir, target).map_err(failed)
            }
            Err(e) => Err(failed(e)),
            Ok(meta) if !meta.is_dir() => Err(format!("\"{path}\" is not a folder")),
            Ok(_) => {
                dir.rename(target, &dir, &old).map_err(failed)?;
                if let Err(e) = dir.rename(&temp, &dir, target) {
                    let _ = dir.rename(&old, &dir, target);
                    return Err(failed(e));
                }
                let _ = dir.remove_dir_all(&old);
                Ok(())
            }
        });
    if swapped.is_err() {
        let _ = dir.remove_dir_all(&temp);
    }
    swapped
}

/// Every entry must be a plain relative path (as in the file manager) and is created relative to
/// `into`, so nothing can land outside it. Symlinks are refused; file modes are not kept.
fn unpack_into(into: &Dir, zip: &mut std::fs::File, path: &str) -> Result<(), String> {
    let bad = |e: zip::result::ZipError| format!("\"{path}\" is not a valid zip: {e}");
    zip.rewind().map_err(|e| format!("Reading {path}: {e}"))?;
    let mut archive = zip::ZipArchive::new(zip).map_err(bad)?;
    if archive.len() > MAX_ENTRIES {
        return Err(format!("The zip has more than {MAX_ENTRIES} entries"));
    }
    let mut left = MAX_UNPACKED;
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(bad)?;
        // Zips made on Windows may separate with backslashes.
        let name = entry.name().map_err(bad)?.replace('\\', "/");
        if entry.is_symlink() {
            return Err(format!("The zip contains a symlink \"{name}\""));
        }
        let relative = files::relative(&name)?;
        let failed = |e: std::io::Error| format!("Unpacking \"{name}\": {e}");
        if entry.is_dir() {
            into.create_dir_all(relative).map_err(failed)?;
            continue;
        }
        if let Some(parent) = relative.parent().filter(|p| !p.as_os_str().is_empty()) {
            into.create_dir_all(parent).map_err(failed)?;
        }
        let mut file = into
            .open_with(relative, OpenOptions::new().write(true).create_new(true))
            .map_err(failed)?;
        let written = std::io::copy(&mut (&mut entry).take(left + 1), &mut file).map_err(failed)?;
        if written > left {
            return Err(format!(
                "The zip unpacks to more than {} MiB",
                MAX_UNPACKED >> 20
            ));
        }
        left -= written;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

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
            download(&root, "mods/x.jar", "http://example.com/x.jar", &any, false)
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
            download(
                &root,
                "mods/x.jar",
                "https://example.com/x.jar",
                &none,
                true
            )
            .unwrap_err()
            .contains("hash")
        );
        assert!(!root.join("mods/x.jar").exists());
    }

    const MOD: &str = "BepInEx/plugins/Author-Mod";

    fn temp_root(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("harrier-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        root
    }

    fn zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default();
        for (name, content) in entries {
            if name.ends_with('/') {
                writer.add_directory(*name, options).unwrap();
            } else {
                writer.start_file(*name, options).unwrap();
                writer.write_all(content).unwrap();
            }
        }
        writer.finish().unwrap().into_inner()
    }

    /// Installs `bytes` as the mod, checked against `hash` (default: its own SHA-256).
    fn put(root: &Path, bytes: &[u8], hash: Option<&str>) -> Result<u64, String> {
        let own = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(bytes));
        let expected = Expected {
            size: None,
            sha512: None,
            sha1: None,
            sha256: Some(hash.unwrap_or(&own)),
        };
        install(root, MOD, &mut &bytes[..], &expected, true)
    }

    /// Everything below `dir`, as sorted relative paths.
    fn tree(dir: &Path) -> Vec<String> {
        let mut found = Vec::new();
        let mut todo = vec![dir.to_path_buf()];
        while let Some(next) = todo.pop() {
            for entry in std::fs::read_dir(next).unwrap() {
                let entry = entry.unwrap().path();
                found.push(entry.strip_prefix(dir).unwrap().display().to_string());
                if entry.is_dir() {
                    todo.push(entry);
                }
            }
        }
        found.sort();
        found
    }

    #[test]
    fn unpacks_into_a_new_folder_and_replaces_it_on_update() {
        let root = temp_root("unpack");
        let first = zip(&[
            ("manifest.json", b"{}"),
            ("plugins/", b""),
            ("plugins\\Mod.dll", b"v1"),
        ]);
        assert_eq!(put(&root, &first, None), Ok(first.len() as u64));
        assert_eq!(
            tree(&root.join("BepInEx/plugins")),
            [
                "Author-Mod",
                "Author-Mod/manifest.json",
                "Author-Mod/plugins",
                "Author-Mod/plugins/Mod.dll"
            ]
        );

        let second = zip(&[("Mod.dll", b"v2")]);
        put(&root, &second, None).unwrap();
        assert_eq!(
            tree(&root.join("BepInEx/plugins")),
            ["Author-Mod", "Author-Mod/Mod.dll"],
            "the old folder is replaced, no temporary files are left"
        );
        assert_eq!(
            std::fs::read(root.join(MOD).join("Mod.dll")).unwrap(),
            b"v2"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_bad_zip_changes_nothing() {
        let root = temp_root("unpack-bad");
        put(&root, &zip(&[("Mod.dll", b"old")]), None).unwrap();
        let before = tree(&root);

        let mut symlink = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        symlink
            .add_symlink("link", "/etc", zip::write::SimpleFileOptions::default())
            .unwrap();
        let symlink = symlink.finish().unwrap().into_inner();
        let many: Vec<String> = (0..=MAX_ENTRIES).map(|i| format!("{i}.txt")).collect();
        let many: Vec<(&str, &[u8])> = many.iter().map(|n| (n.as_str(), &b""[..])).collect();
        let half = vec![0u8; (MAX_UNPACKED / 2 + 1) as usize];
        let cases: [(&str, Vec<u8>, Option<&str>); 9] = [
            ("parent", zip(&[("ok.txt", b"x"), ("../x", b"x")]), None),
            ("absolute", zip(&[("/abs", b"x")]), None),
            ("backslash parent", zip(&[("a\\..\\..\\x", b"x")]), None),
            ("symlink", symlink, None),
            ("too many entries", zip(&many), None),
            ("too large", zip(&[("a", &half), ("b", &half)]), None),
            ("not a zip", b"not a zip".to_vec(), None),
            ("wrong hash", zip(&[("Mod.dll", b"new")]), Some("00")),
            ("duplicate", zip(&[("a/b", b"1"), ("a\\b", b"2")]), None),
        ];
        for (case, bytes, hash) in cases {
            assert!(put(&root, &bytes, hash).is_err(), "{case} must be refused");
            assert_eq!(tree(&root), before, "{case} must change nothing");
        }
        assert_eq!(
            std::fs::read(root.join(MOD).join("Mod.dll")).unwrap(),
            b"old"
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
