use cargo_lock::{Lockfile, SourceId, Version};
use std::ffi::OsString;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::{env, io};

fn main() {
    let lockfile_location = get_lockfile_location().unwrap();
    let (version, source) = read_lockfile(&lockfile_location);

    println!("cargo:rustc-env=OBJECT_STORE_VERSION={}", version);
    println!(
        "cargo:rustc-env=OBJECT_STORE_SOURCE={}",
        source.map(|s| s.to_string()).unwrap_or("".to_string())
    );
}

fn get_lockfile_location() -> io::Result<PathBuf> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    let cargo_lock = OsString::from("Cargo.lock");

    for ancestor in path.as_path().ancestors() {
        for entry in ancestor.read_dir()? {
            let entry = entry?;
            if entry.file_name() == cargo_lock {
                return Ok(entry.path());
            }
        }
    }

    Err(io::Error::new(
        ErrorKind::NotFound,
        "Ran out of places to find Cargo.toml",
    ))
}

fn read_lockfile(path: &Path) -> (Version, Option<SourceId>) {
    let lockfile = Lockfile::load(path).unwrap();
    let idx = lockfile
        .packages
        .iter()
        .position(|p| p.name.as_str() == "object_store")
        .unwrap();
    let package = &lockfile.packages[idx];
    (package.version.clone(), package.source.clone())
}
