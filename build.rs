use semver::Version;
use std::collections::{HashMap, HashSet};
use std::env::VarError;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::{fs, process};

type Target = String;
fn target_and_versions() -> (Vec<(Target, PathBuf)>, HashMap<Target, Version>) {
    let mut versions = HashMap::new();
    let mut targets = Vec::new();

    for entry in fs::read_dir("builds_without_ssh").unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() && entry.file_name() == "target" {
            continue;
        }

        assert!(
            entry.file_type().unwrap().is_file(),
            "should only be files in builds_without_ssh dir"
        );

        let file_name = entry.file_name().into_string().unwrap();
        if let Some(arch) = file_name.strip_prefix("version_of") {
            let version = fs::read_to_string(entry.path()).unwrap();
            let version = Version::parse(&version).unwrap();
            versions.insert(arch.to_string(), version);
        } else {
            targets.push((file_name, entry.path()));
        };
    }
    (targets, versions)
}

fn version_compatible(file_version: &Version) -> bool {
    let version = Version::parse(env!("CARGO_PKG_VERSION")).unwrap();
    if file_version.major == 0 || version.major == 0 {
        *file_version == version
    } else {
        file_version.major == version.major
    }
}

fn main() {
    println!("cargo::rerun-if-changed=src");
    match dbg!(std::env::var("CARGO_FEATURE_SSH")) {
        Err(e) if e == VarError::NotPresent => {
            return;
        }
        Err(e) => panic!("unknown error while checking ssh feature envar: {e}"),
        Ok(v) if v == "1" => (),
        Ok(v) => panic!("unknown value: `{v}` for ssh feature envar"),
    }
    eprintln!("Checking if we have the ssh-less binaries to be included in the final binary");

    // Tell Cargo that if the given file changes, to rerun this build script.

    // let mut needed: HashSet<String> = ["aarch64-unknown-linux-musl", "x86_64-unknown-linux-musl"]
    let mut needed: HashSet<String> = ["x86_64-unknown-linux-gnu"]
        .into_iter()
        .map(|s| s.to_string())
        .collect();


    if let Err(e) = std::fs::create_dir("builds_without_ssh")
        && e.kind() != ErrorKind::AlreadyExists
    {
        panic!("Error creating dir `builds_without_ssh`: {e}");
    }
    let (targets, versions) = target_and_versions();
    for (target, path) in targets {
        match versions.get(&target) {
            Some(v) if version_compatible(v) => {
                needed.remove(&target);
            }
            Some(_) | None => {
                assert!(path.iter().any(|d| d == "builds_without_ssh"));
                fs::remove_file(path).unwrap();
            }
        }
    }

    if let Err(e) = std::fs::create_dir("builds_without_ssh/target")
        && e.kind() != ErrorKind::AlreadyExists
    {
        panic!("Error creating dir `builds_without_ssh/target`: {e}");
    }
    for target in needed {
        // cant work as it holds a lock on the dir
        eprintln!("Spawning cargo to build ssh less build for: {target}");
        use std::process::Stdio;
        let mut cargo = process::Command::new("cargo")
            .arg("build")
            .arg("-vv")
            .arg("--no-default-features")
            .args(["--target-dir", "builds_without_ssh/target"])
            .args(["--target", &target])
            .env_remove("CARGO_FEATURE_SSH")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        use std::io::{Read, Write};
        let mut stdout = cargo.stdout.take().unwrap();
        let mut stderr = cargo.stderr.take().unwrap();
        let status = std::thread::scope(|s| {
            s.spawn(|| {
                let mut readbuf = [0; 10];
                loop {
                    let Ok(n) = stdout.read(&mut readbuf) else {
                        eprintln!("closed stdout");
                        break;
                    };
                    if n == 0 {
                        eprintln!("closed stderr");
                        break;
                    }
                    std::io::stderr().write(&readbuf[..n]).unwrap();
                }
            });
            s.spawn(|| {
                let mut readbuf = [0; 10];
                loop {
                    let Ok(n) = stderr.read(&mut readbuf) else {
                        eprintln!("closed stderr");
                        break;
                    };
                    if n == 0 {
                        eprintln!("closed stderr");
                        break;
                    }
                    std::io::stderr().write(&readbuf[..n]).unwrap();
                }
            });
            eprintln!("Waiting for cargo build to end");
            let status = cargo.wait().unwrap();
            eprintln!("It ended");
            status
        });
        eprintln!("joined");

        if !status.success() {
            panic!("Error compiling build_without_ssh");
        }

        std::fs::rename(
            format!("builds_without_ssh/target/{target}/debug/{}", env!("CARGO_PKG_NAME")),
            format!("builds_without_ssh/{target}"),
        )
        .unwrap();

        std::fs::write(
            format!("builds_without_ssh/{target}"),
            env!("CARGO_PKG_VERSION"),
        )
        .unwrap();
    }
}
