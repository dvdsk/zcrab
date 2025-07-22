use semver::Version;
use std::collections::{HashMap, HashSet};
use std::env::VarError;
use std::path::PathBuf;
use std::{fs, process};

fn main() {
    println!("cargo::rerun-if-changed=src");

    match std::env::var("CARGO_FEATURE_SSH") {
        Err(e) if e == VarError::NotPresent => {
            return;
        }
        Err(e) => panic!("unknown error while checking ssh feature envar: {e}"),
        Ok(v) if v == "1" => (),
        Ok(v) => panic!("unknown value: `{v}` for ssh feature envar"),
    }

    // let mut needed: HashSet<String> = ["aarch64-unknown-linux-musl", "x86_64-unknown-linux-musl"]
    let mut needed: HashSet<&str> = ["x86_64-unknown-linux-musl"].into_iter().collect();

    std::fs::create_dir_all("builds_without_ssh/target").unwrap();
    let (targets, versions) = target_and_versions();
    for (target, path) in targets {
        match versions.get(&target) {
            Some(v) if version_compatible(v) => {
                needed.remove(target.as_str());
            }
            Some(_) | None => {
                assert!(path.iter().any(|d| d == "builds_without_ssh"));
                fs::remove_file(path).unwrap();
            }
        }
    }

    for target in needed {
        build_target(&target);
        copy_bin_and_store_version(target);
    }
}

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
        if let Some(arch) = file_name.strip_prefix("version_of_") {
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

fn copy_bin_and_store_version(target: &str) {
    std::fs::rename(
        format!(
            "builds_without_ssh/target/{target}/debug/{}",
            env!("CARGO_PKG_NAME")
        ),
        format!("builds_without_ssh/{target}"),
    )
    .unwrap();

    std::fs::write(
        format!("builds_without_ssh/version_of_{target}"),
        env!("CARGO_PKG_VERSION"),
    )
    .unwrap();
}

fn build_target(target: &str) {
    eprintln!("Spawning cargo to build ssh less build for: {target}");
    use std::process::Stdio;
    let mut cargo = process::Command::new("cargo")
        .arg("build")
        .arg("-vv")
        .arg("--no-default-features")
        .args(["--target-dir", "builds_without_ssh/target"])
        .args(["--target", target])
        .env_remove("CARGO_FEATURE_SSH")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdout = cargo.stdout.take().unwrap();
    let mut stderr = cargo.stderr.take().unwrap();
    let status = std::thread::scope(|s| {
        s.spawn(|| {
            std::io::copy(&mut stdout, &mut std::io::stderr()).unwrap();
        });
        s.spawn(|| {
            std::io::copy(&mut stderr, &mut std::io::stderr()).unwrap();
        });
        let status = cargo.wait().unwrap();
        status
    });

    if !status.success() {
        panic!("Error compiling build_without_ssh");
    }
}
