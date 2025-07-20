use std::io::ErrorKind;

use color_eyre::eyre::{OptionExt, WrapErr, eyre};
use color_eyre::{Help, Result};

use openssh::{KnownHosts, Session};
use tokio::io::AsyncWriteExt;

struct Connection {
    session: Session,
}

impl Connection {
    async fn new(target: &str) -> Result<Self> {
        Ok(Self {
            session: Session::connect(target, KnownHosts::Strict)
                .await
                .wrap_err("Could not connect")
                .with_note(|| format!("ssh target: {target}"))?,
        })
    }

    async fn copy_basic_build(&self, build: &[u8]) -> Result<()> {
        let mut copy_process = self
            .session
            .command("cp")
            .arg("/dev/stdin")
            .arg(concat!("/tmp/", env!("CARGO_BIN_NAME")))
            .stdin(openssh::Stdio::piped())
            .spawn()
            .await
            .wrap_err("Failed to spawn cp cmd")?;
        let stdin = copy_process
            .stdin()
            .as_mut()
            .expect("just configured stdin to piped");
        stdin
            .write_all(build)
            .await
            .wrap_err("Failed to write bytes to remote")?;
        let res = copy_process
            .wait_with_output()
            .await
            .wrap_err("Copy failed to complete")?;

        if !res.stderr.is_empty() {
            let err = String::from_utf8_lossy(&res.stderr);
            Err(eyre!("cp on remote machine returned error")).with_note(|| format!("error: {err}"))
        } else {
            Ok(())
        }
    }

    async fn has_compatible_version_installed(&self) -> Result<RemoteBin> {
        let compatible_in_path = self
            .check_remote_version(env!("CARGO_BIN_NAME"))
            .await
            .wrap_err("checking for install in path")?;
        if compatible_in_path {
            return Ok(RemoteBin::InPath);
        }

        let compatible_in_tmp = self
            .check_remote_version(concat!("/tmp/", env!("CARGO_BIN_NAME")))
            .await
            .wrap_err("checking for install in /tmp")?;
        if compatible_in_tmp {
            return Ok(RemoteBin::InTmp);
        }

        Ok(RemoteBin::Missing)
    }

    async fn check_remote_version(
        &self,
        path: &str,
    ) -> std::result::Result<bool, color_eyre::eyre::Error> {
        let output = self.session.command(path).arg("--version").output().await;

        match output {
            Ok(remote_version) => compatible_version(remote_version),
            Err(openssh::Error::Remote(io)) if io.kind() == ErrorKind::NotFound => Ok(false),
            Err(other) => Err(other).wrap_err("Error checking if already installed"),
        }
    }

    async fn target_triple(&self) -> Result<Triple> {
        let uname = self
            .session
            .command("uname")
            .arg("--machine")
            .arg("--operating-system")
            .output()
            .await
            .wrap_err("Could not start `uname` on remote machine")?;

        if !uname.stderr.is_empty() {
            let err = String::from_utf8_lossy(&uname.stderr);
            return Err(eyre!("uname on remote machine returned error"))
                .with_note(|| format!("error: {err}"));
        }

        let stdout = String::from_utf8_lossy(&uname.stdout);
        match stdout.trim() {
            "x86_64 GNU/Linux" => Ok(Triple("x86_64-unknown-linux-musl")),
            "aarch64 GNU/Linux" => Ok(Triple("aarch64-unknown-linux-musl")),
            _ => Err(eyre!("unsupported remote target: `{stdout}`")),
        }
    }
}

fn compatible_version(remote_version: std::process::Output) -> Result<bool> {
    if !remote_version.stderr.is_empty() {
        let err = String::from_utf8_lossy(&remote_version.stderr);
        return Err(eyre!("{}", err.trim()).wrap_err("shell on remote machine returned error"));
    }

    let remote_version = String::from_utf8_lossy(&remote_version.stdout);
    let remote_version = remote_version
        .strip_prefix("zcrab ")
        .ok_or_eyre("version string on remote should start with `zcrab `")
        .with_note(|| format!("Version string: {remote_version}"))?;
    let remote_version = semver::Version::parse(remote_version)
        .wrap_err("remote version is not a valid semver string")
        .with_note(|| format!("Version string: {remote_version}"))?;

    let this_version =
        semver::Version::parse(env!("CARGO_PKG_VERSION")).expect("cargo version is semver");
    if this_version.major == 0 || remote_version.major == 0 {
        Ok(remote_version == this_version)
    } else {
        Ok(remote_version.major == this_version.major)
    }
}

enum RemoteBin {
    InTmp,
    InPath,
    Missing,
}

impl RemoteBin {
    fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }
}

struct Triple(&'static str);

fn basic_builds(triple: &Triple) -> Result<&[u8]> {
    match triple.0 {
        "aarch64-unknown-linux" => Ok(include_bytes!(
            "../builds_without_ssh/aarch64_unknown_linux"
        )),
        "x86_64-unknown-linux-musl" => Ok(include_bytes!(
            "../builds_without_ssh/x86_64-unknown-linux-musl"
        )),
        _ => Err(eyre!("No binary included for architecture: {}", triple.0)),
    }
}

pub(crate) fn test() -> Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .build()
        .expect("should always be able to start a tokio runtime");

    rt.block_on(async { test_inner().await })?;
    Ok(())
}

async fn test_inner() -> Result<()> {
    let remote = Connection::new("asgard").await?;
    if !remote
        .has_compatible_version_installed()
        .await
        .wrap_err("Could not check if zcrab is already installed")?
        .is_missing()
    {
        let triple = remote.target_triple().await?;
        let binary = basic_builds(&triple)?;
        remote.copy_basic_build(binary).await?;
        assert!(
            !remote
                .has_compatible_version_installed()
                .await?
                .is_missing()
        );
    }

    Ok(())
}
