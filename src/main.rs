use clap::{Parser, Subcommand};
use color_eyre::eyre::eyre;
use color_eyre::{Result, Section};
use libproc::proc_pid;
use service_install::install_system;
use std::fmt::Display;
use std::thread;
use std::time::Duration;

use policy::{RetentionPolicy, ZFS_PROPERTY};
use zfs::{ConfiguredDataSet, SnapshotMetadata, configured_datasets};

mod configure;
mod policy;
mod status;
mod zfs;

fn until_next_snapshot(
    datasets: &[ConfiguredDataSet],
) -> impl Iterator<Item = (Duration, &ConfiguredDataSet)> {
    datasets
        .iter()
        .filter_map(|dataset| dataset.until_next_snapshot().zip(Some(dataset)))
}

type DataSet = String;

fn install() -> Result<()> {
    install_system!()
        .current_exe()
        .unwrap()
        .service_name("zfs-autosnap")
        .on_boot()
        .prepare_install()?
        .install()?;
    Ok(())
}

fn remove() -> Result<()> {
    install_system!().prepare_remove()?.remove()?;
    Ok(())
}

#[derive(Parser, Debug)]
#[command(
    version,
    about,
    long_about = "Usage
    zfs-autosnap <status | snap | gc | help | version
Tips
    use 'zfs set at.rollc.at:snapkeep=h:24,d:30,w:8,m:6,y1 some/dataset' to enable
    use 'zfs set at.rollc.at:snapkeep=- some/dataset@some-snap' to retain
    add 'zfs-autosnap snap' to cron.hourly
    add 'zfs-autosnap gc'   to cron.daily
"
)]
struct Args {
    #[command(subcommand)]
    command: Commands,
    /// In sandbox mode no actual snapshots are removed or created.
    /// Use this for testing a new configuration.
    #[arg(short, long)]
    sandbox: bool,
    /// Prints more information in Status
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Install the deamon to start on boot
    Install,
    /// Remove the deamon
    Remove,
    /// Configure datasets for use
    Configure,
    /// Show Configuration, snapshots and schedule for next snapshot
    Status,
    /// Run the deamon in the foreground in the current terminal
    Run,
}

impl Display for Commands {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Commands::Install => concat!("install the ", env!("CARGO_PKG_NAME"), "deamon"),
            Commands::Remove => concat!("remove the ", env!("CARGO_PKG_NAME"), "deamon"),
            Commands::Configure => {
                concat!("configure datasets for use with ", env!("CARGO_PKG_NAME"))
            }
            Commands::Status => "show configuration, snapshots and schedule for next snapshot",
            Commands::Run => "run the deamon",
        })
    }
}

fn main() -> Result<()> {
    color_eyre::install().unwrap();
    let args = Args::parse();

    match (args.command, proc_pid::am_root() || args.sandbox) {
        (Commands::Install, true) => install(),
        (Commands::Remove, true) => remove(),
        (Commands::Configure, true) => configure::interactive_cli::start(args.sandbox),
        (Commands::Status, _) => status::print_status(args.verbose),
        (Commands::Run, true) => daemon(args.sandbox),
        (command, false) => {
            Err(eyre!("Need root to {command:?}").suggestion("Try running with sudo"))
        }
    }
}

fn daemon(sandbox: bool) -> Result<()> {
    loop {
        let datasets = configured_datasets()?;
        let until_next_check = until_next_snapshot(&datasets)
            .map(|(dur, _)| dur)
            .min()
            .unwrap_or(Duration::from_secs(60 * 10));
        thread::sleep(until_next_check);
        for dataset in need_snapshot(&datasets) {
            if !sandbox {
                let s = zfs::snapshot(dataset)?;
                println!("made snapshot: {}", s.name);
            } else {
                println!("would snapshot dataset: {dataset}");
            }
        }
        for snapshot in need_removal(&datasets) {
            if !sandbox {
                zfs::destroy_snapshot(snapshot)?;
                println!("removed expired snapshot: {}", snapshot.name);
            } else {
                println!("would remove expired snapshot: {}", snapshot.name);
            }
        }
    }
}

fn need_snapshot(datasets: &[ConfiguredDataSet]) -> impl Iterator<Item = &DataSet> {
    until_next_snapshot(datasets)
        .filter(|(until, _)| until.is_zero())
        .map(|(_, dataset)| &dataset.path)
}

fn need_removal(datasets: &[ConfiguredDataSet]) -> impl Iterator<Item = &SnapshotMetadata> {
    datasets.iter().flat_map(|dataset| {
        dataset
            .retention_policy
            .judge(&dataset.sorted_snapshots)
            .rejected
    })
}
