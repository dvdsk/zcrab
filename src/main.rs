use clap::{Parser, Subcommand};
use color_eyre::Result;
use service_install::install_system;
use std::thread;
use std::time::Duration;

use tomato::{PROPERTY_SNAPKEEP, RetentionPolicy};
use zfs::{ConfiguredDataSet, SnapshotMetadata, configured_datasets};

mod tomato;
mod zfs;

// fn gc_find(snapshots: &HashMap<DataSet, Vec<SnapshotMetadata>>) -> Result<AgeCheckResult> {
//     // List all snapshots we're interested in, group them by dataset, check them against
//     // their parent dataset's retention policy, and aggregate them into the final result,
//     // which can be presented to the user (do_status()) or the garbage collector (do_gc()).
//     let mut keep = vec![];
//     let mut delete = vec![];
//     for (key, group) in snapshots.iter() {
//         let policy = RetentionPolicy::from_str(&zfs::get_property(key, PROPERTY_SNAPKEEP)?)
//             .map_err(|()| "unable to parse retention policy")?;
//         let check = policy.check_age(group);
//         keep.extend(check.keep);
//         delete.extend(check.delete);
//     }
//     Ok(AgeCheckResult { keep, delete })
// }

// fn status() -> Result<()> {
//     // Present a nice summary to the user.
//     let check = gc_find()?;
//     if !check.keep.is_empty() {
//         println!(
//             "keep: {}",
//             Byte::from_bytes(check.keep.iter().map(|s| s.used.get_bytes()).sum::<u128>())
//                 .get_appropriate_unit(true)
//         );
//         for s in check.keep {
//             println!(
//                 "keep: {}\t{}\t{}",
//                 s.name,
//                 s.created.to_rfc3339_opts(SecondsFormat::Secs, true),
//                 s.used.get_appropriate_unit(true)
//             );
//         }
//     }
//     if !check.delete.is_empty() {
//         println!(
//             "delete: {}",
//             Byte::from_bytes(
//                 check
//                     .delete
//                     .iter()
//                     .map(|s| s.used.get_bytes())
//                     .sum::<u128>()
//             )
//             .get_appropriate_unit(true)
//         );
//         for s in check.delete {
//             println!(
//                 "delete: {}\t{}\t{}",
//                 s.name,
//                 s.created.to_rfc3339_opts(SecondsFormat::Secs, true),
//                 s.used.get_appropriate_unit(true)
//             );
//         }
//     }
//     Ok(())
// }

// fn do_gc() -> Result<()> {
//     // Garbage collection. Find all snapshots to delete, and delete them without asking
//     // twice. If you need to only check the status, use do_status.
//     let check = gc_find()?;
//     if !check.delete.is_empty() {
//         println!(
//             "delete: {}",
//             Byte::from_bytes(
//                 check
//                     .delete
//                     .iter()
//                     .map(|s| s.used.get_bytes())
//                     .sum::<u128>()
//             )
//             .get_appropriate_unit(true)
//         );
//     }
//     for s in check.delete {
//         println!(
//             "delete: {}\t{}\t{}",
//             s.name,
//             s.created.to_rfc3339_opts(SecondsFormat::Secs, true),
//             s.used.get_appropriate_unit(true)
//         );
//         zfs::destroy_snapshot(s)?;
//     }
//     Ok(())
// }

fn until_next_snapshot<'a>(
    datasets: &'a [ConfiguredDataSet],
) -> impl Iterator<Item = (Duration, &'a ConfiguredDataSet)> {
    datasets.iter().filter_map(|dataset| {
        dataset
            .sorted_snapshots
            .iter()
            .filter(|snapshot| snapshot.dataset() == dataset.path)
            .max_by_key(|snapshot| snapshot.created.clone())
            .map(|newest| {
                (
                    dataset
                        .retention_policy
                        .shortest_period()
                        .saturating_sub(newest.elapsed()),
                    dataset,
                )
            })
    })
}

fn duration_until_next_snapshot_needed<'a>(datasets: &'a [ConfiguredDataSet]) -> Option<Duration> {
    until_next_snapshot(datasets)
        .map(|(duration_until, _)| duration_until)
        .min()
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
    sandbox: bool,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Install,
    Remove,
    // Status,
    // Test,
    Run,
}

fn main() -> Result<()> {
    let args = Args::parse();

    match args.command {
        Commands::Install => install(),
        Commands::Remove => remove(),
        // Commands::Status => status(),
        // Commands::Test => list_configured_datasets().map(|_| ()),
        Commands::Run => daemon(args.sandbox),
    }
}

fn daemon(sandbox: bool) -> Result<()> {
    loop {
        let datasets = configured_datasets()?;
        let until_next_check =
            duration_until_next_snapshot_needed(&datasets).unwrap_or(Duration::from_secs(60 * 10));
        thread::sleep(until_next_check);
        for dataset in need_snapshot(&datasets) {
            if !sandbox {
                let s = zfs::snapshot(&dataset)?;
                println!("made snapshot: {}", s.name);
            } else {
                println!("would snapshot dataset: {}", dataset);
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

fn need_snapshot<'a>(datasets: &'a [ConfiguredDataSet]) -> impl Iterator<Item = &'a DataSet> {
    until_next_snapshot(datasets)
        .filter(|(until, _)| until.is_zero())
        .map(|(_, dataset)| &dataset.path)
}

fn need_removal(datasets: &[ConfiguredDataSet]) -> impl Iterator<Item = &SnapshotMetadata> {
    datasets
        .iter()
        .map(|dataset| {
            let checker = dataset.retention_policy.checker();
            let rejects = checker.rejected(&dataset.sorted_snapshots);
            rejects
        })
        .flatten()
}
