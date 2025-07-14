use std::io::Write;
use std::time::Duration;

use crate::zfs::{ConfiguredDataSet, SnapshotMetadata, configured_datasets};
use color_eyre::Result;
use humantime::format_duration;
use itertools::Itertools;

pub fn print_status(verbose: bool) -> Result<()> {
    let datasets = configured_datasets()?;
    write_status(&mut std::io::stdout(), &datasets, verbose);
    Ok(())
}

pub fn write_status(f: &mut impl Write, datasets: &[ConfiguredDataSet], verbose: bool) {
    if datasets.is_empty() {
        writeln!(
            f,
            "No datasets configured for auto snapshotting by this tool"
        )
        .unwrap();
        return;
    };

    if verbose {
        writeln!(f, "Configured datasets").unwrap();
        write_configured_datasets_section_verbose(f, datasets);
        writeln!(f).unwrap();
        writeln!(f, "Snapshot to be removed").unwrap();
        write_rejected_snapshot_state_verbose(f, datasets);
    } else {
        writeln!(f, "Configured datasets").unwrap();
        write_configured_datasets_section(f, datasets);
        writeln!(f, "Snapshot to be removed").unwrap();
        write_rejected_snapshot_state(f, datasets);
    }
}

fn write_rejected_snapshot_state(f: &mut impl Write, datasets: &[ConfiguredDataSet]) {
    for dataset in datasets {
        let judgement = dataset.retention_policy.judge(&dataset.sorted_snapshots);
        let mut rejected = judgement.rejected.into_iter().collect_vec();
        rejected.sort();
        let mut rejected = rejected.into_iter();

        let Some(first) = rejected.next() else {
            continue;
        };
        write!(f, "  {}: {}", dataset.path, first.name).unwrap();
        let mut current_line_len = 4 + first.name.chars().count();

        for snapshot in rejected {
            if current_line_len + snapshot.name.chars().count() <= 80 {
                write!(f, " {}", snapshot.name).unwrap();
            } else {
                writeln!(f, "    {}", snapshot.name).unwrap();
                current_line_len += 4 + snapshot.name.chars().count();
            }
        }
    }
}

fn write_rejected_snapshot_state_verbose(f: &mut impl Write, datasets: &[ConfiguredDataSet]) {
    for dataset in datasets {
        let judgement = dataset.retention_policy.judge(&dataset.sorted_snapshots);
        let mut rejected = judgement.rejected.into_iter().collect_vec();
        rejected.sort();

        if rejected.is_empty() {
            continue;
        }

        let name_width = dataset
            .sorted_snapshots
            .iter()
            .map(|s| s.name.chars().count())
            .max()
            .unwrap_or(0);
        let created_width = dataset
            .sorted_snapshots
            .iter()
            .map(|s| {
                s.created
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                    .chars()
                    .count()
            })
            .max()
            .unwrap_or(0);
        let used_width = dataset
            .sorted_snapshots
            .iter()
            .map(|s| {
                s.used
                    .get_appropriate_unit(false)
                    .to_string()
                    .chars()
                    .count()
            })
            .max()
            .unwrap_or(0);

        let column1_width = name_width.max(" Name ".chars().count());
        let column2_width = created_width.max(" #Snapshots ".chars().count());
        let column3_width = used_width.max(" Refers to ".chars().count());

        writeln!(f, "  {}", dataset.path).unwrap();
        writeln!(
            f,
            "    {: <column1_width$} | {: <column2_width$} | {: <column3_width$}",
            "Name", "#Created", "Refers to"
        )
        .unwrap();

        for SnapshotMetadata {
            name,
            created,
            used,
        } in rejected
        {
            writeln!(
                f,
                "    {name: <column1_width$} \
                | {: <column2_width$} \
                | {: <column3_width$}",
                created.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                used.get_appropriate_unit(false).to_string()
            )
            .unwrap();
        }
    }
}

fn write_configured_datasets_section_verbose(f: &mut impl Write, datasets: &[ConfiguredDataSet]) {
    for dataset in datasets {
        let next_snapshot_in = dataset
            .until_next_snapshot()
            .map(|d| d - Duration::from_nanos(d.subsec_nanos() as u64))
            .map(|d| format_duration(d).to_string())
            .unwrap_or("never".to_string());
        let ConfiguredDataSet {
            path,
            retention_policy,
            sorted_snapshots,
        } = dataset;

        writeln!(f, "  {path}").unwrap();
        writeln!(f, "    numbers of snapshots: {}", sorted_snapshots.len()).unwrap();
        writeln!(f, "    next snapshot in: {next_snapshot_in}").unwrap();
        writeln!(f, "    retention policy:").unwrap();
        for rule in &retention_policy.0 {
            writeln!(f, "    - {rule}",).unwrap();
        }
    }
}

fn write_configured_datasets_section(f: &mut impl Write, datasets: &[ConfiguredDataSet]) {
    let path_width = datasets
        .iter()
        .map(|d| d.path.chars().count())
        .max()
        .unwrap_or(0);
    let count_width = datasets
        .iter()
        .map(|d| d.sorted_snapshots.len().ilog10() as usize + 1)
        .max()
        .unwrap_or(0);
    let rules_width = datasets
        .iter()
        .map(|d| format!("{:?}", d.retention_policy).chars().count())
        .max()
        .unwrap_or(0);
    let column1_width = path_width.max(" Name ".chars().count());
    let column2_width = count_width.max(" #Snapshots ".chars().count());
    let column3_width = rules_width.max(" Rules ".chars().count());

    writeln!(
        f,
        "  {: <column1_width$} | {: <column2_width$} | {: <column3_width$} | Next snapshot",
        "Name", "#Snapshots", "Rules"
    )
    .unwrap();

    for dataset in datasets {
        let next_snapshot_in = dataset
            .until_next_snapshot()
            .map(|d| d - Duration::from_nanos(d.subsec_nanos() as u64))
            .map(|d| format_duration(d).to_string())
            .unwrap_or("never".to_string());
        let ConfiguredDataSet {
            path,
            retention_policy,
            sorted_snapshots,
        } = dataset;

        let retention_policy = format!("{retention_policy:?}");
        writeln!(
            f,
            "  {path: <column1_width$} \
                | {: <column2_width$} \
                | {retention_policy: <column3_width$} \
                | {next_snapshot_in}",
            sorted_snapshots.len(),
        )
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::policy::RetentionPolicy;
    use crate::policy::tests::aged;

    fn test_datasets() -> [ConfiguredDataSet; 2] {
        [
            ConfiguredDataSet {
                path: String::from("/home/david/Documents"),
                retention_policy: RetentionPolicy::from_str("15m8:1h48:1d14:1w20").unwrap(),
                sorted_snapshots: Box::new([
                    aged!(10 m),
                    aged!(36 m),
                    aged!(52 m),
                    aged!(1 d),
                    aged!(2 d),
                    aged!(3 d),
                ]),
            },
            ConfiguredDataSet {
                path: String::from("/home/david/Downloads"),
                retention_policy: RetentionPolicy::from_str("1h2:2d2").unwrap(),
                sorted_snapshots: Box::new([
                    aged!(1 h),
                    aged!(2 h),
                    aged!(3 h),
                    aged!(1 d),
                    aged!(2 d),
                    aged!(3 d),
                ]),
            },
        ]
    }

    #[test]
    fn verbose() {
        let mut output = Vec::new();
        write_status(&mut output, &test_datasets(), true);
        let output = String::from_utf8(output).unwrap();
        println!("{output}");
    }

    #[test]
    fn terse() {
        let mut output = Vec::new();
        write_status(&mut output, &test_datasets(), false);
        let output = String::from_utf8(output).unwrap();
        println!("{output}");
    }
}
