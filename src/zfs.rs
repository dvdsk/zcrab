use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use byte_unit::Byte;
use chrono::prelude::*;
use color_eyre::eyre::eyre;
use itertools::Itertools;
use color_eyre::Result;

use crate::{DataSet, PROPERTY_SNAPKEEP, RetentionPolicy};

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct SnapshotMetadata {
    pub name: String,
    pub created: chrono::DateTime<Utc>,
    pub used: Byte,
}

impl core::fmt::Debug for SnapshotMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

impl PartialOrd for SnapshotMetadata {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SnapshotMetadata {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.created.timestamp().cmp(&other.created.timestamp())
    }
}

impl SnapshotMetadata {
    pub(crate) fn dataset(&self) -> &str {
        self.name
            .split_once('@')
            .expect("Snapshot names always contain @")
            .0
    }

    pub(crate) fn elapsed(&self) -> Duration {
        Utc::now()
            .signed_duration_since(self.created)
            .to_std()
            .unwrap_or(Duration::ZERO)
    }
}

pub fn snapshot(dataset: &str) -> Result<SnapshotMetadata> {
    // Take a snapshot of the given dataset, with an auto-generated name.
    let now = Utc::now();
    let name = format!(
        "{}@{}-autosnap",
        dataset,
        now.to_rfc3339_opts(SecondsFormat::Secs, true)
    );
    call_do("snap", &[&name])?;
    Ok(SnapshotMetadata {
        name: name.clone(),
        created: now,
        used: parse_used(&get_property(&name, "used")?)?,
    })
}

pub fn add_snapshots() -> Result<HashMap<DataSet, Box<[SnapshotMetadata]>>> {
    // List all snapshots under our control.
    // zfs list -H -t snapshot -o name,creation,used,at.rollc.at:snapkeep
    let lines = call_read(
        "list",
        &[
            "-t",
            "snapshot",
            "-o",
            &format!("name,creation,used,{}", PROPERTY_SNAPKEEP),
        ],
    )?;

    let snapshots = parse_snapshots(lines)?;
    let mut snapshots: HashMap<_, _> = snapshots
        .into_iter()
        .map(|meta| (meta.dataset().to_string(), meta))
        .into_group_map();

    for list in snapshots.values_mut() {
        // sort newest (largest timestamp) first
        list.sort_by(|a, b| b.cmp(a));
    }
    // make it so the list order can no longer be changed
    let snapshots = snapshots
        .into_iter()
        .map(|(dataset, snapshot_list)| (dataset, snapshot_list.into_boxed_slice()))
        .collect();

    Ok(snapshots)
}

fn parse_snapshots(lines: Vec<Vec<String>>) -> Result<Vec<SnapshotMetadata>> {
    let mut snapshots = Vec::with_capacity(lines.len());
    for line in lines {
        // Skip snapshots that don't have the 'at.rollc.at:snapkeep' property.
        //
        // FIXME dvdsk: it does not. That filtering is done by zfs list in `list_snapshots`.
        //
        // This works both for datasets where a snapshot did not inherit the property
        // (which means the dataset should not be managed), and for explicitly marking a
        // snapshot to be retained / opted out.
        //
        match line.as_slice() {
            [_, _, _, snapkeep] if snapkeep == "-" => continue,
            [name, created, used, _] => {
                let metadata = SnapshotMetadata {
                    name: name.to_string(),
                    created: parse_datetime(created)?,
                    used: parse_used(used)?,
                };
                snapshots.push(metadata)
            }
            _ => return Err(eyre!("list snapshots parse error")),
        }
    }
    Ok(snapshots)
}

fn parse_datetime(s: &String) -> Result<chrono::DateTime<chrono::Utc>> {
    let r = {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%a %b %e %H:%M %Y") {
            dt
        } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%s") {
            dt
        } else {
            return Err(eyre!("can't parse datetime: {}", s))
        }
    };
    Ok(chrono::Utc.from_utc_datetime(&r))
}

pub fn get_property(dataset: &str, property: &str) -> Result<String> {
    // Get a single named property on given dataset.
    // zfs get -H -o value $property $dataset
    Ok(call_read("get", &["-o", "value", property, dataset])?
        .get(0)
        .unwrap()[0]
        .clone())
}

pub struct ConfiguredDataSet {
    pub path: String,
    pub retention_policy: RetentionPolicy,
    // newest to oldest
    pub sorted_snapshots: Box<[SnapshotMetadata]>,
}

pub fn configured_datasets() -> Result<Vec<ConfiguredDataSet>> {
    let mut snapshots = add_snapshots()?;
    let datasets = iter_datasets()?;
    datasets.map_ok(|(name, policy)| {
        ConfiguredDataSet {
            sorted_snapshots: snapshots.remove(&name).unwrap_or_default(),
            path: name,retention_policy: policy
        }
    }).collect()
}

pub fn iter_datasets() -> Result<impl Iterator<Item = Result<(String, RetentionPolicy)>>> {
    // Which datasets should get a snapshot?
    // zfs get -H -t filesystem,volume -o name,value at.rollc.at:snapkeep
    Ok(call_read(
        "get",
        &[
            "-t",
            "filesystem,volume",
            "-o",
            "name,value",
            PROPERTY_SNAPKEEP,
        ],
    )?
    .into_iter()
    .map(|pairs| {
        pairs
            .try_into()
            .expect("get with two -o values returns pairs")
    })
    .filter(|[_, retention]: &[String; 2]| retention != "-")
    .map(|[path, retention]| 
        // not proper err handling place
        {
        let res = (path, RetentionPolicy::from_str(&retention)?);
        Ok(res)
        }
    ))
}

pub fn destroy_snapshot(snapshot: &SnapshotMetadata) -> Result<()> {
    // This will destroy the named snapshot. Since ZFS has a single verb for destroying
    // anything, which could cause irreparable harm, we double check that the name we
    // got passed looks like a snapshot name, and return an error otherwise.
    if !snapshot.name.contains('@') {
        return Err(eyre!("Tried to destroy something that is not a snapshot"));
    }
    // zfs destroy -H ...@...
    call_do("destroy", &[&snapshot.name])
}

fn call_read(action: &str, args: &[&str]) -> Result<Vec<Vec<String>>> {
    // Helper function to get/list datasets and their properties into a nice table.
    Ok(subprocess::Exec::cmd("zfs")
        .arg(action)
        .arg("-H")
        .args(args)
        .stdout(subprocess::Redirection::Pipe)
        .capture()?
        .stdout_str()
        .lines()
        .filter(|&s| !s.is_empty())
        .map(|s| s.split('\t').map(|ss| ss.to_string()).collect())
        .collect())
}

fn call_do(action: &str, args: &[&str]) -> Result<()> {
    // Perform a side effect, like snapshot or destroy.
    if subprocess::Exec::cmd("zfs")
        .arg(action)
        .args(args)
        .join()?
        .success()
    {
        Ok(())
    } else {
        Err(eyre!("zfs command error"))
    }
}

fn parse_used(x: &str) -> Result<Byte> {
    // The zfs(1) commandline tool says e.g. 1.2M but means 1.2MiB,
    // so we mash it to make byte_unit parsing happy.
    match x.chars().last() {
        Some('K' | 'M' | 'G' | 'T' | 'P' | 'E' | 'Z') => Ok(Byte::from_str(x.to_owned() + "iB")?),
        _ => Ok(Byte::from_str(x)?),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_snapshots() {
        let lines = vec![
            // name, created, used, snapkeep
            vec![
                String::from("first"),
                String::from("Sat Oct 2 09:59 2021"),
                String::from("13G"),
                String::from("at.rollc.at:snapkeep=h24d30w8m6y1"),
            ],
            vec![
                String::from("skip"),
                String::from("Sat Oct 1 19:59 2021"),
                String::from("2G"),
                String::from("-"),
            ],
        ];
        let snapshots = parse_snapshots(lines).unwrap();
        assert_eq!(
            snapshots,
            vec![SnapshotMetadata {
                name: String::from("first"),
                created: Utc.from_utc_datetime(
                    &chrono::NaiveDateTime::parse_from_str(
                        "Sat Oct 2 09:59 2021",
                        "%a %b %e %H:%M %Y",
                    )
                    .unwrap(),
                ),
                used: Byte::from(13u64 * 1024 * 1024 * 1024),
            }]
        );
    }

    #[test]
    fn test_parse_snapshots_empty() {
        let lines = vec![];
        let snapshots = parse_snapshots(lines).unwrap();
        assert_eq!(snapshots, vec![]);
    }

    #[test]
    fn test_parse_snapshots_invalid_row() {
        let lines = vec![vec![String::from("unexpected")]];
        let err = parse_snapshots(lines).unwrap_err();
        assert_eq!(err.to_string(), "list snapshots parse error");
    }

    #[test]
    fn test_parse_snapshots_invalid_date() {
        let lines = vec![vec![
            String::from("first"),
            String::from("2 Oct 2021 9:52AM"),
            String::from("3G"),
            String::from("at.rollc.at:snapkeep=h24d30w8m6y1"),
        ]];
        let err = parse_snapshots(lines).unwrap_err();
        assert!(err.to_string().starts_with("can't parse datetime:"));
    }
}
