use chrono::Utc;
use color_eyre::eyre::{Context, eyre};
use color_eyre::{Result, Section};
use itertools::Itertools;
use std::str::FromStr;
use std::time::Duration;

use crate::zfs::SnapshotMetadata;

// We use this property to control the retention policy.  Check readme.md, but also
// check_age, ZFS::list_snapshots, and ZFS::list_datasets_for_snapshot.
pub const PROPERTY_SNAPKEEP: &str = "at.rollc.at:snapkeep";

// Describes the number of snapshots to keep for each period.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct RetentionPolicy(pub Vec<RetentionRule>);

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct RetentionRule {
    snapshot_period: Duration,
    retained_copies: usize,
}

impl Ord for RetentionRule {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.snapshot_period.cmp(&other.snapshot_period)
    }
}

impl PartialOrd for RetentionRule {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for RetentionRule {
    type Err = color_eyre::Report;

    fn from_str(rule: &str) -> Result<Self> {
        let valid_syntax = [
            ('s', 1),
            ('m', 60),
            ('h', 60 * 60),
            ('d', 60 * 60 * 24),
            ('w', 60 * 60 * 24 * 7),
            ('y', 60 * 60 * 24 * 365),
        ]
        .map(|(u, seconds)| (u, Duration::from_secs(seconds)));
        for (unit, unit_duration) in valid_syntax {
            if let Some((unit_amount, retained_copies)) = rule.split_once(unit) {
                let unit_amount = unit_amount
                    .parse::<usize>()
                    .wrap_err("Could not parse duration between snapshots")
                    .with_note(|| format!("Rule input: {rule}"))?;
                let retained_copies = retained_copies
                    .parse::<usize>()
                    .wrap_err("Could not parse number of copies to keep")
                    .with_note(|| format!("Rule input: {rule}"))?;
                return Ok(Self {
                    snapshot_period: Duration::from(unit_duration.mul_f64(unit_amount as f64)),
                    retained_copies,
                });
            }
        }

        Err(
            eyre!("No valid time unit found in rule: '{rule}'").with_note(|| {
                format!(
                    "Valid patterns are: {}",
                    valid_syntax.iter().map(|(pat, _)| pat).join("|")
                )
            }),
        )
    }
}

impl FromStr for RetentionPolicy {
    type Err = color_eyre::Report;

    fn from_str(s: &str) -> Result<Self> {
        let mut rules: Vec<_> = s
            .split(':')
            .map(RetentionRule::from_str)
            .collect::<Result<_, _>>()?;
        rules.sort_unstable();

        let policy = RetentionPolicy(rules);
        if policy.0.is_empty() {
            Err(eyre!(
                "If a dataset has a retention policy it needs to have at last one rule"
            ))
        } else {
            Ok(policy)
        }
    }
}

impl RetentionPolicy {
    pub fn shortest_period(&self) -> Duration {
        self.0
            .iter()
            .map(|rule| rule.snapshot_period)
            .min()
            .expect("Retention policy requires a minimum set")
    }

    pub(crate) fn checker(&self) -> PolicyChecker<'_> {
        PolicyChecker {
            rules_shortest_resention_first: &self.0,
        }
    }
}

pub struct PolicyChecker<'a> {
    rules_shortest_resention_first: &'a [RetentionRule],
}

impl PolicyChecker<'_> {
    pub(crate) fn rejected<'a>(
        &self,
        snapshots_newest_first: &'a [SnapshotMetadata],
    ) -> Vec<&'a SnapshotMetadata> {
        let mut to_remove: Vec<_> = snapshots_newest_first.iter().collect();
        for rule in self.rules_shortest_resention_first {
            for i in 0..rule.retained_copies {
                let range = rule.snapshot_period.mul_f64(i as f64)
                    ..rule.snapshot_period.mul_f64((i + 1) as f64);
                for (j, snapshot) in to_remove.iter().enumerate() {
                    let elapsed = Utc::now()
                        .signed_duration_since(snapshot.created)
                        .to_std()
                        .unwrap_or(Duration::ZERO);
                    dbg!(&snapshot.name, elapsed, &range);
                    if range.contains(&elapsed) {
                        dbg!(&to_remove);
                        to_remove.remove(j);
                        dbg!(&to_remove);
                        break;
                    }
                }
            }
        }

        to_remove
    }
}

#[cfg(test)]
mod tests {
    use byte_unit::Byte;
    use chrono::Utc;

    use super::*;

    fn test_snapshot(age: Duration, name: String) -> SnapshotMetadata {
        let now = Utc::now();
        SnapshotMetadata {
            name,
            created: now - age,
            used: Byte::from_bytes(0),
        }
    }

    macro_rules! aged {
        ($amount:literal m) => {
            test_snapshot(Duration::from_secs(60 * $amount), format!("{}m", $amount))
        };
        ($amount:literal s) => {
            test_snapshot(Duration::from_secs($amount), format!("{}s", $amount))
        };
    }

    #[test]
    fn kept_util_amount_times_period() {
        let policy = RetentionPolicy::from_str("10m2").unwrap();
        let snapshots = [aged!(9 m), aged!(19 m), aged!(29 m)];
        let checker = policy.checker();
        let rejected = checker.rejected(&snapshots);
        let rejected = rejected.iter().map(|s| (*s).clone()).collect::<Vec<_>>();
        assert_eq!(&rejected, &snapshots[2..]);
    }

    #[test]
    fn retained_short_do_not_count_to_retained_long() {
        let policy = RetentionPolicy::from_str("50s2:10m2").unwrap();
        let snapshots = [
            aged!(40 s),
            aged!(80 s),
            aged!(120 s),
            aged!(9 m),
            aged!(19 m),
            aged!(29 m),
        ];
        let checker = policy.checker();
        let rejected = checker.rejected(&snapshots);
        // note:
        // algo: take oldest from period..2*period
        let a = rejected.iter().map(|s| (*s).clone()).collect::<Vec<_>>();
        let b = [aged!(9 m), aged!(29 m)];
        assert!(a.iter().zip(b).all(|(a, b)| a.name == b.name));
    }
}
