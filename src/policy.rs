use chrono::Utc;
use color_eyre::eyre::{Context, eyre};
use color_eyre::{Result, Section};
use core::fmt;
use itertools::Itertools;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::str::FromStr;
use std::time::Duration;

use crate::zfs::SnapshotMetadata;

// User property names must contain a colon (":") character to distinguish them from native
// properties.  They may contain lowercase letters, numbers, and the following punctuation
// characters: colon (":"), dash ("-"), period ("."), and underscore ("_").  The expected
// convention is that the property name is divided into two portions such as
// module:property, but this namespace is not enforced by ZFS.  User property names can be
// at most 256 characters, and cannot begin with a dash ("-").

pub const ZFS_PROPERTY: &str = concat!(env!("CARGO_PKG_NAME"), ":policy");

// Describes the number of snapshots to keep for each period.
#[derive(PartialEq, Eq, Hash, Clone)]
pub struct RetentionPolicy(pub Vec<RetentionRule>);

impl fmt::Debug for RetentionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for rule in self.0.iter().take(self.0.len().saturating_sub(1)) {
            f.write_fmt(format_args!("{rule:?} "))?;
        }
        if let Some(last) = self.0.last() {
            f.write_fmt(format_args!("{last:?}"))?;
        }

        Ok(())
    }
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct RetentionRule {
    pub snapshot_period: Duration,
    pub retained_copies: usize,
}

const VALID_SYNTAX: [(char, u64); 6] = [
    ('s', 1),
    ('m', 60),
    ('h', 60 * 60),
    ('d', 60 * 60 * 24),
    ('w', 60 * 60 * 24 * 7),
    ('y', 60 * 60 * 24 * 365),
];

impl fmt::Debug for RetentionRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let period = self.snapshot_period.as_secs();
        for (unit, duration) in VALID_SYNTAX.iter().rev() {
            if period % duration == 0 && period / duration > 0 {
                let amount = period / duration;
                return f.write_fmt(format_args!("{amount}{unit}:{}", self.retained_copies));
            }
        }

        f.write_str("RetentionRule should be formattable")
    }
}

impl fmt::Display for RetentionRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const VALID_SYNTAX: [(&str, u64); 6] = [
            ("seconds", 1),
            ("minutes", 60),
            ("hours", 60 * 60),
            ("days", 60 * 60 * 24),
            ("weeks", 60 * 60 * 24 * 7),
            ("years", 60 * 60 * 24 * 365),
        ];

        let period = self.snapshot_period.as_secs();
        for (unit, duration) in VALID_SYNTAX.iter().rev() {
            if period % duration == 0 && period / duration > 0 {
                let amount = period / duration;
                return f.write_fmt(format_args!(
                    "maintain last {amount} snapshots spaced out by {} {unit}",
                    self.retained_copies
                ));
            }
        }

        f.write_str("RetentionRule should be formattable")
    }
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
        for (unit, unit_duration) in VALID_SYNTAX {
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
                    snapshot_period: Duration::from_secs(unit_duration).mul_f64(unit_amount as f64),
                    retained_copies,
                });
            }
        }

        Err(
            eyre!("No valid time unit found in rule: '{rule}'").with_note(|| {
                format!(
                    "Valid patterns are: {}",
                    VALID_SYNTAX.iter().map(|(pat, _)| pat).join("|")
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

impl RetentionRule {
    pub fn next_snapshot_in(&self, snapshots: &[SnapshotMetadata]) -> Option<Duration> {
        let mut snapshots_oldest_first = snapshots.iter().collect_vec();
        snapshots_oldest_first.sort();

        let now = Utc::now();
        not_too_old(&snapshots_oldest_first, self)
            .last()
            .map(|snapshot| snapshot.created + self.snapshot_period)
            .map(|next_at| {
                next_at
                    .signed_duration_since(now)
                    .to_std()
                    .unwrap_or(Duration::ZERO)
            })
    }

    pub(crate) fn rejects<'a>(
        &self,
        snapshots_oldest_first: &[&'a SnapshotMetadata],
    ) -> HashSet<&'a SnapshotMetadata> {
        let mut to_remove: HashSet<_> = snapshots_oldest_first.iter().copied().collect();

        let not_too_old = not_too_old(snapshots_oldest_first, self);

        let mut retain_next_newer_than = chrono::DateTime::from_timestamp_millis(0).unwrap();
        for snapshot in not_too_old {
            if snapshot.created >= retain_next_newer_than {
                retain_next_newer_than = snapshot.created + self.snapshot_period;
                to_remove.remove(snapshot);
            }
        }

        to_remove
    }
}

type Retainers<'rules> = HashSet<&'rules RetentionRule>;

pub struct Judgement<'snapshots, 'rules> {
    pub rejected: HashSet<&'snapshots SnapshotMetadata>,
    pub retained: HashMap<&'snapshots SnapshotMetadata, Retainers<'rules>>,
}

impl fmt::Debug for Judgement<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Judgement")
            .field("rejected", &self.rejected.iter().collect::<BTreeSet<_>>())
            .field(
                "retained",
                &self.retained.iter().collect::<BTreeMap<_, _>>(),
            )
            .finish()
    }
}

impl RetentionPolicy {
    pub fn next_snapshot_in(&self, snapshots: &[SnapshotMetadata]) -> Option<Duration> {
        let mut snapshots_oldest_first = snapshots.to_vec();
        snapshots_oldest_first.sort();

        self.0
            .iter()
            .filter_map(|rule| rule.next_snapshot_in(snapshots))
            .min()
    }

    pub fn judge<'snapshots, 'rules>(
        &'rules self,
        snapshots_newest_first: &'snapshots [SnapshotMetadata],
    ) -> Judgement<'snapshots, 'rules> {
        let mut rules_longest_resention_first = self.0.iter().collect_vec();
        rules_longest_resention_first.sort_by(|a, b| b.cmp(a));
        let mut snapshots_oldest_first = snapshots_newest_first.iter().collect_vec();
        snapshots_oldest_first.sort();

        let mut res = Judgement {
            rejected: HashSet::new(),
            retained: snapshots_oldest_first
                .iter()
                .map(|snapshot| {
                    (
                        *snapshot,
                        rules_longest_resention_first.iter().copied().collect(),
                    )
                })
                .collect(),
        };

        for rule in rules_longest_resention_first {
            for snapshot in rule.rejects(&snapshots_oldest_first) {
                let Some(mut retainers) = res.retained.remove(snapshot) else {
                    continue;
                };

                retainers.remove(rule);
                if retainers.is_empty() {
                    res.rejected.insert(snapshot);
                } else {
                    res.retained.insert(snapshot, retainers);
                }
            }
        }

        res
    }
}

fn not_too_old<'a>(
    snapshots_oldest_first: &[&'a SnapshotMetadata],
    rule: &RetentionRule,
) -> impl Iterator<Item = &'a SnapshotMetadata> {
    // This algorithm is nontrivial since there might be jumps larger then
    // `snapshot_period` between snapshots (due to system being offline).

    let mut skip_first_n = 0;
    for n in 0..snapshots_oldest_first.len() {
        let mut keep_next_newer_than = chrono::DateTime::from_timestamp_millis(0).unwrap();
        let mut n_considerd = 0;
        for snapshot in snapshots_oldest_first.iter().skip(n) {
            if snapshot.created >= keep_next_newer_than {
                keep_next_newer_than = snapshot.created + rule.snapshot_period;
                n_considerd += 1;
            }
        }

        if n_considerd <= rule.retained_copies {
            skip_first_n = n;
            break;
        }
    }

    snapshots_oldest_first.iter().skip(skip_first_n).copied()
}

#[cfg(test)]
pub(crate) mod tests {
    use byte_unit::Byte;
    use chrono::Utc;

    use super::*;

    pub(crate) fn test_snapshot(age: Duration, name: String) -> SnapshotMetadata {
        let now = Utc::now();
        SnapshotMetadata {
            name,
            created: now - age,
            used: Byte::from_bytes(0),
        }
    }

    macro_rules! aged {
        ($amount:literal d) => {
            $crate::policy::tests::test_snapshot(
                ::std::time::Duration::from_secs(60 * 60 * 24 * $amount),
                format!("{}d", $amount),
            )
        };
        ($amount:literal h) => {
            $crate::policy::tests::test_snapshot(
                ::std::time::Duration::from_secs(60 * 60 * $amount),
                format!("{}h", $amount),
            )
        };
        ($amount:literal m) => {
            $crate::policy::tests::test_snapshot(
                ::std::time::Duration::from_secs(60 * $amount),
                format!("{}m", $amount),
            )
        };
        ($amount:literal s) => {
            $crate::policy::tests::test_snapshot(
                ::std::time::Duration::from_secs($amount),
                format!("{}s", $amount),
            )
        };
    }
    pub(crate) use aged;

    mod snapshot_creation {
        use super::*;

        #[test]
        fn optimal_interval() {
            let policy = RetentionPolicy::from_str("10m2").unwrap();
            let snapshots = [aged!(5 m), aged!(15 m)];
            let next_in = policy.next_snapshot_in(&snapshots).unwrap();
            assert_eq!(next_in.as_secs_f32().round() as usize, 60 * 5);
        }
    }

    mod snapshot_removal {
        use super::*;

        #[test]
        fn kept_util_amount_times_period() {
            let policy = RetentionPolicy::from_str("10m2").unwrap();
            let snapshots = [aged!(8 m), aged!(19 m), aged!(30 m)];
            let rejected = policy.judge(&snapshots).rejected;
            let rejected = rejected.iter().map(|s| (*s).clone()).collect::<Vec<_>>();
            assert_eq!(&rejected, &snapshots[2..]);
        }

        #[test]
        fn do_not_keep_more_than_copies_to_retain() {
            let policy = RetentionPolicy::from_str("50s2").unwrap();
            let snapshots = [
                aged!(40 s),
                aged!(80 s),
                aged!(120 s),
                aged!(9 m),
                aged!(18 m),
                aged!(29 m),
            ];
            let rejected = dbg!(policy.judge(&snapshots)).rejected;
            assert_eq!(rejected.len(), 4);
        }

        #[test]
        fn retained_short_do_not_count_to_retained_long() {
            let policy = RetentionPolicy::from_str("50s2:10m2").unwrap();
            let snapshots = [
                aged!(38 s),
                aged!(79 s),
                aged!(120 s),
                aged!(7 m),
                aged!(18 m),
                aged!(29 m),
            ];
            let rejected = dbg!(policy.judge(&snapshots)).rejected;
            assert!(rejected.iter().any(|s| s.name == aged!(29 m).name));
            assert!(rejected.iter().any(|s| s.name == aged!(79 s).name));
        }

        #[test]
        fn restores_after_offline() {
            let policy = RetentionPolicy::from_str("40s99:100s99").unwrap();
            let snapshots = [
                aged!(20 s),
                aged!(41 s),
                aged!(82 s),
                aged!(123 s), // recovered after restart
                aged!(284 s),
                aged!(305 s),
                aged!(346 s),
                aged!(388 s), // org start
            ];

            let rejected = dbg!(policy.judge(&snapshots)).rejected;
            assert!(rejected.is_empty());
        }
    }
}
