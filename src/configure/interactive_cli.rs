use core::fmt;
use std::fmt::Display;
use std::time::Duration;

use color_eyre::eyre::Result;
use inquire::ui::RenderConfig;
use inquire::validator::Validation;
use inquire::{CustomType, Select, prompt_confirmation};
use itertools::Itertools;

use crate::policy::{RetentionPolicy, RetentionRule};
use crate::zfs;

use super::Configured;

enum Wizard {
    SetupDataset,
    ChangeDataset,
}

impl Display for Wizard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Wizard::SetupDataset => f.write_str("Set up a dataset for auto snapshotting"),
            Wizard::ChangeDataset => f.write_str("Change the auto snapshot settings for a dataset"),
        }
    }
}

pub fn start(sandbox: bool) -> Result<()> {
    let mut unconfigured = zfs::iter_unconfigured_datasets()?.peekable();
    let mut configured = zfs::iter_configured_datasets()?.peekable();

    let mut options = Vec::new();
    options.extend(unconfigured.peek().map(|_| Wizard::SetupDataset));
    options.extend(configured.peek().map(|_| Wizard::ChangeDataset));

    let action = if options.len() > 1 {
        Select::new("What do you want to do?", options)
            .without_filtering()
            .without_help_message()
            .prompt_skippable()?
    } else {
        Some(options.remove(0))
    };

    let changed = match action {
        Some(Wizard::SetupDataset) => setup_dataset(unconfigured),
        Some(Wizard::ChangeDataset) => change_dataset(configured),
        None => return Ok(()),
    }?;

    if let Some(changed) = changed
        && !sandbox
    {
        changed.store_and_apply_retention_policy()?
    }

    Ok(())
}

fn setup_dataset(unconfigured: impl Iterator<Item = String>) -> Result<Option<Configured>> {
    let Some(dataset) = Select::new(
        "Which dataset do you wish to set up auto snapshotting for?",
        unconfigured.collect(),
    )
    .prompt_skippable()?
    else {
        return Ok(None);
    };

    let Some(rule) = new_rule()? else {
        return Ok(None);
    };
    let mut policy = RetentionPolicy(vec![rule]);

    loop {
        if !prompt_confirmation("Would you like to add another rule? (y/n)")? {
            return Ok(Some(Configured {
                name: dataset,
                policy,
            }));
        }

        let Some(rule) = new_rule()? else {
            return Ok(None);
        };
        policy.0.push(rule);
    }
}

enum ChangeDataset {
    AddRule,
    RemoveRule,
}

impl fmt::Display for ChangeDataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeDataset::AddRule => f.write_str("Add another auto snapshot rule"),
            ChangeDataset::RemoveRule => f.write_str("Remove a snapshot rule"),
        }
    }
}

fn change_dataset(
    configured: impl Iterator<Item = Result<(String, RetentionPolicy)>>,
) -> Result<Option<Configured>> {
    let configured: Vec<_> = configured
        .map_ok(|(name, policy)| Configured { name, policy })
        .collect::<Result<_, _>>()?;
    let Some(mut to_modify) = Select::new(
        "For which dataset do you wish to modify the auto snapshotting settings?",
        configured,
    )
    .prompt_skippable()?
    else {
        return Ok(None);
    };

    loop {
        let choice = Select::new(
            "What do want to do?",
            vec![ChangeDataset::AddRule, ChangeDataset::RemoveRule],
        )
        .prompt_skippable()?;

        match choice {
            Some(ChangeDataset::AddRule) => {
                if let Some(new_rule) = new_rule()? {
                    to_modify.policy.0.push(new_rule);
                }
            }
            Some(ChangeDataset::RemoveRule) => remove_rule(&mut to_modify.policy)?,
            None => return Ok(Some(to_modify)),
        }
    }
}

fn remove_rule(policy: &mut RetentionPolicy) -> Result<()> {
    let Some(to_remove) =
        Select::new("Which rule do you wish to remove?", policy.0.clone()).prompt_skippable()?
    else {
        return Ok(());
    };

    let to_remove = policy
        .0
        .iter()
        .position(|r| *r == to_remove)
        .expect("The rule was there in Select prompt");
    policy.0.remove(to_remove);

    Ok(())
}

fn new_rule() -> Result<Option<RetentionRule>> {
    let Some(snapshot_period) = CustomType::<Duration> {
        message: "What should be the period between snapshots?",
        starting_input: None,
        default: None,
        placeholder: None,
        help_message: Some("Formats like 2min, 5m, 5minutes all work"),
        formatter: &|d| humantime::format_duration(d).to_string(),
        default_value_formatter: &|d| humantime::format_duration(d).to_string(),
        parser: &|s| humantime::parse_duration(s).map_err(|_| ()),
        validators: Vec::new(),
        error_message: String::new(),
        render_config: RenderConfig::default(),
    }
    .prompt_skippable()?
    else {
        return Ok(None);
    };

    let Some(retained_copies) = CustomType::<usize>::new("How many copies should be retained?")
        .with_validator(|v: &usize| {
            Ok(if *v > 0 {
                Validation::Valid
            } else {
                Validation::Invalid(inquire::validator::ErrorMessage::Custom(
                    "Number of copies must be larger then zero".to_string(),
                ))
            })
        })
        .with_error_message("Type a whole number larger then zero")
        .prompt_skippable()?
    else {
        return Ok(None);
    };

    Ok(Some(RetentionRule {
        snapshot_period,
        retained_copies,
    }))
}
