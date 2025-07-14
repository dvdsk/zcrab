use std::fmt::Display;
use color_eyre::Result;

use crate::policy::RetentionPolicy;
use crate::zfs;

pub mod interactive_cli;

struct Configured {
    name: String,
    policy: RetentionPolicy,
}

impl Display for Configured {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

impl Configured {
    fn store_and_apply_retention_policy(&self) -> Result<()> {
        zfs::set_policy(&self.name, &self.policy)
    }
}
