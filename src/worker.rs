use crate::core::{BtrfsContainer, BtrfsDataset};
use crate::model::Entity;
use anyhow::Result;
use chrono::{Duration, Utc};
use log::*;

pub trait Job {
    fn run(&self) -> Result<()>;
    fn next_check(&self) -> Result<Duration>;

    fn is_ready(&self) -> Result<bool> {
        self.next_check().map(|d| {
            trace!(
                "Job is {}ready based on having {} delay to next check.",
                if d.is_zero() { "" } else { "not " },
                if d.is_zero() { "no" } else { "a" }
            );
            d.is_zero()
        })
    }
}

pub struct LocalSnapshotJob<'a> {
    dataset: &'a BtrfsDataset,
}

impl<'a> LocalSnapshotJob<'a> {
    pub fn new(dataset: &'a BtrfsDataset) -> Self {
        Self { dataset }
    }
}

impl<'a> Job for LocalSnapshotJob<'a> {
    fn run(&self) -> Result<()> {
        self.dataset.create_local_snapshot()
    }

    fn next_check(&self) -> Result<Duration> {
        let latest = self.dataset.latest_snapshot()?;
        Ok(if let Some(latest_snapshot) = latest {
            trace!(
                "Existing snapshot for {} at {}.",
                self.dataset.model().id(),
                latest_snapshot.datetime()
            );
            let now = Utc::now();
            let next_datetime = latest_snapshot.datetime() + Duration::hours(1);
            if now < next_datetime {
                next_datetime - now
            } else {
                Duration::zero()
            }
        } else {
            trace!("No existing snapshot for {}.", self.dataset.model().id());
            Duration::zero()
        })
    }
}

pub struct LocalSyncJob<'a> {
    dataset: &'a BtrfsDataset,
    container: &'a BtrfsContainer,
}

impl<'a> LocalSyncJob<'a> {
    pub fn new(dataset: &'a BtrfsDataset, container: &'a BtrfsContainer) -> Self {
        Self { dataset, container }
    }
}

impl<'a> Job for LocalSyncJob<'a> {
    fn run(&self) -> Result<()> {
        todo!();
    }

    fn next_check(&self) -> Result<Duration> {
        todo!()
    }
}
