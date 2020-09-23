use crate::worker::{Job, LocalPruneJob, LocalSnapshotJob, LocalSyncJob};
use anyhow::Result;
use libblkcapt::core::{BtrfsContainer, BtrfsDataset, BtrfsPool};

use libblkcapt::model::Entities;
use libblkcapt::model::Entity;
use log::*;
use std::{cmp::min, sync::Arc, time::Duration};
use xactor::{message, Actor, Context, Handler};

pub struct JobDispatchActor {
    jobs: Vec<Box<dyn Job>>,
}

#[message()]
struct DispatchMessage();

#[async_trait::async_trait]
impl Actor for JobDispatchActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        ctx.address().send(DispatchMessage())?;
        info!("Job dispatch actor started successfully.");
        Ok(())
    }

    async fn stopped(&mut self, _ctx: &mut Context<Self>) {
        info!("Job dispatch actor stopped successfully.");
    }
}

impl JobDispatchActor {
    pub fn new(model: Entities) -> Result<Self> {
        let pools = model
            .btrfs_pools
            .iter()
            .map(|p| BtrfsPool::validate(p.clone()).map(Arc::new))
            .collect::<Result<Vec<_>>>()?;
        let datasets = pools
            .iter()
            .flat_map(|p| {
                p.model()
                    .datasets
                    .iter()
                    .map(move |d| BtrfsDataset::validate(p, d.clone()).map(Arc::new))
            })
            .collect::<Result<Vec<_>>>()?;
        let containers = pools
            .iter()
            .flat_map(|p| {
                p.model()
                    .containers
                    .iter()
                    .map(move |d| BtrfsContainer::validate(p, d.clone()).map(Arc::new))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut jobs = Vec::<Box<dyn Job>>::new();
        for dataset in datasets.iter() {
            jobs.push(Box::new(LocalSnapshotJob::new(dataset)));
            jobs.push(Box::new(LocalPruneJob::new(dataset)));
        }
        for sync in model.snapshot_syncs() {
            let sync_dataset = datasets
                .iter()
                .find(|d| d.model().id() == sync.dataset_id())
                .expect("FIXME");
            let sync_container = containers
                .iter()
                .find(|d| d.model().id() == sync.container_id())
                .expect("FIXME");
            jobs.push(Box::new(LocalSyncJob::new(sync_dataset, sync_container)));
        }
        info!("Job dispatcher initialized with {} jobs.", jobs.len());

        Ok(Self { jobs })
    }
}

#[async_trait::async_trait]
impl Handler<DispatchMessage> for JobDispatchActor {
    async fn handle(&mut self, ctx: &mut Context<Self>, _msg: DispatchMessage) {
        trace!("Job dispatch actor dispatch message.");

        let mut ready_jobs = self
            .jobs
            .iter()
            .filter(|j| j.is_ready().expect("FIXME"))
            .collect::<Vec<_>>();
        while !ready_jobs.is_empty() {
            debug!("Iterating Work with {} ready jobs.", ready_jobs.len());
            for job in ready_jobs {
                job.run().await.expect("FIXME!");
            }
            ready_jobs = self
                .jobs
                .iter()
                .filter(|j| j.next_check().expect("FIXME2").is_zero() && j.is_ready().expect("FIXME"))
                .collect::<Vec<_>>();
        }

        let mut next_wait = Duration::from_secs(3600 * 24);
        for job in self.jobs.iter() {
            let job_wait = job.next_check().expect("FIXME!").to_std().unwrap();
            trace!("Job wants to wait {}.", humantime::format_duration(job_wait));
            next_wait = min(job_wait, next_wait);
        }
        debug!("Waiting {} for next iteration.", humantime::format_duration(next_wait));
        ctx.send_later(DispatchMessage(), next_wait);
        trace!("Job dispatch actor message processing complete.");
    }
}
