use crate::worker::{Job, LocalPruneJob, LocalSnapshotJob, LocalSyncJob};
use anyhow::Result;
use libblkcapt::core::{observation_manager_init, BtrfsContainer, BtrfsDataset, BtrfsPool};
use libblkcapt::model::storage;
use libblkcapt::model::Entity;
use log::*;
use std::{cmp::min, mem, rc::Rc, time::Duration};

pub async fn service() -> Result<()> {
    let mut entities = storage::load_entity_state();
    observation_manager_init(mem::take(entities.observers.as_mut()));
    let entities = entities;

    // should these have into iters and consume the models?
    let pools = entities
        .btrfs_pools
        .iter()
        .map(|p| BtrfsPool::validate(p.clone()).map(Rc::new))
        .collect::<Result<Vec<_>>>()?;
    let datasets = pools
        .iter()
        .flat_map(|p| {
            p.model()
                .datasets
                .iter()
                .map(move |d| BtrfsDataset::validate(p, d.clone()).map(Rc::new))
        })
        .collect::<Result<Vec<_>>>()?;
    let containers = pools
        .iter()
        .flat_map(|p| {
            p.model()
                .containers
                .iter()
                .map(move |d| BtrfsContainer::validate(p, d.clone()).map(Rc::new))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut jobs = Vec::<Box<dyn Job>>::new();
    for dataset in datasets.iter() {
        jobs.push(Box::new(LocalSnapshotJob::new(dataset)));
        jobs.push(Box::new(LocalPruneJob::new(dataset)));
    }
    for sync in entities.snapshot_syncs() {
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
    let jobs = jobs;

    info!("Job dispatcher initialized with {} jobs.", jobs.len());
    loop {
        let mut ready_jobs = jobs.iter().filter(|j| j.is_ready().expect("FIXME")).collect::<Vec<_>>();
        while !ready_jobs.is_empty() {
            debug!("Iterating Work with {} ready jobs.", ready_jobs.len());
            for job in ready_jobs {
                job.run().await?;
            }
            ready_jobs = jobs
                .iter()
                .filter(|j| j.next_check().expect("FIXME2").is_zero() && j.is_ready().expect("FIXME"))
                .collect::<Vec<_>>();
        }

        let mut next_wait = Duration::from_secs(3600 * 24);
        for job in jobs.iter() {
            let job_wait = job.next_check()?.to_std().unwrap();
            trace!("Job wants to wait {}.", humantime::format_duration(job_wait));
            next_wait = min(job_wait, next_wait);
        }
        debug!("Waiting {} for next iteration.", humantime::format_duration(next_wait));
        tokio::select! {
            _ = tokio::time::delay_for(next_wait) => {
                trace!("Wait elapsed.");
            }
            _ = tokio::signal::ctrl_c() => {
                trace!("Cancelled.");
                break;
            }
        }
    }
    info!("Job dispatcher shutdown successfully.");
    Ok(())
}
