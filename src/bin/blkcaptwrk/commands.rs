use anyhow::Result;
use blkcapt::core::{BtrfsDataset, BtrfsPool, BtrfsContainer};
use blkcapt::model::storage;
use blkcapt::model::Entity;
use blkcapt::worker::{Job, LocalSnapshotJob, LocalSyncJob, LocalPruneJob};
use log::*;
use std::rc::Rc;

pub fn service() -> Result<()> {
    let entities = storage::load_entity_state();

    // should these have into iters and consume the models?
    let pools = entities
        .pools()
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
        let sync_dataset = datasets.iter().find(|d| d.model().id() == sync.dataset_id()).expect("FIXME");
        let sync_container = containers.iter().find(|d| d.model().id() == sync.container_id()).expect("FIXME");
        jobs.push(Box::new(LocalSyncJob::new(sync_dataset, sync_container)));
    }
    let jobs = jobs;

    info!("Worker initialized with {} jobs.", jobs.len());

    let mut ready_jobs = jobs
        .iter()
        .filter_map(|j| if j.is_ready().expect("FIXME") { Some(j) } else { None })
        .collect::<Vec<_>>();
    while ready_jobs.len() > 0 {
        debug!("Iterating Work with {} ready jobs.", ready_jobs.len());
        for job in ready_jobs {
            job.run()?;
        }
        ready_jobs = jobs
            .iter()
            .filter_map(|j| if j.next_check().expect("FIXME2").is_zero() && j.is_ready().expect("FIXME") { Some(j) } else { None })
            .collect::<Vec<_>>();
    }

    info!("Work complete successfully.");
    Ok(())
}
