use anyhow::Result;
use blkcapt::core::{BtrfsDataset, BtrfsPool};
use blkcapt::model::storage;
use blkcapt::worker::{Job, LocalSnapshotJob};
use log::*;
use std::rc::Rc;

pub fn service() -> Result<()> {
    let entities = storage::load_entity_state();

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
                .map(move |d| BtrfsDataset::validate(Rc::clone(p), d.clone()))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut jobs = Vec::<Box<dyn Job>>::new();
    for dataset in datasets.iter() {
        jobs.push(Box::new(LocalSnapshotJob::new(dataset)));
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
            .filter_map(|j| if j.is_ready().expect("FIXME") { Some(j) } else { None })
            .collect::<Vec<_>>();
    }

    info!("Work complete successfully.");
    Ok(())
}
