use apalis_core::worker::context::WorkerContext;
use futures::{FutureExt, Stream, stream};
use sqlx::MySqlPool;

use crate::{
    Config,
    queries::{reenqueue_orphaned::reenqueue_orphaned, register_worker::register_worker},
};

fn is_worker_missing_error(err: &sqlx::Error) -> bool {
    matches!(err, sqlx::Error::Io(io_err) if io_err.kind() == std::io::ErrorKind::NotFound)
}

/// Send a keep-alive signal to the database to indicate that the worker is still active
pub async fn keep_alive(
    pool: MySqlPool,
    config: Config,
    worker: WorkerContext,
) -> Result<(), sqlx::Error> {
    let worker = worker.name().to_owned();
    let queue = config.queue().to_string();
    log::debug!(
        "Sending keep-alive heartbeat: worker_id={}, queue={}",
        worker,
        queue
    );
    let res = sqlx::query_file!("queries/backend/keep_alive.sql", worker, queue)
        .execute(&pool)
        .await?;
    log::debug!(
        "Keep-alive query finished: rows_affected={}",
        res.rows_affected()
    );
    if res.rows_affected() == 0 {
        log::warn!("Keep-alive reported missing worker row");
        return Err(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "WORKER_DOES_NOT_EXIST",
        )));
    }
    log::debug!("Keep-alive successful");
    Ok(())
}

/// Register a worker and keep retrying until registration succeeds.
pub async fn register_worker_until_success(
    pool: MySqlPool,
    config: Config,
    worker: WorkerContext,
    storage_type: &str,
) -> Result<(), sqlx::Error> {
    let retry_delay = *config.keep_alive();
    let queue = config.queue().to_string();
    let mut attempt: u64 = 1;
    loop {
        log::info!(
            "Worker registration attempt {}: worker_id={}, queue={}, storage_type={}",
            attempt,
            worker.name(),
            queue,
            storage_type
        );
        match register_worker(pool.clone(), config.clone(), worker.clone(), storage_type).await {
            Ok(()) => {
                log::info!(
                    "Worker registration completed after {} attempt(s): worker_id={}",
                    attempt,
                    worker.name()
                );
                return Ok(());
            }
            Err(e) => {
                log::warn!(
                    "Failed to register worker {} on attempt {}, retrying in {:?}: {}",
                    worker.name(),
                    attempt,
                    retry_delay,
                    e
                );
                apalis_core::timer::sleep(retry_delay).await;
                attempt += 1;
            }
        }
    }
}

/// Best-effort orphan re-enqueue followed by strict worker registration gate.
pub async fn bootstrap_worker(
    pool: MySqlPool,
    config: Config,
    worker: WorkerContext,
    storage_type: &str,
) -> Result<(), sqlx::Error> {
    let queue = config.queue().to_string();
    log::info!(
        "Bootstrapping worker: worker_id={}, queue={}, storage_type={}",
        worker.name(),
        queue,
        storage_type
    );
    if let Err(e) = reenqueue_orphaned(pool.clone(), &config).await {
        log::warn!(
            "Failed to re-enqueue orphaned tasks during bootstrap: {}",
            e
        );
    } else {
        log::debug!("Bootstrap re-enqueue step completed");
    }

    register_worker_until_success(pool, config, worker, storage_type).await
}

/// Create a stream that sends keep-alive signals at regular intervals
pub fn keep_alive_stream(
    pool: MySqlPool,
    config: Config,
    worker: WorkerContext,
    storage_type: &str,
) -> impl Stream<Item = Result<(), sqlx::Error>> + Send {
    let storage_type = storage_type.to_string();
    stream::unfold((), move |_| {
        let pool = pool.clone();
        let config = config.clone();
        let worker = worker.clone();
        let storage_type = storage_type.clone();
        let interval = apalis_core::timer::Delay::new(*config.keep_alive());
        interval.then(move |_| async move {
            let res = keep_alive(pool.clone(), config.clone(), worker.clone()).await;
            let output = match res {
                Ok(()) => Ok(()),
                Err(e) if is_worker_missing_error(&e) => {
                    let queue = config.queue().to_string();
                    log::warn!(
                        "Worker {} missing during keep-alive in queue {}, re-registering",
                        worker.name(),
                        queue
                    );
                    register_worker_until_success(pool, config, worker, &storage_type).await
                }
                Err(e) => Err(e),
            };
            Some((output, ()))
        })
    })
}
