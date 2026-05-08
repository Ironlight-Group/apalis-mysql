use apalis_core::worker::context::WorkerContext;
use sqlx::MySqlPool;

use crate::Config;

/// Register a new worker in the database
pub async fn register_worker(
    pool: MySqlPool,
    config: Config,
    worker: WorkerContext,
    storage_type: &str,
) -> Result<(), sqlx::Error> {
    let worker_id = worker.name().to_owned();
    let queue = config.queue().to_string();
    let layers = worker.get_service().to_owned();
    let keep_alive = config.keep_alive().as_secs() as i64;
    log::debug!(
        "Attempting worker registration: worker_id={}, queue={}, storage_type={}, keep_alive_secs={}",
        worker_id,
        queue,
        storage_type,
        keep_alive
    );
    let res = sqlx::query_file!(
        "queries/backend/register_worker.sql",
        worker_id,
        queue,
        storage_type,
        layers,
        worker_id,
        keep_alive,
    )
    .execute(&pool)
    .await?;
    log::debug!(
        "Worker registration query finished: rows_affected={}",
        res.rows_affected()
    );
    if res.rows_affected() == 0 {
        log::warn!("Worker registration skipped because a recent worker row already exists");
        return Err(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "WORKER_ALREADY_EXISTS",
        )));
    }
    log::info!("Worker registration successful");
    Ok(())
}
