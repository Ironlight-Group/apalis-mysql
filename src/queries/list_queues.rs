use apalis_core::backend::{BackendExt, ListQueues, QueueInfo};
use ulid::Ulid;

use crate::{CompactType, MySqlContext, MySqlStorage};

struct QueueInfoRow {
    name: Option<String>,
    stats: serde_json::Value,
    workers: serde_json::Value,
    activity: serde_json::Value,
}

fn decode_json_field<T>(value: serde_json::Value) -> T
where
    T: serde::de::DeserializeOwned + Default,
{
    match value {
        serde_json::Value::String(raw) => serde_json::from_str(&raw).unwrap_or_default(),
        other => serde_json::from_value(other).unwrap_or_default(),
    }
}

impl From<QueueInfoRow> for QueueInfo {
    fn from(row: QueueInfoRow) -> Self {
        Self {
            name: row.name.unwrap_or_default(),
            stats: decode_json_field(row.stats),
            workers: decode_json_field(row.workers),
            activity: decode_json_field(row.activity),
        }
    }
}

impl<Args, D, F> ListQueues for MySqlStorage<Args, D, F>
where
    Self: BackendExt<
            Context = MySqlContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = sqlx::Error,
        >,
{
    fn list_queues(&self) -> impl Future<Output = Result<Vec<QueueInfo>, Self::Error>> + Send {
        let pool = self.pool.clone();

        async move {
            let queues = sqlx::query_file_as!(QueueInfoRow, "queries/backend/list_queues.sql")
                .fetch_all(&pool)
                .await?
                .into_iter()
                .map(QueueInfo::from)
                .collect();
            Ok(queues)
        }
    }
}
