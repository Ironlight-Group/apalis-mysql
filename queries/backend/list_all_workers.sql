SELECT
    id,
    worker_type,
    storage_name,
    layers,
    last_seen,
    started_at
FROM
    workers
ORDER BY
    last_seen DESC
LIMIT
    ? OFFSET ?
