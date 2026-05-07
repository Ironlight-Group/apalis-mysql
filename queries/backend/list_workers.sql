SELECT
    id,
    worker_type,
    storage_name,
    layers,
    last_seen,
    started_at
FROM
    workers
WHERE
    worker_type = ?
ORDER BY
    last_seen DESC
LIMIT
    ? OFFSET ?
