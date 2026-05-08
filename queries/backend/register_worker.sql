INSERT INTO workers (id, worker_type, storage_name, layers, last_seen, started_at)
SELECT ?, ?, ?, ?, NOW(), NOW()
FROM DUAL
WHERE NOT EXISTS (
    SELECT 1 FROM workers
    WHERE id = ?
      AND TIMESTAMPDIFF(SECOND, last_seen, NOW()) < ?
)
ON DUPLICATE KEY UPDATE
    worker_type = VALUES(worker_type),
    storage_name = VALUES(storage_name),
    layers = VALUES(layers),
    last_seen = NOW(),
    started_at = NOW();
