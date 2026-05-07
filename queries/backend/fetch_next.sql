SELECT
    job,
    id,
    job_type,
    status,
    attempts,
    max_attempts,
    priority,
    run_at,
    last_result,
    lock_at,
    lock_by,
    done_at,
    metadata
FROM
    jobs
WHERE
    job_type = ?
    AND (
        (
            status = 'Pending'
            AND lock_by IS NULL
        )
        OR (
            status = 'Failed'
            AND attempts < max_attempts
        )
    )
    AND (
        run_at IS NULL
        OR run_at <= ?
    )
ORDER BY
    priority DESC,
    run_at ASC,
    id ASC
LIMIT
    ? FOR
UPDATE
    SKIP LOCKED;
