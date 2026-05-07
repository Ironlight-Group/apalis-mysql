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
    status = ?
ORDER BY
    done_at DESC,
    run_at DESC
LIMIT
    ? OFFSET ?
