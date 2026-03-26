SELECT
    id,
    status,
    last_result as result
FROM
    jobs
WHERE
    JSON_CONTAINS(?, JSON_QUOTE(id))
    AND (
        status = 'Done'
        OR (
            status = 'Failed'
            AND attempts >= max_attempts
        )
        OR status = 'Killed'
    )
