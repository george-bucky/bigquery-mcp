SELECT
  job_log.job_id,
  FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', job_log.creation_time) AS creation_time,
  job_log.query
FROM
  `region-{region}`.INFORMATION_SCHEMA.JOBS AS job_log
CROSS JOIN
  UNNEST(job_log.referenced_tables) AS referenced_table
WHERE
  job_log.creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
  AND job_log.state = 'DONE'
  AND job_log.job_type = 'QUERY'
  AND job_log.statement_type = 'SELECT'
  AND referenced_table.project_id = @project_id
  AND referenced_table.dataset_id = @dataset_id
  AND referenced_table.table_id = @table_id
  AND (
    @email_domain IS NULL
    OR ENDS_WITH(job_log.user_email, CONCAT('@', @email_domain))
  )
ORDER BY
  ABS(FARM_FINGERPRINT(job_log.job_id)) ASC,
  job_log.creation_time DESC
LIMIT
  @limit
