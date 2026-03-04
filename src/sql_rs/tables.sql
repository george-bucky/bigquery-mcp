WITH table_descriptions AS (
  SELECT
    table_name,
    option_value AS raw_description
  FROM
    `{project}.{dataset}.INFORMATION_SCHEMA.TABLE_OPTIONS`
  WHERE
    option_name = 'description'
),
table_storage AS (
  SELECT
    table_name,
    storage_last_modified_time
  FROM
    `region-{region}`.INFORMATION_SCHEMA.TABLE_STORAGE
  WHERE
    project_id = '{project}'
    AND table_schema = '{dataset}'
    AND COALESCE(deleted, FALSE) = FALSE
)
SELECT
  tables.table_name AS relation,
  tables.table_type AS relation_type,
  TRIM(table_descriptions.raw_description, '"') AS description,
  FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', tables.creation_time) AS created_at,
  FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', table_storage.storage_last_modified_time) AS last_modified_at
FROM
  `{project}.{dataset}.INFORMATION_SCHEMA.TABLES` AS tables
LEFT JOIN
  table_descriptions
ON
  table_descriptions.table_name = tables.table_name
LEFT JOIN
  table_storage
ON
  table_storage.table_name = tables.table_name
WHERE
  @include_without_description
  OR table_descriptions.raw_description IS NOT NULL
ORDER BY
  relation
LIMIT
  @limit
OFFSET
  @offset
