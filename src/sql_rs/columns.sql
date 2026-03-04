SELECT
  column_name AS column,
  field_path,
  data_type,
  description
FROM
  `{project}.{dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
WHERE
  table_name = @table
  AND (@include_undocumented OR description IS NOT NULL)
ORDER BY
  field_path
