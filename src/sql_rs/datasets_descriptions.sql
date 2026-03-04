SELECT
  schema_name AS dataset,
  TRIM(option_value, '"') AS description
FROM
  `region-{region}`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS
WHERE
  option_name = 'description'
ORDER BY
  dataset
LIMIT
  @limit
OFFSET
  @offset
