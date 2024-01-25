SELECT
  TABLE_NAME,                      -- Name of the table that contains the foreign key
  CONSTRAINT_NAME,                 -- Name of the foreign key constraint
  COLUMN_NAME,                     -- Name of the column that is a foreign key
  REFERENCED_TABLE_NAME,           -- Name of the referenced table (parent table)
  REFERENCED_COLUMN_NAME           -- Name of the referenced column in the parent table
FROM
  INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE
  REFERENCED_TABLE_NAME = 'institution'   -- Replace 'countries' with your table name
  AND REFERENCED_COLUMN_NAME = 'InstituteAcronym';  -- Replace 'CountryShort' with your column name