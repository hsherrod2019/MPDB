ALTER TABLE test.size_fraction
CHANGE COLUMN `Size_range_from_[μm]` Size_range_from_μm FLOAT;



SELECT COLUMN_NAME, DATA_TYPE
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'test'
  AND TABLE_NAME = 'size_fraction'
  AND COLUMN_NAME = 'Size_range_from_[μm]';


ALTER TABLE test.size_fraction
CHANGE COLUMN `Size_range_to_[μm]` Size_range_to_μm FLOAT;



SELECT COLUMN_NAME, DATA_TYPE
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'test'
  AND TABLE_NAME = 'size_fraction'
  AND COLUMN_NAME = 'Size_range_to_[μm]';
  
  
SELECT COLUMN_NAME, DATA_TYPE
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'test'
  AND TABLE_NAME = 'size_fraction'
  AND COLUMN_NAME = 'Size_category';
  
  -- Remove NULL
ALTER TABLE test.size_fraction
MODIFY Size_category VARCHAR(50) NULL;

ALTER TABLE size_fraction
MODIFY COLUMN Size_range_from_μm VARCHAR(75);
