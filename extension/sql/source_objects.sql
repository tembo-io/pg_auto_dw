CREATE TABLE IF NOT EXISTS source_objects
(
    id bigserial PRIMARY KEY,  -- Auto-incrementing primary key with a larger range
	schema_oid oid,
    schema_name name,
    schema_description text,
	table_oid oid,
    table_name name,
    table_description text,
    column_ordinal_position smallint,
    column_name name,
    column_base_type_name name,
    column_modification_number integer,
    column_type_name text,
    column_description text,
	column_pk_ind INT DEFAULT 0,
	column_pk_name name,
	column_fk_ind INT DEFAULT 0,
	column_dw_flag CHAR(1) DEFAULT 'N',
    valid_from timestamp without time zone DEFAULT (now() AT TIME ZONE 'UTC'), -- Default to current GMT timestamp
    valid_to timestamp without time zone,  -- End of validity period
    current_flag CHAR(1) DEFAULT 'Y',   -- Indicator of current record
	deleted_flag CHAR(1) DEFAULT 'N'
);

DROP TABLE IF EXISTS temp_source_objects;

CREATE TEMPORARY TABLE temp_source_objects AS
WITH
schema_qry AS (
	SELECT 
		pg_namespace.oid AS schema_oid, 
		pg_namespace.nspname AS schema_name,
	    pg_description.description AS schema_description
	FROM pg_catalog.pg_namespace
	LEFT JOIN pg_catalog.pg_description ON 	pg_namespace.oid = pg_description.objoid AND 
											pg_description.objsubid = 0 -- No Sub Objects
	WHERE pg_namespace.nspname !~ 'pg_.*' AND pg_namespace.nspname NOT IN ('information_schema', 'auto_dw')
),
table_qry AS (
	SELECT 
		pg_class.oid AS table_oid, 
		pg_class.relname AS table_name,
		pg_class.relnamespace AS table_schema_oid,
	    pg_description.description AS table_description
	FROM pg_catalog.pg_class
	LEFT JOIN pg_catalog.pg_description ON 	pg_class.oid = pg_description.objoid AND 
											pg_description.objsubid = 0 -- No Sub Objects
	WHERE 
      	pg_class.relkind = 'r'  -- 'r' stands for ordinary table
),
column_qry AS (
	SELECT 
		pg_attribute.attrelid AS column_table_oid,
		pg_attribute.attname AS column_name,
		pg_attribute.attnum AS column_ordinal_position,
		pg_attribute.atttypid AS column_type_oid,
		pg_attribute.atttypmod  AS column_modification_number,
	    pg_catalog.format_type(atttypid, atttypmod) AS column_type_name,
	    pg_description.description AS column_description
	FROM pg_attribute
	LEFT JOIN pg_catalog.pg_description ON 	pg_attribute.attrelid = pg_description.objoid AND 
											pg_attribute.attnum = pg_description.objsubid
	WHERE 
		pg_attribute.attnum > 0  -- Only real columns, not system columns
		AND NOT pg_attribute.attisdropped  -- Only columns that are not dropped
),
type_qry AS (
	SELECT
		oid AS type_oid,
		typname AS base_type_name
	FROM pg_type
),
pk_table_column_qry AS (
	SELECT
		conrelid AS table_oid,
		unnest(conkey) AS column_ordinal_position,
		1 AS column_pk_ind,
		conname AS column_pk_name
	FROM
		pg_constraint
	WHERE
		contype = 'p'
),
fk_table_column_qry AS (
	SELECT DISTINCT -- Distinct one column could have multiple FKs.
		conrelid AS table_oid,
		unnest(conkey) AS column_ordinal_position,
		1 AS column_fk_ind
	FROM
		pg_constraint
	WHERE
		contype = 'f'
),
source_objects_prep AS (
	SELECT
	  schema_qry.schema_oid,
	  schema_qry.schema_name,
	  schema_qry.schema_description,
	  table_qry.table_oid,
	  table_qry.table_name,
	  COALESCE(table_qry.table_description, 'NA') AS table_description,
	  column_qry.column_ordinal_position,
	  column_qry.column_name,
	  type_qry.base_type_name AS column_base_type_name,
	  column_qry.column_modification_number,
	  column_qry.column_type_name,
	  COALESCE(column_qry.column_description, 'NA') AS column_description,
	  COALESCE(pk_table_column_qry.column_pk_ind, 0) AS column_pk_ind,
	  COALESCE(pk_table_column_qry.column_pk_name, 'NA') AS column_pk_name,
	  COALESCE(fk_table_column_qry.column_fk_ind, 0) AS column_fk_ind
	FROM schema_qry
	LEFT JOIN table_qry ON schema_qry.schema_oid = table_qry.table_schema_oid
	LEFT JOIN column_qry ON table_qry.table_oid = column_qry.column_table_oid
	LEFT JOIN type_qry ON column_qry.column_type_oid = type_qry.type_oid
	LEFT JOIN pk_table_column_qry ON 
								table_qry.table_oid = pk_table_column_qry.table_oid AND
								column_qry.column_ordinal_position = pk_table_column_qry.column_ordinal_position
	LEFT JOIN fk_table_column_qry ON 
								table_qry.table_oid = fk_table_column_qry.table_oid AND
								column_qry.column_ordinal_position = fk_table_column_qry.column_ordinal_position
),
column_dw_flag AS (
  	-- Currently on List
	SELECT
	    schema_oid,
		table_oid, 
		column_ordinal_position
	FROM source_objects
	WHERE current_flag = 'Y' AND column_dw_flag = 'Y'
	-- Adding TABLE COLUMNS
	UNION
	SELECT
		schema_oid,
		table_oid, 
		column_ordinal_position
	FROM source_objects_prep
	-- 'a^' ~ mach nothing.  Initialized to 'public'.
	WHERE 
	    schema_name ~ '^public$' AND
		table_name ~ '.*' AND
		column_name ~ '.*'
	--- Removing Schemas
	EXCEPT
	SELECT
		schema_oid,
		table_oid, 
		column_ordinal_position
	FROM source_objects_prep
	-- 'a^' ~ mach nothing.  Initialized to '^public$'.
	WHERE 
	    schema_name ~ 'a^' AND
		table_name ~ 'a^' AND
		column_name ~ 'a^'
)
SELECT
  source_objects_prep.schema_oid,
  source_objects_prep.schema_name,
  source_objects_prep.schema_description,
  source_objects_prep.table_oid,
  source_objects_prep.table_name,
  source_objects_prep.table_description,
  source_objects_prep.column_ordinal_position,
  source_objects_prep.column_name,
  source_objects_prep.column_base_type_name,
  source_objects_prep.column_modification_number,
  source_objects_prep.column_type_name,
  source_objects_prep.column_description,
  source_objects_prep.column_pk_ind,
  source_objects_prep.column_pk_name,
  source_objects_prep.column_fk_ind,
  CASE
	WHEN column_dw_flag.column_ordinal_position IS NOT NULL THEN 'Y'
	ELSE 'N'
  END AS column_dw_flag
FROM source_objects_prep
LEFT JOIN column_dw_flag ON 
	source_objects_prep.schema_oid = column_dw_flag.schema_oid AND -- Remove to track tables even if they move schemas.
	source_objects_prep.table_oid = column_dw_flag.table_oid AND
	source_objects_prep.column_ordinal_position = column_dw_flag.column_ordinal_position
ORDER BY source_objects_prep.schema_name, source_objects_prep.table_name, source_objects_prep.column_ordinal_position
;

-- If anything associated with current columns change set the current_flg to 'N'
UPDATE source_objects
SET valid_to = (now() AT TIME ZONE 'UTC'), current_flag = 'N'
FROM temp_source_objects
WHERE source_objects.current_flag = 'Y'
    AND source_objects.schema_oid = temp_source_objects.schema_oid
	AND source_objects.table_oid = temp_source_objects.table_oid
	AND source_objects.column_ordinal_position = temp_source_objects.column_ordinal_position
	AND (
	  source_objects.schema_name IS DISTINCT FROM temp_source_objects.schema_name OR
	  source_objects.schema_description IS DISTINCT FROM temp_source_objects.schema_description OR
	  source_objects.table_name IS DISTINCT FROM temp_source_objects.table_name OR
	  source_objects.table_description IS DISTINCT FROM temp_source_objects.table_description OR
	  source_objects.column_name IS DISTINCT FROM temp_source_objects.column_name OR
	  source_objects.column_base_type_name IS DISTINCT FROM temp_source_objects.column_base_type_name OR
	  source_objects.column_modification_number IS DISTINCT FROM temp_source_objects.column_modification_number OR
	  source_objects.column_type_name IS DISTINCT FROM temp_source_objects.column_type_name OR
	  source_objects.column_description IS DISTINCT FROM temp_source_objects.column_description OR
	  source_objects.column_pk_ind IS DISTINCT FROM temp_source_objects.column_pk_ind OR
  	  source_objects.column_pk_name IS DISTINCT FROM temp_source_objects.column_pk_name OR
      source_objects.column_fk_ind IS DISTINCT FROM temp_source_objects.column_fk_ind OR
	  source_objects.column_dw_flag IS DISTINCT FROM temp_source_objects.column_dw_flag
	);
	
-- If anything that was deleted from the prior record set comes back.
UPDATE source_objects
SET deleted_flag = 'N'
FROM temp_source_objects
WHERE source_objects.current_flag = 'Y' AND source_objects.deleted_flag = 'Y'
    AND source_objects.schema_oid = temp_source_objects.schema_oid
	AND source_objects.table_oid = temp_source_objects.table_oid
	AND source_objects.column_ordinal_position = temp_source_objects.column_ordinal_position
	AND (
	  source_objects.schema_name = temp_source_objects.schema_name OR
	  source_objects.schema_description = temp_source_objects.schema_description OR
	  source_objects.table_name = temp_source_objects.table_name OR
	  source_objects.table_description = temp_source_objects.table_description OR
	  source_objects.column_name = temp_source_objects.column_name OR
	  source_objects.column_base_type_name = temp_source_objects.column_base_type_name OR
	  source_objects.column_modification_number = temp_source_objects.column_modification_number OR
	  source_objects.column_type_name = temp_source_objects.column_type_name OR
	  source_objects.column_description = temp_source_objects.column_description OR
	  source_objects.column_pk_ind = temp_source_objects.column_pk_ind OR
  	  source_objects.column_pk_name = temp_source_objects.column_pk_name OR
      source_objects.column_fk_ind = temp_source_objects.column_fk_ind OR
	  source_objects.column_dw_flag = temp_source_objects.column_dw_flag
	);

-- Inserting new records.
INSERT INTO source_objects (
	schema_oid,
	schema_name,
	schema_description,
	table_oid,
	table_name,
	table_description,
	column_ordinal_position,
	column_name,
	column_base_type_name,
	column_modification_number,
	column_type_name,
	column_description,
	column_pk_ind,
	column_pk_name,
	column_fk_ind,
	column_dw_flag
)
SELECT
    temp_source_objects.schema_oid,
	temp_source_objects.schema_name,
	temp_source_objects.schema_description,
	temp_source_objects.table_oid,
	temp_source_objects.table_name,
	temp_source_objects.table_description,
	temp_source_objects.column_ordinal_position,
	temp_source_objects.column_name,
	temp_source_objects.column_base_type_name,
	temp_source_objects.column_modification_number,
	temp_source_objects.column_type_name,
	temp_source_objects.column_description,
	temp_source_objects.column_pk_ind,
	temp_source_objects.column_pk_name,
	temp_source_objects.column_fk_ind,
	temp_source_objects.column_dw_flag
FROM temp_source_objects
LEFT JOIN source_objects ON source_objects.current_flag = 'Y' 
    AND source_objects.schema_oid = temp_source_objects.schema_oid
	AND source_objects.table_oid = temp_source_objects.table_oid
	AND source_objects.column_ordinal_position = temp_source_objects.column_ordinal_position
WHERE source_objects.column_ordinal_position IS NULL;

DROP TABLE IF EXISTS temp_source_objects;
