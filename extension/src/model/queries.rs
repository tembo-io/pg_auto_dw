pub const SOURCE_TABLE_SAMPLE: &str = r#"
    WITH Temp_Data (schema, "table", status, status_code, status_response) AS (
        VALUES
        ('PUBLIC', 'CUSTOMER',  'Skipped', 'SKIP', 'Source Table was skipped as column(s) need additional context. Please run the following SQL query for more information: SELECT schema, table, column, status, status_response FROM auto_dw.source_status_detail() WHERE schema = ''public'' AND table = ''customers''.')
    )
    SELECT * FROM Temp_Data;
        "#;

pub const SOURCE_COLUMN: &str = r#"
        WITH
        source_objects_tranformation_cal AS (
            SELECT 
                MAX(pk_transformer_responses)AS max_pk_transformer_response
            FROM auto_dw.transformer_responses AS t
            GROUP BY fk_source_objects
        ),
        source_object_transformation_latest AS (
            SELECT t.* FROM auto_dw.transformer_responses AS t
            JOIN source_objects_tranformation_cal AS c ON t.pk_transformer_responses = c.max_pk_transformer_response
        )
        SELECT 
            s.schema_name::TEXT AS schema, 
            s.table_name::TEXT AS table, 
            s.column_name::TEXT AS column,
            CASE
                WHEN t.confidence_score IS NULL THEN 'Queued for Processing'
                WHEN t.confidence_score >= .8 THEN 'Ready to Deploy'
                ELSE 'Requires Attention'
            END AS status,
            CASE 
                WHEN t.confidence_score IS NOT NULL THEN CONCAT((t.confidence_score * 100)::INT::TEXT, '%')
                ELSE '-'
            END AS confidence_level,
            CASE 
                WHEN t.confidence_score IS NOT NULL THEN 
                    (
                    'Status: ' ||
                    CASE
                        WHEN t.confidence_score IS NULL THEN 'Queued for Processing'
                        WHEN t.confidence_score >= .8 THEN 'Ready to Deploy'
                        ELSE 'Requires Attention'
                    END || ': ' ||
                    'Model: ' || model_name || 
                    ' categorized this column as a ' || category || 
                    ' with a confidence of ' || CONCAT((t.confidence_score * 100)::INT::TEXT, '%') || '.  ' ||
                    'Model Reasoning: ' || t.reason
                    )
                ELSE '-'
            END AS status_response
        FROM auto_dw.source_objects AS s
        LEFT JOIN source_object_transformation_latest AS t ON s.pk_source_objects = t.fk_source_objects
        WHERE s.current_flag = 'Y' AND s.deleted_flag = 'N'
        ORDER BY s.schema_name, s.table_name, s.column_ordinal_position;
        "#;

pub const SOURCE_OBJECTS_JSON: &str = r#"
            WITH
            table_tranformation_time_cal AS (
                SELECT 
                    s.table_oid, 
                    MAX(s.valid_from) AS max_table_update, 
                    MAX(t.created_at) AS max_table_transformer_generation
                FROM auto_dw.source_objects AS s
                LEFT JOIN auto_dw.transformer_responses AS t ON s.pk_source_objects = t.fk_source_objects
                WHERE current_flag = 'Y' AND deleted_flag = 'N'
                GROUP BY table_oid),
            tables_requiring_transformation AS (
                SELECT DISTINCT table_oid FROM table_tranformation_time_cal
                WHERE (max_table_update > max_table_transformer_generation) OR max_table_transformer_generation IS NULL
            ),
            source_table_details AS (
                SELECT s.*
                FROM auto_dw.source_objects AS s
                JOIN tables_requiring_transformation AS t ON s.table_oid = t.table_oid
                WHERE current_flag = 'Y' AND deleted_flag = 'N'
            ),
            source_prep AS (
                SELECT 
                    table_oid,
                    column_ordinal_position,
                    json_build_object(
                        'PK Source Objects', pk_source_objects,
                        'Column Ordinal Position', column_ordinal_position
                    ) AS column_link,
                    schema_name, table_name, 
                    'Column No: ' 	|| column_ordinal_position 	|| ' ' ||
                    'Named: '  		|| column_name 				|| ' ' ||
                    'of type: ' 	|| column_type_name 		|| ' ' ||
                    CASE
                        WHEN column_pk_ind =1 THEN 'And is a primary key.' ELSE ''
                    END  ||
				    'Column Comments: ' || column_description
                    AS column_details 
                FROM source_table_details
            )
            SELECT
            table_oid,
            json_build_object(
                'Column Links', array_agg(column_link ORDER BY column_ordinal_position ASC)
            ) AS table_column_links,
            json_build_object(
                'Schema Name', schema_name,
                'Table Name', table_name,
                'Column Details', array_agg(column_details ORDER BY column_ordinal_position ASC)
            ) AS table_details
            FROM source_prep
            GROUP BY table_oid, schema_name, table_name
            ;
        "#;

#[no_mangle]
pub fn source_object_dw(schema_pattern_include: &str, table_pattern_include: &str, column_pattern_include: &str, schema_pattern_exclude: &str, table_pattern_exclude: &str, column_pattern_exclude: &str) -> String {
    format!(r#"
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
table_source_list AS (
	-- Currently on List
	SELECT
		schema_oid,
		table_oid, 
		column_ordinal_position
	FROM auto_dw.source_objects
	WHERE current_flag = 'Y' AND deleted_flag = 'N'
	-- Adding TABLE COLUMNS
	UNION
	SELECT
		schema_oid,
		table_oid, 
		column_ordinal_position
	FROM source_objects_prep
	-- 'a^' ~ mach nothing.
	WHERE 
		schema_name ~ '{}' AND
		table_name ~ '{}' AND
		column_name ~ '{}'
	--- Removing Schemas
	EXCEPT
	SELECT
		schema_oid,
		table_oid, 
		column_ordinal_position
	FROM source_objects_prep
	WHERE 
		schema_name ~ '{}' AND
		table_name ~ '{}' AND
		column_name ~ '{}'
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
source_objects_prep.column_fk_ind
FROM source_objects_prep
JOIN table_source_list ON 
	source_objects_prep.schema_oid = table_source_list.schema_oid AND -- Remove to track tables even if they move schemas.
	source_objects_prep.table_oid = table_source_list.table_oid AND
	source_objects_prep.column_ordinal_position = table_source_list.column_ordinal_position
ORDER BY source_objects_prep.schema_name, source_objects_prep.table_name, source_objects_prep.column_ordinal_position
;

-- Mark anything that was deleted.
UPDATE auto_dw.source_objects
SET deleted_flag = 'Y'
WHERE source_objects.current_flag = 'Y'
AND NOT EXISTS (
    SELECT 1
    FROM temp_source_objects
    WHERE source_objects.schema_oid = temp_source_objects.schema_oid
	  AND source_objects.table_oid = temp_source_objects.table_oid
	  AND source_objects.column_ordinal_position = temp_source_objects.column_ordinal_position
);

-- If anything associated with current columns change set the current_flg to 'N'
UPDATE auto_dw.source_objects
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
	source_objects.column_fk_ind IS DISTINCT FROM temp_source_objects.column_fk_ind
	);

-- If anything that was deleted from the prior record set comes back.
UPDATE auto_dw.source_objects
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
	source_objects.column_fk_ind = temp_source_objects.column_fk_ind
	);

-- Inserting new records.
INSERT INTO auto_dw.source_objects (
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
	column_fk_ind
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
	temp_source_objects.column_fk_ind
FROM temp_source_objects
LEFT JOIN auto_dw.source_objects ON source_objects.current_flag = 'Y' 
	AND source_objects.schema_oid = temp_source_objects.schema_oid
	AND source_objects.table_oid = temp_source_objects.table_oid
	AND source_objects.column_ordinal_position = temp_source_objects.column_ordinal_position
WHERE source_objects.column_ordinal_position IS NULL;

DROP TABLE IF EXISTS temp_source_objects;
"#, schema_pattern_include, table_pattern_include, column_pattern_include, schema_pattern_exclude, table_pattern_exclude, column_pattern_exclude)
}

#[no_mangle]
pub fn insert_into_build_call(build_id: &str, build_flag: &str, build_status: &str, status: &str) -> String {
    format!(r#"
    INSERT INTO auto_dw.build_call (fk_transformer_responses, build_id, build_flag, build_status)
    WITH
    source_objects_tranformation_cal AS (
        SELECT 
            MAX(pk_transformer_responses)AS max_pk_transformer_response
        FROM auto_dw.transformer_responses AS t
        GROUP BY fk_source_objects
    ),
    source_object_transformation_latest AS (
        SELECT t.* FROM auto_dw.transformer_responses AS t
        JOIN source_objects_tranformation_cal AS c ON t.pk_transformer_responses = c.max_pk_transformer_response
    ),
    sour_object_status AS (
        SELECT 
            t.pk_transformer_responses,
            s.schema_name::TEXT AS schema, 
            s.table_name::TEXT AS table, 
            s.column_name::TEXT AS column,
            CASE
                WHEN t.confidence_score IS NULL THEN 'Queued for Processing'
                WHEN t.confidence_score >= .8 THEN 'Ready to Deploy'
                ELSE 'Requires Attention'
            END AS status,
            CASE 
                WHEN t.confidence_score IS NOT NULL THEN CONCAT((t.confidence_score * 100)::INT::TEXT, '%')
                ELSE '-'
            END AS confidence_level,
            CASE 
                WHEN t.confidence_score IS NOT NULL THEN 
                    (
                    'Status: ' ||
                    CASE
                        WHEN t.confidence_score IS NULL THEN 'Queued for Processing'
                        WHEN t.confidence_score >= .8 THEN 'Ready to Deploy'
                        ELSE 'Requires Attention'
                    END || ': ' ||
                    'Model: ' || model_name || 
                    ' categorized this column as a ' || category || 
                    ' with a confidence of ' || CONCAT((t.confidence_score * 100)::INT::TEXT, '%') || '.  ' ||
                    'Model Reasoning: ' || t.reason
                    )
                ELSE '-'
            END AS status_response
        FROM auto_dw.source_objects AS s
        LEFT JOIN source_object_transformation_latest AS t ON s.pk_source_objects = t.fk_source_objects
        WHERE s.current_flag = 'Y' AND s.deleted_flag = 'N'
        ORDER BY s.schema_name, s.table_name, s.column_ordinal_position
        )
    SELECT 
        pk_transformer_responses AS fk_transformer_responses,
        '{}' AS build_id,
        '{}' AS build_flag,
        '{}' AS build_status
    FROM sour_object_status
    WHERE status = '{}';
"#, build_id, build_flag, build_status, status)
}

#[no_mangle]
pub fn build_object_pull(build_id: &str) -> String {
    format!(r#"
WITH system AS (
	SELECT system_identifier AS id FROM pg_control_system() LIMIT 1
)
SELECT 
schema_name::TEXT AS schema_name, 
table_name::TEXT AS table_name, 
category::TEXT AS column_category, 
column_name::TEXT AS column_name, 
column_type_name::TEXT AS column_type_name, 
system.id::BIGINT AS system_id,
so.table_oid::OID as table_oid,
so.column_ordinal_position::SMALLINT AS column_ordinal_position
FROM system, auto_dw.build_call AS bc
LEFT JOIN auto_dw.transformer_responses AS t ON bc.fk_transformer_responses = t.pk_transformer_responses
LEFT JOIN auto_dw.source_objects AS so ON t.fk_source_objects = so.pk_source_objects
WHERE build_id = '{}';
"#, build_id)
}