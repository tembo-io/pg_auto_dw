pub const SELLER_DV: &str = r#"
    /*** CREATE HUB ***/
    DROP TABLE IF EXISTS public.hub_seller;

    CREATE TABLE public.hub_seller (
        hub_seller_hk VARCHAR NOT NULL,
        load_ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        record_source VARCHAR NOT NULL,
        seller_id_bk VARCHAR
    );

    INSERT INTO public.hub_seller (
        hub_seller_hk,
        load_ts,
        record_source,
        seller_id_bk
    )
    WITH initialized AS (
        SELECT
        CASE
            WHEN COUNT(*) > 0 THEN TRUE
            ELSE FALSE
        END is_initialized
        FROM public.hub_seller
    )
    SELECT
        ENCODE(PUBLIC.DIGEST(ARRAY_TO_STRING(ARRAY[-1], ',')::TEXT, 'sha256'), 'hex') AS hub_seller_hk,
        '0001-01-01'::TIMESTAMP WITHOUT TIME ZONE AS load_ts, 
        'SYSTEM'::TEXT AS record_source,
            '-1'::TEXT AS seller_id_bk
        FROM initialized WHERE NOT initialized.is_initialized
    UNION
    SELECT
        ENCODE(PUBLIC.DIGEST(ARRAY_TO_STRING(ARRAY[-2], ',')::TEXT, 'sha256'), 'hex') AS hub_seller_hk,
        '0001-01-01'::TIMESTAMP WITHOUT TIME ZONE AS load_ts,
        'SYSTEM'::TEXT AS record_source,
            '-2'::TEXT AS seller_id_bk
        FROM initialized WHERE NOT initialized.is_initialized
    ;

    INSERT INTO public.hub_seller (
        hub_seller_hk,
        load_ts,
        record_source,
        seller_id_bk
    )
    WITH
    stg_data AS (
    SELECT
        ENCODE(
            public.DIGEST(
                ARRAY_TO_STRING(
                    ARRAY[stg.seller_id], ','), 'sha256'), 'hex') AS hub_seller_hk,
        (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::TIMESTAMP(6) AS load_ts,
        'STG_OLIST_ECOM' AS record_source,
        stg.seller_id::TEXT AS seller_id_bk
    FROM public.seller AS stg
    ),
    new_stg_data AS (
    SELECT stg_data.* FROM stg_data
    LEFT JOIN public.hub_seller ON stg_data.hub_seller_hk = hub_seller.hub_seller_hk
    WHERE hub_seller.hub_seller_hk IS NULL
    )
    SELECT
    hub_seller_hk,
    load_ts,
    record_source,
    seller_id_bk
    FROM new_stg_data;

    /*** CREATE SAT ***/

    DROP TABLE IF EXISTS public.sat_seller;

    CREATE TABLE public.sat_seller
    (
        hub_seller_hk VARCHAR NOT NULL,
        load_ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        record_source VARCHAR NOT NULL,
        sat_seller_hd VARCHAR NOT NULL,
    city VARCHAR(255),
    state CHAR(2),
    zip_5 VARCHAR(10)
    );

    INSERT INTO public.sat_seller (
        hub_seller_hk,
        load_ts,
        record_source,
        sat_seller_hd,
        city,
        state,
        zip_5
    )
    WITH stg AS (
        SELECT 
            *,
            ENCODE(
                public.DIGEST(
                    ARRAY_TO_STRING(
                        ARRAY[stg.seller_id], ','), 'sha256'), 'hex') AS hub_seller_hk,
            ENCODE(
                public.DIGEST(
                    ARRAY_TO_STRING(
                        ARRAY[stg.city, stg.state, stg.zip_5], ','), 'sha256'), 'hex') AS sat_seller_hd
        FROM public.seller AS stg
    ),
    new_stg_data AS (  
    SELECT stg.*
        FROM stg
    LEFT JOIN public.sat_seller ON 
        stg.hub_seller_hk = sat_seller.hub_seller_hk AND
        stg.sat_seller_hd = sat_seller.sat_seller_hd
    WHERE sat_seller.hub_seller_hk IS NULL
    )
    SELECT   
    hub_seller_hk,
    (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS load_ts ,
    'PUBLIC SCHEMA' AS record_source ,
    sat_seller_hd,
    city,
    state,
    zip_5
    FROM new_stg_data
    ; 
    "#;

pub const GO_OUTPUT: &str = r#"
    Build ID: df6fdea1-10c3-474c-ae62-e63def80bb0b

    Data warehouse tables are currently being built.

    If you need to check the status of the data warehouse, please issue the following command:

    // SELECT * FROM auto_dw.health();

    Alert: One or more tables lack data warehouse (DW) schedule. You have the option to either manually push these tables or add a schedule to their context.

    Example - Manual Push:
    // SELECT auto_dw.go('Push-Table', 'PUBLIC.FOO');

    Example - Adding a 4 AM Daily Schedule to TABLE FOO's Context:
    // SELECT auto_dw.update_context('public.foo', '{"cron": "0 4 * * *"}');
    "#;

pub const SOURCE_TABLE_SAMPLE: &str = r#"
    WITH Temp_Data (schema, "table", status, status_code, status_response) AS (
        VALUES
        ('PUBLIC', 'CUSTOMER',  'Skipped', 'SKIP', 'Source Table was skipped as column(s) need additional context. Please run the following SQL query for more information: SELECT schema, table, column, status, status_response FROM auto_dw.source_status_detail() WHERE schema = ''public'' AND table = ''customers''.')
    )
    SELECT * FROM Temp_Data;
        "#;

pub const SOURCE_COLUMN_SAMPLE: &str = r#"
        WITH Temp_Data (schema, "table", "column", status, confidence_level, status_response) AS (
            VALUES
            ('PUBLIC', 'CUSTOMER', 'CUSTOMER_ID', 'Ready', '10', 'Ready: Column...'),
            ('PUBLIC', 'CUSTOMER', 'ACCOUNT_CREATION_DATE', 'Ready', '10', 'Ready: Column...'),
            ('PUBLIC', 'CUSTOMER', 'MEMBERSHIP_TYPE', 'Ready', '9', 'Ready: Column...'),
            ('PUBLIC', 'CUSTOMER', 'ZIP', 'Requires Attention', '6', 'Requires Attention: Column cannot be appropriately categorized as it may contain sensitive data.  Specifically, if the zip is an extended zip it may be considered PII.'),
            ('PUBLIC', 'CUSTOMER', 'EMAIL', 'Ready', '10', 'Ready: Column...')
        )
        SELECT * FROM Temp_Data;
        "#;


pub const SOURCE_OBJECTS_INIT: &str = r#"
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
            FROM auto_dw.source_objects
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
            source_objects.column_fk_ind IS DISTINCT FROM temp_source_objects.column_fk_ind OR
            source_objects.column_dw_flag IS DISTINCT FROM temp_source_objects.column_dw_flag
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
            source_objects.column_fk_ind = temp_source_objects.column_fk_ind OR
            source_objects.column_dw_flag = temp_source_objects.column_dw_flag
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
        LEFT JOIN auto_dw.source_objects ON source_objects.current_flag = 'Y' 
            AND source_objects.schema_oid = temp_source_objects.schema_oid
            AND source_objects.table_oid = temp_source_objects.table_oid
            AND source_objects.column_ordinal_position = temp_source_objects.column_ordinal_position
        WHERE source_objects.column_ordinal_position IS NULL;

        DROP TABLE IF EXISTS temp_source_objects;
        "#;

pub const SOURCE_OBJECTS_UPDATE: &str = r#"
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
            FROM auto_dw.source_objects
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
                schema_name ~ 'a^' AND
                table_name ~ 'a^' AND
                column_name ~ 'a^'
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
            source_objects.column_fk_ind IS DISTINCT FROM temp_source_objects.column_fk_ind OR
            source_objects.column_dw_flag IS DISTINCT FROM temp_source_objects.column_dw_flag
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
            source_objects.column_fk_ind = temp_source_objects.column_fk_ind OR
            source_objects.column_dw_flag = temp_source_objects.column_dw_flag
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
        LEFT JOIN auto_dw.source_objects ON source_objects.current_flag = 'Y' 
            AND source_objects.schema_oid = temp_source_objects.schema_oid
            AND source_objects.table_oid = temp_source_objects.table_oid
            AND source_objects.column_ordinal_position = temp_source_objects.column_ordinal_position
        WHERE source_objects.column_ordinal_position IS NULL;

        DROP TABLE IF EXISTS temp_source_objects;
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
    column_dw_flag AS (
        -- Currently on List
        SELECT
            schema_oid,
            table_oid, 
            column_ordinal_position
        FROM auto_dw.source_objects
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
        -- 'a^' ~ mach nothing.  Initialized to '^public$'.
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
        source_objects.column_fk_ind IS DISTINCT FROM temp_source_objects.column_fk_ind OR
        source_objects.column_dw_flag IS DISTINCT FROM temp_source_objects.column_dw_flag
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
        source_objects.column_fk_ind = temp_source_objects.column_fk_ind OR
        source_objects.column_dw_flag = temp_source_objects.column_dw_flag
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
    LEFT JOIN auto_dw.source_objects ON source_objects.current_flag = 'Y' 
        AND source_objects.schema_oid = temp_source_objects.schema_oid
        AND source_objects.table_oid = temp_source_objects.table_oid
        AND source_objects.column_ordinal_position = temp_source_objects.column_ordinal_position
    WHERE source_objects.column_ordinal_position IS NULL;

    DROP TABLE IF EXISTS temp_source_objects;
"#, schema_pattern_include, table_pattern_include, column_pattern_include, schema_pattern_exclude, table_pattern_exclude, column_pattern_exclude)



}