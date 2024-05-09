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