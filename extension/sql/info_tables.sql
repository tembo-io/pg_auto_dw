DROP TABLE IF EXISTS source_objects;

CREATE TABLE IF NOT EXISTS source_objects
(
    pk_source_objects bigserial PRIMARY KEY,
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

DROP TABLE IF EXISTS auto_dw.transformer_responses;

CREATE TABLE IF NOT EXISTS transformer_responses
(
    pk_transformer_responses BIGSERIAL PRIMARY KEY,
    fk_source_objects BIGINT,
    model_name TEXT,
    category TEXT,
    confidence_score NUMERIC(3, 2),
    reason TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'UTC'),
    current_flag CHAR(1) DEFAULT 'Y',
    CONSTRAINT fk_source_objects FOREIGN KEY (fk_source_objects) 
	   	REFERENCES source_objects(pk_source_objects)
		ON DELETE CASCADE
);