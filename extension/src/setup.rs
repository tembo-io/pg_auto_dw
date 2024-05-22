use pgrx::prelude::*;
extension_sql_file!("../sql/extensions.sql");
extension_sql_file!("../sql/raise_notice.sql");
extension_sql_file!("../sql/sample_source_tables/seller.sql");
extension_sql_file!("../sql/sample_source_tables/customer.sql");
extension_sql_file!("../sql/source_objects.sql");

// Creating Extension TABLES ** Purely ** as a Hello World + Example.

// TABLE_STATUS
// Used pulls table in from the information schema for processing, starting status new.
extension_sql!( r#"

CREATE TABLE table_status (
    id SERIAL PRIMARY KEY,
    schema_name VARCHAR(255) NOT NULL DEFAULT 'public',
    table_name VARCHAR(255) NOT NULL,
    status_text TEXT NOT NULL DEFAULT 'new' CHECK (status_text IN ('new', 'changed', 'ready', 'processed')),
    usage_indicator BOOLEAN NOT NULL DEFAULT TRUE
);
"#,
    name = "create_table_status_table",
);

// TABLE_COLUMN_CAT
// Used to categorize attribute either manually or by LLM.
// If overridden then confidence_level = na and is_overridden = true.
extension_sql!( r#"
CREATE TABLE table_column_cat (
    id SERIAL PRIMARY KEY,
    schema_name VARCHAR(255) NOT NULL DEFAULT 'public',
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    column_cat VARCHAR(255) NOT NULL DEFAULT 'na',
    confidence_level VARCHAR(255) NOT NULL DEFAULT 'na' CHECK (confidence_level IN ('high', 'medium', 'low', 'na')),
    is_overridden BOOLEAN NOT NULL DEFAULT FALSE
);

INSERT INTO table_column_cat (schema_name, table_name, column_name, column_cat, confidence_level, is_overridden)
VALUES
    ('public', 'customer', 'customer_id', 'bk_1', 'high', FALSE),
    ('public', 'customer', 'first_name', 'desc', 'high', FALSE),
    ('public', 'customer', 'last_name', 'desc', 'high', FALSE),
    ('public', 'customer', 'email', 'desc', 'medium', FALSE),
    ('public', 'customer', 'phone_number', 'desc', 'high', FALSE),
    ('public', 'customer', 'date_of_birth', 'desc-p', 'high', FALSE),
    ('public', 'customer', 'last_updated', 'desc', 'high', FALSE),
    ('public', 'customer', 'created_at', 'desc', 'high', FALSE);
"#,
    name = "create_table_column_cat_stats_table",
);