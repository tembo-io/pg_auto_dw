## Categories:
**Informative**

# SOURCE_COLUMNS <br> ![Status](https://img.shields.io/badge/status-draft-yellow)

Returns a table indicating the status of all columns included for DW automation.

## Syntax
```sql
source_columns()
```

## Usage Notes
Use this function to see the status of source columns in the DW automation process.  Results can be used to identify table columns that require additional attention.

## Examples
```sql
SELECT * FROM auto_dw.source_columns();
```

