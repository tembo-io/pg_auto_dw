## Categories:
**Informative**

# SOURCE_TABLES

Returns a table indicating the status of all tables included for DW automation.

## Syntax
```sql
source_tables()
```

## Usage Notes
Use this function to see the status of source tables in the DW automation process.  Results can be used to identify tables that require additional attention or to understand the DW build status.

## Examples
```sql
SELECT * FROM auto_dw.source_tables();
```
