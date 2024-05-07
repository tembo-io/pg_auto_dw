## Categories:
**[SQL Function - Informative](../readme.md#informative-functions)**

# HEALTH <br> ![Status](https://img.shields.io/badge/status-draft-yellow)

Returns a table indicating the health of all DW automations.

## Syntax
```sql
health()
```

## Usage Notes
Use this function often to understand the state of your data warehouse.  Results can be used to identify operational errors and data availability.

## Examples
```sql
SELECT * FROM auto_dw.health();
```
