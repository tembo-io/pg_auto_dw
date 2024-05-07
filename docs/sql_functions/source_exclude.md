## Categories:
**Interactive**

# SOURCE_TABLES

- Removes objects from the DW automation queue.
- Returns a table indicating objects that have been removed from the DW automation queue. 

## Syntax
``` SQL
source_exclude(<object_pattern>)
```

## Usage Notes
Use this function to remove SCHEMAS, TABLES, and COLUMNS from the DW automation queue.

## Examples

All objects in the PUBLIC SCHEMA have been added by default.  To remove SCHEMA PUBLIC issues the following statement.
```sql
-- Remove PUBLIC SCHEMA and associated objects from the queue.
SELECT * FROM auto_dw.source_exclude('PUBLIC');
```

Remove COLUMN from TABLE MARKETING.PROSPECTS.  
```sql
-- Remove attribute LAST_REACHED_TS 
SELECT * FROM auto_dw.source_exclude('marketing.prospects.last_reached_ts');
```
Note: If automations warehoused this column, automations will not remove the associated column or data. 

