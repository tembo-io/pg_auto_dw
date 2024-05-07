## Categories:
**[SQL Function - Informative](readme.md##interactive-functions)**

# SOURCE_INCLUDE <br> ![Status](https://img.shields.io/badge/status-draft-yellow)

- Adds objects to the DW automation queue.
- Returns a table indicating objects that have been added to the DW automation queue. 

## Syntax
``` SQL
source_include(<object_pattern>)
```

## Usage Notes
Use this function to add SCHEMAS, TABLES, and COLUMNS to the DW automation queue.  If new attributes have been added to a table you may add them to the queue with this function.

> **Note:** All objects in the PUBLIC schema are added by default upon extension creation.  To remove see example in function source_exclude().

## Examples

Add TABLE ERROR_LOGS
```sql
-- Adds all TABLE ERROR_LOGS COLUMNS to the queue.
SELECT * FROM auto_dw.source_include('logging.error_logs.*');
```

Add SCHEMA MARKETING
```sql
-- Adds all TABLE and TABLE COLUMNS from SCHEMA MARKETING. 
SELECT * FROM auto_dw.source_include('marketing.*.*');
```

Add new COLUMN from TABLE MARKETING.PROSPECTS
```sql
-- Add attribute LAST_REACHED_TS 
SELECT * FROM auto_dw.source_include('marketing.prospects.last_reached_ts');
```

