## Categories:
**Interactive**

# UPDATE_CONTEXT
Adds context to objects for DW automation processes.

## Syntax
``` SQL
update_context(<object>, <context>)
```

## Usage Notes
Use this function to add context to SCHEMAS, TABLES, and COLUMNS.

## Examples

Adding a 4 AM Daily Schedule to TABLE ERROR_LOGS
```sql
-- Adds all TABLE ERROR_LOGS COLUMNS to the queue.
SELECT auto_dw.update_context('public.foo', '{"cron": "0 4 * * *"}'
```

<br>

Indicate that COLUMN ZIP does not contain sensitive information.
```sql
SELECT auto_dw.update_context('PUBLIC.CUSTOMER.ZIP', {"sensitive": false});
```

