## Categories:
**[SQL Function - Informative](readme.md##interactive-functions)**

# GO <br> ![Status](https://img.shields.io/badge/status-draft-yellow)
Initiates data warehouse builds and initiates dataflows.

## Syntax
``` SQL
go(<flag>, <status>)
```

## Usage Notes
Use this function to build an entire data warehouse or push data from a single table into the built dw tables.

## Examples

Build a Data Warehouse
```sql
-- Builds a DW for all source tables that are ready-to-deploy.
SELECT auto_dw.go('Build', 'RTD');
```
&nbsp;&nbsp;&nbsp;&nbsp;or
```sql
-- Builds a DW for all source tables that are ready-to-deploy.
SELECT auto_dw.go(); -- Runs the default, which builds for all tables that are ready-to-deploy.
```
<br>

Perform a Dry Run
```sql
-- Build, Test, and Rollback DW automation for all source tables that are ready-to-deploy.
SELECT auto_dw.go('DryRun', 'RTD');
```

<br>

Push data from a table.
```sql
-- Push Source TABLE MARKETING.PROSPECTS data to the DW.
SELECT auto_dw.go('Push-Table', 'marketing.prospects');
```
