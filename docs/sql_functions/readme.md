## SQL Function Documentation 
![Status](https://img.shields.io/badge/status-draft-yellow)

The following SQL functions provide the primary modality for interacting with the extension PG_AUTO_DW. Functions are broken into two categories: informative and interactive. Interactive functions can change the data warehouse (DW).

|  Category    | Function                              | Purpose                                                               |
|--------------|---------------------------------------|-----------------------------------------------------------------------|
| Informative  | [`health()`](health.md)               | Understand DW health.                                              |
| Informative  | [`source_tables()`](source_tables.md) | Understand the status of all tables included for DW automation.    |
| Informative  | [`source_columns()`](source_columns.md)| Understand the status of all table columns included for DW automation. |
| Interactive  | [`source_include(object_pattern)`](source_include.md) | Add source objects for DW automation.                              |
| Interactive  | [`source_exclude(object_pattern)`](source_exclude.md) | Remove source objects for DW automation.                           |
| Interactive  | [`update_context(object, context)`](update_context.md) | Provide information to facilitate DW automation.                      |
| Interactive  | [`go(flag, status)`](go.md)           | Initiates DW builds and dataflows.                                    |
