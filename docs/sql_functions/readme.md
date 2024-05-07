## SQL Function Documentation 
![Status](https://img.shields.io/badge/status-draft-yellow)

The following SQL functions provide the primary modality for interacting with the extension PG_AUTO_DW. Functions are broken into two categories: informative and interactive. Interactive functions can change the data warehouse (DW).

### Informative Functions
These functions do not affect the database.
|  Availability | Function | Purpose |
|--------------|---------------------------------------|-----------------------------------------------------------------------|
| ![Proposal Version](https://img.shields.io/badge/proposal-0.0.2-blue) | [`health()`](health.md) | Understand DW health. |
| ![Proposal Version](https://img.shields.io/badge/proposal-0.0.1-blue) | [`source_tables()`](source_tables.md) | Understand the status of all tables included for DW automation. |
| ![Proposal Version](https://img.shields.io/badge/proposal-0.0.1-blue) | [`source_columns()`](source_columns.md)| Understand the status of all table columns included for DW automation. |

### Interactive Functions
These functions can only effect the data warehouse portion of the database.
|  Availability | Function | Purpose |
|--------------|---------------------------------------|-----------------------------------------------------------------------|
| ![Proposal Version](https://img.shields.io/badge/proposal-0.0.1-blue) | [`source_include(object_pattern)`](source_include.md) | Add source objects for DW automation. |
| ![Proposal Version](https://img.shields.io/badge/proposal-0.0.1-blue) | [`source_exclude(object_pattern)`](source_exclude.md) | Remove source objects for DW automation. |
| ![Proposal Version](https://img.shields.io/badge/proposal-0.0.2-blue) | [`update_context(object, context)`](update_context.md) | Provide information to facilitate DW automation. |
| ![Proposal Version](https://img.shields.io/badge/proposal-0.0.1-blue) | [`go(flag, status)`](go.md) | Initiates DW builds and dataflows. |
