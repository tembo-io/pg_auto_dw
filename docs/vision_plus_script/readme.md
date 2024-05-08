## Vision and Script
**Project Vision:** To create an open source extension that automates the data warehouse build.  We aim to do this within a structured environment that incorporates best practices and harnesses the capabilities of Large Language Modes (LLM) technologies.

**Goal:** This extension will enable users to: 
- Automate the DW Build
- Automate DW Maintenance  
- Understand DW Health
- Support Data Governance

All these capabilities will be delivered through a [small set of intuitive functions](../sql_functions/readme.md).

**Script 1:** We want to make building a data warehouse easy.  And if the source tables are structured well and appropriately named, it should take two steps. 
1) Install Extension
2) Build Data Warehouse
![User Story 1](PG_AUTO_DW-Visualized_Demo_Script_0.0.1-User_Story_1.png)

**Script 2:** Sometimes it’s best to get a little push-back when creating a data warehouse, which supports appropriate data governance.  In this instance a table was not ready to deploy to the data warehouse as a table column may need to be considered as sensitive.  In this sample script, Auto DW’s engine, considered knew the attribute is useful for analysis but also may need to be considered sensitive.  In this script the user will:
1) Identify a Skipped Table
2) Identify the Root Cause 
3) Decide to Institute Some Data Governance Best Practices
![User Story 2](PG_AUTO_DW-Visualized_Demo_Script_0.0.1-User_Story_2.png)

**Auto DW Process Flow:** The script highlighted in example 2 demonstrates that there are several approaches to successfully implementing a data warehouse when using this extension. Below is a BPMN diagram that illustrates these various paths. 
![Functions Visualized](PG_AUTO_DW-Visualized-0.0.1-Functions.png)
