# pg_auto_dw
An auto data warehouse extension for Postgres

## Principles

* Build in public
    * Public repo
    * Call attention/scrutiny to the work - release every week or two with blog/tweet calling attention to your work
* Documentation-driven development
    * While you’re writing code, write docs on how the product works
* Demo-driven development (recursive, go back to step 1 as needed. It's OK to get things wrong and iterate.)
    * Step 1 - write demo script
    * Step 2 - document vision + demo script in public README
    * Step 3 - mock up demo (fake UX)
    * Step 4 - make fake demo real (narrow use case)
    * Step 5 - ship v24.1 (calver) that can do a little more than just the pre-canned demo
        * Ship product + demo video + documentation

## Demo Script

* I install the extension in my existing postgres cluster
* I run function `auto_dw.evaluate()` - confidence score for each table + field
    * I see one table has a field that needs help
* I edit a table to have a description for a problem field
* I run `auto_dw.evaluate(table)` - and I see the confidence for that table is fixed, green light
* I run `auto_dw.go()` and it sets up everything, new schemas, jobs to keep them updated, etc.
* I add a new table
* I run  `auto_dw.evaluate()` and see the new table, and it’s good
* I run `auto_dw.go()` and it’s now processing new table too
* I don’t want some tables warehoused, I run `auto_dw.omit(table, table)`
* I run `auto_dw.status()` to see those tables are no longer part of the system
* I show the auto_dw dashboard in Tembo Cloud [blocked currently, but let's get @ChuckHend working on this capability]
