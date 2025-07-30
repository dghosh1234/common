Thanks for sharing the use case. Here's a clear and professional version of your process description, structured for better readability and precision:


---

Data Generation Process: Source to Target Table

Use Case:

When the source and target tables have the same structure.
(For differing structures, use the Custom Query section.)


---

Process Overview:

1. Table Structure Compatibility:

Assumes identical structure between source and target tables (same columns and data types).

If additional columns exist in the target table, synthetic data will be generated for those fields.



2. Insert & Update Statement Generation:

The program automatically generates INSERT and UPDATE statements.

Target table integrity constraints (e.g., primary keys, unique constraints) are considered during generation.



3. Update Logic:

For each record in the source table:

The program checks for a matching record in the target table based on specified criteria.

If a match is found:

An UPDATE statement is generated to modify the existing record.


If no match is found:

An INSERT statement is generated using the source record (effectively performing an "upsert").



If neither source nor target contains a matching record:

The system will generate synthetic data and create an INSERT statement accordingly.






---

Let me know if you'd like this restructured as documentation, code comments, or added to a template.


