It looks like you're trying to write Case One for a scenario where a user chooses to pause a custom query, particularly in situations involving:

Multiple source tables

Complex joins or groupings

Source and target table column names being different


Here's a cleaned-up and clear version of what you're trying to express:


---

✅ Case One: When the user selects to pause a custom query

This case applies in the following scenarios:

When the query involves multiple source tables, complex joins, or GROUP BY clauses.

When the source and target table column names are different.


📌 In such cases, the paused query should follow this format:

SELECT 
    source_column_name1 AS target_column_name1,
    source_column_name2 AS target_column_name2,
    ...
FROM 
    source_table_name;

> Example: If your target table has a column customer_id but the source has cust_id, then the paused query should be:

SELECT 
    cust_id AS customer_id
FROM 
    source_table;



This ensures a smooth mapping between source and target during the transformation or migration process.


---

Would you like me to help format this into documentation, add more examples, or write other cases?

