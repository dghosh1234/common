SELECT
    a.owner AS child_owner,
    a.table_name AS child_table,
    a.constraint_name AS fk_constraint,
    acc.column_name AS child_column,
    c.owner AS parent_owner,
    c.table_name AS parent_table,
    pcc.column_name AS parent_column
FROM
    all_constraints a
    JOIN all_cons_columns acc
        ON a.owner = acc.owner
       AND a.constraint_name = acc.constraint_name
    JOIN all_constraints c
        ON a.r_owner = c.owner
       AND a.r_constraint_name = c.constraint_name
    JOIN all_cons_columns pcc
        ON c.owner = pcc.owner
       AND c.constraint_name = pcc.constraint_name
       AND acc.position = pcc.position
WHERE
    a.constraint_type = 'R'  -- Foreign Key
ORDER BY
    child_table, fk_constraint, acc.position;
