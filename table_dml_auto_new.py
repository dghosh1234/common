import os
import oracledb
import random
import string
from dotenv import load_dotenv
import sys
from faker import Faker

fake = Faker()

#Purpose:
#This script generates mock DELETE, INSERT, and UPDATE SQL statements for a specified Oracle table, supporting multiple Informatica Router-like groups.
#For each group, it prompts for group-specific WHERE conditions and DML options (number of records, order by, update column, etc.), and outputs all group DML blocks into a single .sql file under clear comment headers.
#It also creates a backup table (postfixed _BKP) using a single CTAS statement containing all records to be deleted across all groups, ensuring safe backup before DML.
#At the end, the script appends commented-out SQL blocks for restoring from backup and dropping the backup table, leaving no active restore/cleanup SQL in the output.
#This tool is ideal for testing, development, and simulating complex router-based DML scenarios in Oracle environments, with robust backup/restore reference logic.

#Developed by: Debasish Ghosh
#Date: July 2025
# GRANT SELECT ANY TABLE ON schema <schema>  TO <target schema>;
#oracledb.init_oracle_client(lib_dir=r"C:\path\to\instantclient")
# Load Oracle connection details from environment variables or a .env file

#dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
#load_dotenv(dotenv_path)
#load_dotenv()
# env_path = ".env"
# if len(sys.argv) > 1:
#     env_path = sys.argv[1]
# load_dotenv(env_path)

# # Oracle connection details are now loaded from environment variables or .env file

# ORACLE_USER = os.getenv('ORACLE_USER')
# ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD')
# ORACLE_DSN = os.getenv('ORACLE_DSN')

# #ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD', 'changeme')
# #ORACLE_DSN = os.getenv('ORACLE_DSN', 'localhost:1521/FREE') 1522 for MAC



# connection = oracledb.connect(
#     user=ORACLE_USER,
#     password=ORACLE_PASSWORD,
#     dsn=ORACLE_DSN
# )

# Remove old env loading logic and replace with prompt-based .env selection
ENV_BASE_PATH = "Path to env file folder"

def prompt_env_filename(role):
    while True:
        print(f"\nPlease choose connection name as name - BDI_DW, WAR_DW")
        filename = input(f"Enter the {role} .env filename (e.g., .env.prod): ").strip()
        full_path = os.path.join(ENV_BASE_PATH, filename)
        if os.path.isfile(full_path):
            return full_path
        print("File not found. Please try again.")

# Prompt for source and destination .env filenames
source_env_path = prompt_env_filename("source")
dest_env_path = prompt_env_filename("destination")

# Load source .env
load_dotenv(dotenv_path=source_env_path, override=True)
source_user = os.getenv("ORACLE_USER")
source_password = os.getenv("ORACLE_PASSWORD")
source_dsn = os.getenv("ORACLE_DSN")

# Load destination .env (override=False to keep both sets in os.environ)
load_dotenv(dotenv_path=dest_env_path, override=False)
dest_user = os.getenv("ORACLE_USER")
dest_password = os.getenv("ORACLE_PASSWORD")
dest_dsn = os.getenv("ORACLE_DSN")

# Prompt for source schema and table
print()
source_schema = input("Enter source schema name: ").strip()
print()
source_table = input("Enter source table name: ").strip()

# Prompt for destination schema and table
print()
dest_schema = input("Enter destination schema name: ").strip()
print()
dest_table = input("Enter destination table name: ").strip()

# Check if all input values are the same (connection, schema, table)
if (
    source_env_path == dest_env_path and
    source_schema.lower() == dest_schema.lower() and
    source_table.lower() == dest_table.lower()
):
    print("[ERROR] Source and target connection, schema, and table are all the same. Target table name must be different.")
    sys.exit(1)

# === Check if source and target tables exist before proceeding ===
def table_exists(connection, schema, table):
    cursor = connection.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2
    """, (schema.upper(), table.upper()))
    return cursor.fetchone()[0] > 0

try:
    source_connection = oracledb.connect(user=source_user, password=source_password, dsn=source_dsn)
    dest_connection = oracledb.connect(user=dest_user, password=dest_password, dsn=dest_dsn)
    if not table_exists(source_connection, source_schema, source_table):
        print(f"[ERROR] Source table {source_schema}.{source_table} does not exist.")
        sys.exit(1)
    if not table_exists(dest_connection, dest_schema, dest_table):
        print(f"[ERROR] Destination table {dest_schema}.{dest_table} does not exist.")
        sys.exit(1)
except oracledb.Error as e:
    print(f"Error connecting to Oracle Database: {e}")
    sys.exit(1)

# Below section to get columns, Primary key (PK), unique key (UK), or treat first column as PK
# Returns: (columns, key_type, key_cols)
def get_table_metadata(connection, schema, table):
    cursor = connection.cursor()
    # Get all columns and their data types, lengths, and defaults for the table
     #SELECT column_name, data_type, char_length, data_default, virtual_column FROM all_tab_columns
    cursor.execute("""
        SELECT column_name, data_type, char_length, data_default, virtual_column FROM all_tab_cols
        WHERE owner = :1 AND table_name = :2
        ORDER BY column_id
    """, (schema.upper(), table.upper()))
    columns_full = cursor.fetchall()
    # Parse sequence columns and their sequence names (robust to quotes, schema, whitespace)
    seq_cols = {}
    for row in columns_full:
        col, dtype, length, default, _ = row
        if default:
            default_str = str(default).replace('"', '').replace(' ', '').upper()
            if '.NEXTVAL' in default_str:
                seq_name = default_str.split('.NEXTVAL')[0]
                # Remove schema if present
                if '.' in seq_name:
                    seq_name = seq_name.split('.')[-1]
                seq_cols[col] = seq_name
    # Detect identity columns with GENERATED ALWAYS
    cursor.execute("""
        SELECT column_name FROM all_tab_identity_cols
        WHERE owner = :1 AND table_name = :2 AND generation_type = 'ALWAYS'
    """, (schema.upper(), table.upper()))
    always_identity_cols = {row[0] for row in cursor.fetchall()}
    # Exclude virtual columns
    columns = [(row[0], row[1], row[2]) for row in columns_full if row[4] != 'YES']  # (name, type, length)

    # Get primary key columns
    cursor.execute("""
        SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols
        WHERE cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name
        AND cons.owner = :1 AND cons.table_name = :2
    """, (schema.upper(), table.upper()))
    pk_cols = [row[0] for row in cursor.fetchall()]
    # Get unique key columns
    cursor.execute("""
        SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols
        WHERE cons.constraint_type = 'U' AND cons.constraint_name = cols.constraint_name
        AND cons.owner = :1 AND cons.table_name = :2
    """, (schema.upper(), table.upper()))
    uk_cols = [row[0] for row in cursor.fetchall()]
    # Use PK if present, else UK, else first column as PK
    key_type = None
    key_cols = []
    if pk_cols:
        key_type = 'PRIMARY'
        key_cols = pk_cols
    elif uk_cols:
        key_type = 'UNIQUE'
        key_cols = uk_cols
    elif columns:
        key_type = 'FIRST_COLUMN_AS_PK'
        key_cols = [columns[0][0]]
    return columns, key_type, key_cols, always_identity_cols, seq_cols

def get_table_check_constraints(connection, schema, table):
    """Return a dict: {column_name: set of allowed values} for columns with simple IN-list check constraints."""
    cursor = connection.cursor()
    # Only handle simple CHECK constraints of the form: CHECK (col IN ('A','B','C'))
    cursor.execute("""
        SELECT acc.column_name, ac.search_condition
        FROM all_constraints ac
        JOIN all_cons_columns acc ON ac.owner = acc.owner AND ac.constraint_name = acc.constraint_name
        WHERE ac.constraint_type = 'C' AND ac.owner = :1 AND ac.table_name = :2
    """, (schema.upper(), table.upper()))
    check_map = {}
    for col, cond in cursor.fetchall():
        if cond is None:
            continue
        # Try to parse: <col> IN ('A','B','C') or <col> IN (1,2,3)
        import re
        m = re.search(rf"{col} IN \\(([^)]+)\\)", cond, re.IGNORECASE)
        if m:
            vals = m.group(1)
            # Remove quotes and spaces, split by comma
            allowed = [v.strip().strip("'") for v in vals.split(',')]
            check_map.setdefault(col, set()).update(allowed)
    return check_map

# 

def main():
    gen_final_insert = 'Y'
    try:
        # Use source connection for source table metadata and data extraction
        source_connection = oracledb.connect(user=source_user, password=source_password, dsn=source_dsn)
        # Use destination connection for destination table metadata and DML generation
        dest_connection = oracledb.connect(user=dest_user, password=dest_password, dsn=dest_dsn)

        # Get source table metadata
        columns, key_type, key_cols, always_identity_cols, seq_cols = get_table_metadata(source_connection, source_schema, source_table)
        if not columns:
            print(f"No columns found for table {source_table} in schema {source_schema}.")
            return
        col_names = [col for col, _, _ in columns]
        # Prompt for primary key column if not detected
        if not key_cols:
            print("No primary or unique key detected.")
            print()
            while True:
                pk_input = input(f"Please specify the primary key column from {col_names}: ").strip().upper()
                matches = [c for c in col_names if c.upper() == pk_input]
                if matches:
                    key_cols = [matches[0]]
                    print(f"Using {matches[0]} as the primary key column.")
                    break
                print(f"Invalid column. Choose from {col_names}.")
                print()
        # Foreign keys
        def get_foreign_keys(connection, schema, table):
            cursor = connection.cursor()
            cursor.execute('''
                SELECT acc.column_name, ac_r.table_name AS r_table
                FROM all_constraints ac
                JOIN all_cons_columns acc ON ac.owner = acc.owner AND ac.constraint_name = acc.constraint_name
                JOIN all_constraints ac_r ON ac.r_owner = ac_r.owner AND ac.r_constraint_name = ac_r.constraint_name
                WHERE ac.constraint_type = 'R' AND ac.owner = :1 AND ac.table_name = :2
            ''', (schema.upper(), table.upper()))
            return [(row[0], row[1]) for row in cursor.fetchall()]
        fk_info = get_foreign_keys(source_connection, source_schema, source_table)
        if fk_info:
            print("Foreign key columns:")
            for col, ref_table in fk_info:
                print(f"  {col} -> {ref_table}")
        else:
            print("Foreign key columns: None")
        cursor = source_connection.cursor()
        cursor.execute("""
            SELECT column_name FROM all_tab_cols
            WHERE owner = :1 AND table_name = :2 AND virtual_column = 'YES'""", (source_schema.upper(), source_table.upper()))
        virtual_cols = [row[0] for row in cursor.fetchall()]
        print(f"Virtual columns: {virtual_cols if virtual_cols else 'None'}")
        print()
        updatable_cols = [col for col in col_names if col not in key_cols and col not in always_identity_cols and col not in seq_cols]
        # Exclude CLOB columns from updatable columns
        updatable_cols = [col for col, dtype, _ in columns if col not in key_cols and col not in always_identity_cols and col not in seq_cols and dtype.upper() != 'CLOB']
        if not updatable_cols:
            print("No updatable columns available (all are PK, identity, sequence, or CLOB columns). Exiting.")
            return
        # Remove all code before the group_conditions loop that references num_records, order_cols, update_col, num_update_records, or user_where
        # This includes:
        # - The prompt for order by columns (order_cols)
        # - The prompt for update column (update_col)
        # - The prompt for number of records to update (num_update_records)
        # - The prompt for user-defined WHERE condition (user_where)
        # - Any use of these variables in SQL generation before the router_conditions loop
        # Only use group['num_records'], group['order_cols'], group['update_col'], group['num_update_records'], group['user_where'] inside multi_group_conditions loop

        # === Router group prompts (already moved after metadata loading) ===
        router_conditions = []
        print("\n[Router Transformation] Enter routing conditions (like Informatica Router). For each group, you will be prompted for group name, WHERE condition, and all DML options. Type 'done' as the group name to finish.")
        first_group = True
        while True:
            if not first_group:
                print("\n")  # Two line gap before new group name
            else:
                first_group = False
            while True:
                group_name = input("Enter group name (or 'done' to finish): \n").strip()
                if not group_name:
                    print("Group name cannot be blank. Please enter a group name or 'done'.\n")
                    continue
                break
            if group_name.lower() == 'done':
                break
            where_cond = input(f"\nEnter base WHERE condition for group '{group_name}': \n").strip()
            # Prompt for number of records to extract for this group
            while True:
                try:
                    group_num_records = int(input(f"\nEnter number of records to extract for group '{group_name}': \n").strip())
                    break
                except ValueError:
                    print("Please enter a valid integer.\n")
            # Prompt for order by columns for this group
            orderable_cols = [col for col, dtype, _ in columns if dtype.upper() != 'CLOB']
            while True:
                order_col_input = input(f"\nEnter order by column(s) for group '{group_name}', comma-separated (choose from {orderable_cols}, or leave blank for default PK order): \n").strip().upper()
                if not order_col_input:
                    group_order_cols = key_cols.copy()
                    print(f"[INFO] No order by column provided. Using primary key columns: {group_order_cols}\n")
                    break
                group_order_cols = [c.strip() for c in order_col_input.split(',') if c.strip()]
                if all(any(c.upper() == col.upper() for col in orderable_cols) for c in group_order_cols) and group_order_cols:
                    break
                print(f"Invalid column(s). Choose from {orderable_cols}.\n")
            # Prompt for updatable columns for this group
            updatable_cols = [col for col, dtype, _ in columns if col not in key_cols and col not in always_identity_cols and col not in seq_cols and dtype.upper() != 'CLOB']
            if not updatable_cols:
                print("No updatable columns available (all are PK, identity, sequence, or CLOB columns). Skipping group.\n")
                continue
            # Prompt for number of records to update for this group (cannot exceed number of inserted records)
            while True:
                try:
                    group_num_update_records = int(input(f"\nEnter number of records to update for group '{group_name}' (1 to {group_num_records}): \n").strip())
                    if 1 <= group_num_update_records <= group_num_records:
                        break
                    else:
                        print(f"Please enter a value between 1 and {group_num_records}.\n")
                except ValueError:
                    print("Please enter a valid integer.\n")
            # For each update, ask which column to update (no repeats), and value (custom or faker)
            update_cols = []
            update_values = []
            available_cols = [col for col, dtype, _ in columns if col not in key_cols and col not in always_identity_cols and col not in seq_cols and dtype.upper() != 'CLOB']
            for i in range(group_num_update_records):
                print(f"\nUpdate {i+1} of {group_num_update_records}")
                # Show only columns not already chosen
                remaining_cols = [col for col in available_cols if col not in update_cols]
                if not remaining_cols:
                    print("No more unique columns left to update for this group.")
                    break
                while True:
                    col_input = input(f"Choose a column to update (choose from {remaining_cols}): ").strip().upper()
                    matches = [c for c in remaining_cols if c.upper() == col_input]
                    if matches:
                        chosen_col = matches[0]
                        update_cols.append(chosen_col)
                        break
                    print(f"Invalid column or already chosen. Choose from {remaining_cols}.")
                val = input(f"Enter a value or calculation for column '{chosen_col}' (leave blank to auto-generate): ").strip()
                update_values.append(val)
            # Store update columns and values for this group
            router_conditions.append({
                'group': group_name,
                'base_where': where_cond,
                'user_where': '',  # Ensure user_where is always present
                'num_records': group_num_records,
                'order_cols': group_order_cols,
                'num_update_records': group_num_update_records,
                'update_cols': update_cols,
                'update_values': update_values
            })
        # === DDL for backup table (target table + _BKP) as CTAS, wrapped in transaction ===
        backup_table = f"{dest_table}_BKP"
        insert_cols = ', '.join([col for col, _, _ in columns])
        pk_col = key_cols[0]
        backup_ids = []  # Use list to preserve order and allow duplicates if needed
        backup_id_set = set()
        backup_id_map = {}
        group_rows_map = {}
        used_pk_values = set()  # Track PKs already selected by previous groups
        for group in router_conditions:
            base_where = group['base_where'].strip()
            user_where = group['user_where'].strip()
            if base_where.lower().startswith('where '):
                base_where = base_where[6:].lstrip()
            if user_where.lower().startswith('where '):
                user_where = user_where[6:].lstrip()
            where_clauses = []
            if base_where:
                where_clauses.append(f"({base_where})")
            if user_where:
                where_clauses.append(f"({user_where})")
            # Exclude PKs already used in previous groups
            if used_pk_values:
                pk_col = key_cols[0]
                exclude_pks = ','.join(f"'{v}'" if isinstance(v, str) else str(v) for v in used_pk_values)
                where_clauses.append(f"{pk_col} NOT IN ({exclude_pks})")
            where_str = ''
            if where_clauses:
                where_str = ' WHERE ' + ' AND '.join(where_clauses)
            order_str = f" ORDER BY {', '.join(group['order_cols'])} DESC FETCH FIRST {group['num_records']} ROWS ONLY"
            cursor = source_connection.cursor()
            # For backup table, still collect PKs from target table
            pk_select_sql = f"SELECT {pk_col} FROM {dest_schema}.{dest_table}{where_str}{order_str}"
            cursor.execute(pk_select_sql)
            ids = [str(row[0]) for row in cursor.fetchall()]
            for id_ in ids:
                if id_ not in backup_id_set:
                    backup_ids.append(id_)
                    backup_id_set.add(id_)
            backup_id_map[group['group']] = ids
            # For DML, collect rows from source table
            select_cols = ', '.join([col for col, _, _ in columns])
            select_sql = f"SELECT {select_cols} FROM {source_schema}.{source_table}{where_str}{order_str}"
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            group_rows_map[group['group']] = rows
            # Add PKs from this group to used_pk_values
            for row in rows:
                pk_val = row[[col for col, _, _ in columns].index(pk_col)]
                used_pk_values.add(pk_val)
        # After collecting group_rows_map, build backup_ids from the actual PK values used in DML (from source rows)
        backup_ids = []
        backup_id_set = set()
        for group in router_conditions:
            rows = group_rows_map[group['group']]
            for row in rows:
                pk_val = row[[col for col, _, _ in columns].index(pk_col)]
                pk_str = f"'{pk_val}'" if isinstance(pk_val, str) else str(pk_val)
                if pk_str not in backup_id_set:
                    backup_ids.append(pk_str)
                    backup_id_set.add(pk_str)
        ids_list = ', '.join(backup_ids)
        select_cols = ', '.join([col for col, _, _ in columns])
        backup_ctas_sql = f"BEGIN\n  EXECUTE IMMEDIATE 'DROP TABLE {dest_schema}.{backup_table} PURGE';\nEXCEPTION\n  WHEN OTHERS THEN\n    IF SQLCODE != -942 THEN\n      RAISE;\n    END IF;\nEND;\n/\n\nBEGIN\n  EXECUTE IMMEDIATE 'CREATE TABLE {dest_schema}.{backup_table} AS SELECT {select_cols} FROM {dest_schema}.{dest_table} WHERE {pk_col} IN ({ids_list})';\nEXCEPTION\n  WHEN OTHERS THEN\n    RAISE;\nEND;\n/\n"
        output_filename = f"mock_{dest_table.lower()}_router.sql"
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write(f"-- Backup table (_BKP) created as CTAS with all deleted records, transactional and safe drop\n{backup_ctas_sql}\n")
        # Refactor DML generation to use router_conditions
        # Create a single output file for all groups
        with open(output_filename, 'a', encoding='utf-8') as f:
            for group in router_conditions:
                f.write(f"\n-- =============================================\n")
                f.write(f"-- Router Group: {group['group']}\n")
                f.write(f"-- =============================================\n")
                insert_cols = ', '.join([col for col, _, _ in columns])
                select_cols = []
                for col, dtype, _ in columns:
                    if dtype.upper() == 'CLOB':
                        select_cols.append(f"DBMS_LOB.SUBSTR({col}, 4000, 1) AS {col}")
                    else:
                        select_cols.append(col)
                select_cols_str = ', '.join(select_cols)
                select_sql = f"SELECT {select_cols_str} FROM {source_schema}.{source_table}"
                where_clauses = []
                base_where = group['base_where'].strip()
                user_where = group['user_where'].strip()
                if base_where.lower().startswith('where '):
                    base_where = base_where[6:].lstrip()
                if user_where.lower().startswith('where '):
                    user_where = user_where[6:].lstrip()
                if base_where:
                    where_clauses.append(f"({base_where})")
                if user_where:
                    where_clauses.append(f"({user_where})")
                if where_clauses:
                    select_sql += " WHERE " + " AND ".join(where_clauses)
                select_sql += f" ORDER BY {', '.join(group['order_cols'])} DESC FETCH FIRST {group['num_records']} ROWS ONLY"
                # Always write the extract query as a commented block for information only
                f.write(f"-- Extract Query for group '{group['group']}':\n")
                f.write(f"-- {select_sql}\n\n")
                print(f"\n[INFO] Extracting data for group '{group['group']}' with query:\n{select_sql}\n")
                rows = group_rows_map[group['group']]
                if not rows:
                    f.write(f"-- [INFO] No data found in source table for group '{group['group']}' with the given criteria.\n\n")
                    print(f"[INFO] No data found in source table for group '{group['group']}' with the given criteria.")
                    continue
                f.write("BEGIN\n")
                # DELETE
                for row in rows:
                    where_clauses = []
                    for key_col in key_cols:
                        idx = [col for col, _, _ in columns].index(key_col)
                        val = row[idx]
                        if val is None:
                            where_clauses.append(f"{key_col} IS NULL")
                        elif isinstance(val, str):
                            where_clauses.append(f"{key_col} = '" + val.replace("'", "''") + "'")
                        else:
                            where_clauses.append(f"{key_col} = {val}")
                    where_str = ' AND '.join(where_clauses)
                    delete_sql = f"DELETE FROM {dest_schema}.{dest_table} WHERE {where_str};"
                    f.write("  " + delete_sql + '\n')
                # INSERT
                for row in rows:
                    values = []
                    for idx, val in enumerate(row):
                        col_name, col_type, _ = columns[idx]
                        if val is None:
                            values.append('NULL')
                        elif col_type.upper() == 'DATE':
                            if isinstance(val, str):
                                date_str = val.split('.')[0] if '.' in val else val
                            else:
                                date_str = val.strftime('%Y-%m-%d %H:%M:%S')
                            values.append(f"TO_DATE('{date_str}', 'YYYY-MM-DD HH24:MI:SS')")
                        elif col_type.upper().startswith('TIMESTAMP'):
                            if isinstance(val, str):
                                ts_str = val
                                if '.' not in ts_str:
                                    ts_str += '.000000'
                            else:
                                ts_str = val.strftime('%Y-%m-%d %H:%M:%S.%f')
                            values.append(f"TO_TIMESTAMP('{ts_str}', 'YYYY-MM-DD HH24:MI:SS.FF')")
                        elif isinstance(val, str):
                            values.append("'" + val.replace("'", "''") + "'")
                        else:
                            values.append(str(val))
                    values_str = ', '.join(values)
                    insert_sql = f"INSERT INTO {dest_schema}.{dest_table} ({insert_cols}) VALUES ({values_str});"
                    f.write("  " + insert_sql + '\n')
                # UPDATE (one column per statement, as chosen by user)
                update_cols = group['update_cols']
                update_values = group['update_values']
                for i, row in enumerate(rows[:group['num_update_records']]):
                    update_col = update_cols[i]
                    update_val = update_values[i] if i < len(update_values) else ''
                    update_col_idx = [col for col, _, _ in columns].index(update_col)
                    update_col_type = columns[update_col_idx][1]
                    if update_val:
                        set_val = update_val
                        if update_col_type.upper() == 'DATE':
                            set_expr = f"{update_col} = TO_DATE('{set_val}', 'YYYY-MM-DD HH24:MI:SS')"
                        elif update_col_type.upper().startswith('TIMESTAMP'):
                            set_expr = f"{update_col} = TO_TIMESTAMP('{set_val}', 'YYYY-MM-DD HH24:MI:SS.FF')"
                        elif update_col_type.upper() == 'NUMBER' and set_val.replace('.', '', 1).isdigit():
                            set_expr = f"{update_col} = {set_val}"
                        else:
                            set_expr = f"{update_col} = '" + set_val.replace("'", "''") + "'"
                    else:
                        if update_col_type.upper() in ['VARCHAR2', 'CHAR', 'NVARCHAR2']:
                            set_val = fake.word() if update_col_type.upper() == 'CHAR' else fake.sentence(nb_words=3)
                        elif update_col_type.upper() == 'NUMBER':
                            set_val = fake.random_int(min=1, max=9999)
                        elif update_col_type.upper() == 'DATE':
                            set_val = fake.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S')
                        elif update_col_type.upper() == 'CLOB':
                            set_val = fake.text(max_nb_chars=100)
                        else:
                            set_val = fake.word()
                        if set_val is None:
                            set_expr = f"{update_col} = NULL"
                        elif update_col_type.upper() == 'DATE':
                            set_expr = f"{update_col} = TO_DATE('{set_val}', 'YYYY-MM-DD HH24:MI:SS')"
                        elif update_col_type.upper().startswith('TIMESTAMP'):
                            set_expr = f"{update_col} = TO_TIMESTAMP('{set_val}', 'YYYY-MM-DD HH24:MI:SS.FF')"
                        elif isinstance(set_val, str):
                            set_expr = f"{update_col} = '" + set_val.replace("'", "''") + "'"
                        else:
                            set_expr = f"{update_col} = {set_val}"
                    where_clauses = []
                    for key_col in key_cols:
                        idx = [col for col, _, _ in columns].index(key_col)
                        val = row[idx]
                        if val is None:
                            where_clauses.append(f"{key_col} IS NULL")
                        elif isinstance(val, str):
                            where_clauses.append(f"{key_col} = '" + val.replace("'", "''") + "'")
                        else:
                            where_clauses.append(f"{key_col} = {val}")
                    where_str = ' AND '.join(where_clauses)
                    update_sql = f"UPDATE {dest_schema}.{dest_table} SET {set_expr} WHERE {where_str};"
                    f.write("  " + update_sql + '\n')
                f.write("  COMMIT;\n")
                f.write("EXCEPTION\n  WHEN OTHERS THEN\n    ROLLBACK;\n    RAISE;\nEND;\n")
                f.write(f"-- End of group: {group['group']}\n\n")
        # At the end, append the same DELETE statements as used for backup, but as a commented block
        with open(output_filename, 'a', encoding='utf-8') as f:
            f.write("\n-- The following block is a commented-out version of the initial backup DELETEs for reference\n")
            f.write("-- BEGIN\n")
            for pk_str in backup_ids:
                delete_sql = f"--   DELETE FROM {dest_schema}.{dest_table} WHERE {pk_col} = {pk_str};\n"
                f.write(delete_sql)
            f.write("--   COMMIT;\n")
            f.write("-- EXCEPTION\n--   WHEN OTHERS THEN\n--     ROLLBACK;\n--     RAISE;\n-- END;\n")
            f.write("-- End of commented-out backup DELETE block\n")
        # At the end, append only the commented-out block to restore data from backup (_BKP) table to the original target table
        # Remove the uncommented restore block
        with open(output_filename, 'a', encoding='utf-8') as f:
            f.write("\n-- The following block is a commented-out restore from backup (_BKP) to the original target table\n")
            f.write("-- BEGIN\n")
            f.write(f"--   INSERT INTO {dest_schema}.{dest_table} (SELECT * FROM {dest_schema}.{backup_table});\n")
            f.write("--   COMMIT;\n")
            f.write("-- EXCEPTION\n--   WHEN OTHERS THEN\n--     ROLLBACK;\n--     RAISE;\n-- END;\n")
            f.write("-- End of commented-out restore block\n")
        # At the end, append a block to drop the backup table (_BKP) as a transaction
        """
        with open(output_filename, 'a', encoding='utf-8') as f:
            f.write("\nBEGIN\n")
            f.write(f"  EXECUTE IMMEDIATE 'DROP TABLE {dest_schema}.{backup_table} PURGE';\n")
            f.write("EXCEPTION\n")
            f.write("  WHEN OTHERS THEN\n")
            f.write("    IF SQLCODE != -942 THEN\n")
            f.write("      RAISE;\n")
            f.write("    END IF;\n")
            f.write("END;\n")
        """
        # At the end, append a commented-out block to drop the backup table (_BKP)
        with open(output_filename, 'a', encoding='utf-8') as f:
            f.write("\n-- The following block is a commented-out drop of the backup (_BKP) table\n")
            f.write("-- BEGIN\n")
            f.write(f"--   EXECUTE IMMEDIATE 'DROP TABLE {dest_schema}.{backup_table} PURGE';\n")
            f.write("-- EXCEPTION\n")
            f.write("--   WHEN OTHERS THEN\n")
            f.write("--     IF SQLCODE != -942 THEN\n")
            f.write("--       RAISE;\n")
            f.write("--     END IF;\n")
            f.write("-- END;\n")
            f.write("-- End of commented-out drop backup block\n")
        print(f"\n[INFO] All generated DELETE, INSERT, and UPDATE statements for all router groups have been written to {output_filename}\n")
    except oracledb.Error as e:
        print(f"Error connecting to Oracle Database: {e}")
    finally:
        if source_connection:
            source_connection.close()
            print("Oracle Database source connection closed.")
        if dest_connection:
            dest_connection.close()
            print("Oracle Database destination connection closed.")

if __name__ == "__main__":
    main()



