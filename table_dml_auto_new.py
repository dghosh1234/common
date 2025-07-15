import os
import oracledb
import random
import string
from dotenv import load_dotenv, dotenv_values
import sys
from faker import Faker
import yaml
from datetime import datetime
#import readline  # Enables line editing (backspace, delete, arrows) in input() prompts on Linux/macOS
import re

fake = Faker()
#Purpose:
#This script generates mock DELETE, INSERT, and UPDATE SQL statements for a specified Oracle table, supporting multiple Informatica Router-like groups.
#For each group, it prompts for group-specific WHERE conditions and DML options (number of records, order by, update column, etc.), and outputs all group DML blocks into a single .sql file under clear comment headers.
#It also creates a backup table (postfixed _adtf_bkp) using a single CTAS statement containing all records to be deleted across all groups, ensuring safe backup before DML.
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

def write_custom_yaml(yaml_data, filename):
    """Write YAML with list format for sql section"""
    with open(filename, 'w', encoding='utf-8') as f:
        #f.write("# YAML file with custom comment formatting\n\n")
        
        
        for section_key in ['seeds', 'files', 'sql-checks']:
            f.write(f"{section_key}:\n")
            if yaml_data[section_key]:
                for key, value in yaml_data[section_key].items():
                    f.write(f"  {key}: {value}\n")
            f.write("\n")
        
        f.write("sql:\n")
        for sql_item in yaml_data['sql']:
            f.write("  - description: {}\n".format(sql_item['description']))
            
            sql_lines = sql_item['sql'].split('\n')
            if len(sql_lines) > 1:
                f.write("    sql: |\n")
                for line in sql_lines:
                    f.write(f"      {line}\n")
            else:
                f.write(f"    sql: {sql_item['sql']}\n")
            
            f.write(f"    dml: {sql_item['dml']}\n")
            f.write(f"    schema: {sql_item['schema']}\n")
            f.write(f"    db: {sql_item['db']}\n")
            f.write(f"    dbtype: {sql_item['dbtype']}\n")
            f.write(f"    connection_name: {sql_item['connection_name']}\n")


ENV_BASE_PATH = "C:\\Users\\dghos\\Desktop\\GitHub\\table_dml_generation"
def prompt_env_filename(role):
    while True:
        print(f"\nPlease choose connection name - BDI_DW, WAR_DW")
        filename = input(f"Enter the {role} .env filename (e.g., bdi_dw): ").strip()
        full_path = os.path.join(ENV_BASE_PATH, filename)
        if os.path.isfile(full_path):
            if 'bdi_dw' in filename.lower():
                connection_name = 'BDI_DW'
            elif 'war_dw' in filename.lower():
                connection_name = 'WAR_DW'
            else:
                connection_name = filename.upper().replace('.ENV', '')
            return full_path, connection_name
        print(f"File not found: {full_path}")
        print("Please try again with exact filename (bdi_dw or war_dw).")

source_env_path, source_connection_name = prompt_env_filename("source")
dest_env_path, dest_connection_name = prompt_env_filename("destination")

source_env = dotenv_values(source_env_path)
dest_env = dotenv_values(dest_env_path)

source_user = source_env["ORACLE_USER"]
source_password = source_env["ORACLE_PASSWORD"]
source_dsn = source_env["ORACLE_DSN"]

dest_user = dest_env["ORACLE_USER"]
dest_password = dest_env["ORACLE_PASSWORD"]
dest_dsn = dest_env["ORACLE_DSN"]

def get_oracle_service_name(connection):
    """Get the actual Oracle service name from an active connection"""
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT SYS_CONTEXT('USERENV','SERVICE_NAME') FROM DUAL")
        service_name = cursor.fetchone()[0]
        cursor.close()
        return service_name.strip() if service_name else "unknown"
    except Exception as e:
        print(f"[WARNING] Could not retrieve Oracle service name: {e}")
        return "unknown"

print()
source_schema = input("Enter source schema name: ").strip()
print()
use_custom_query = input("Do you want to supply your own query to populate the source data? (Y/N): ").strip().upper()
if use_custom_query == 'Y':
    print("\nPaste your source query below (up to 4000 characters). Press Enter when done:")
    print("(Tip: You can paste multi-line SQL. End input with a blank line or after 4000 characters.)")
    print("(Note: If your query already contains ROWNUM or FETCH FIRST limits, we'll detect them automatically.)")
    user_query_lines = []
    total_chars = 0
    while total_chars < 4000:
        line = input()
        if line == '' and user_query_lines:
            break
        if total_chars + len(line) > 4000:
            line = line[:4000 - total_chars]
        user_query_lines.append(line)
        total_chars += len(line)
        if total_chars >= 4000:
            break
    user_source_query = '\n'.join(user_query_lines)
    
    query_upper = user_source_query.upper()
    has_rownum = 'ROWNUM' in query_upper
    has_fetch_first = 'FETCH FIRST' in query_upper
    
    if has_rownum or has_fetch_first:
        print("[INFO] Detected existing row limiting in query (ROWNUM or FETCH FIRST)")
        print("[INFO] Your query will be executed as-is without additional row limiting.")
        custom_query_limit = None  # No additional limit needed
    else:
        print()
        while True:
            try:
                custom_query_limit = int(input("Enter the maximum number of records to extract from your custom query: ").strip())
                if custom_query_limit > 0:
                    break
                else:
                    print("Please enter a positive number.")
            except ValueError:
                print("Please enter a valid integer.")
        
        print(f"[INFO] Custom query will be limited to {custom_query_limit} records.")
    
    source_table = None 
    selected_source_columns = None  
else:
    source_table = input("Enter source table name: ").strip()
    
    print()
    use_specific_columns = input("Do you want to select specific columns from the source table? (Y/N, default N): ").strip().upper()
    if use_specific_columns == 'Y':
        print("Enter the column names you want to select, separated by commas:")
        print("(Example: ID, NAME, EMAIL, CREATED_DATE)")
        selected_columns_input = input("Columns: ").strip()
        if selected_columns_input:
            selected_source_columns = [col.strip() for col in selected_columns_input.split(',') if col.strip()]
            print(f"[INFO] Will select these columns from source: {selected_source_columns}")
        else:
            selected_source_columns = None
            print("[INFO] No columns specified. Will select all common columns.")
    else:
        selected_source_columns = None
        print("[INFO] Will select all common columns between source and target tables.")

print()
dest_schema = input("Enter destination schema name: ").strip()
print()
dest_table = input("Enter destination table name: ").strip()

if (
    source_env_path == dest_env_path and
    source_schema.lower() == dest_schema.lower() and
    (source_table and source_table.lower() == dest_table.lower())
):
    print("[ERROR] Source and target connection, schema, and table are all the same. Target table name must be different.")
    sys.exit(1)

def table_exists(connection, schema, table):
    cursor = connection.cursor()
    
    found_types = []
    
    try:
        cursor.execute("""
            SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2
        """, (schema.upper(), table.upper()))
        table_count = cursor.fetchone()[0]
        if table_count:
            found_types.append('TABLE')
    except Exception as e:
        pass
    
    try:
        cursor.execute("""
            SELECT COUNT(*) FROM all_views WHERE owner = :1 AND view_name = :2
        """, (schema.upper(), table.upper()))
        view_count = cursor.fetchone()[0]
        if view_count:
            found_types.append('VIEW')
    except Exception as e:
        pass
    
    try:
        cursor.execute("""
            SELECT COUNT(*) FROM all_synonyms WHERE owner = :1 AND synonym_name = :2
        """, (schema.upper(), table.upper()))
        synonym_count = cursor.fetchone()[0]
        if synonym_count:
            found_types.append('SYNONYM')
    except Exception as e:
        pass
    
    try:
        cursor.execute("""
            SELECT COUNT(*) FROM all_synonyms WHERE owner = 'PUBLIC' AND synonym_name = :1
        """, (table.upper(),))
        public_synonym_count = cursor.fetchone()[0]
        if public_synonym_count:
            found_types.append('PUBLIC_SYNONYM')
    except Exception as e:
        pass
    
    try:
        cursor.execute(f"SELECT 1 FROM {schema}.{table} WHERE ROWNUM = 1")
        found_types.append('DIRECT_ACCESS')
    except Exception as e:
        pass
    
    return bool(found_types)
    
try:
    load_dotenv(dotenv_path=dest_env_path, override=False)
    source_connection = oracledb.connect(user=source_user, password=source_password, dsn=source_dsn)
    dest_connection = oracledb.connect(user=dest_user, password=dest_password, dsn=dest_dsn)
    
    source_service_name = get_oracle_service_name(source_connection)
    dest_service_name = get_oracle_service_name(dest_connection)
    print(f"[INFO] Source Oracle service: {source_service_name}")
    print(f"[INFO] Destination Oracle service: {dest_service_name}")
    
    if use_custom_query == 'Y':
        pass  
    else:
        if not table_exists(source_connection, source_schema, source_table):
            print(f"[ERROR] Source table {source_schema}.{source_table} does not exist.")
            sys.exit(1)
    
    if not table_exists(dest_connection, dest_schema, dest_table):
        print(f"[ERROR] Destination table {dest_schema}.{dest_table} does not exist.")
        sys.exit(1)
except oracledb.Error as e:
    print(f"Error connecting to Oracle Database: {e}")
    sys.exit(1)

def get_table_metadata(connection, schema, table):
    cursor = connection.cursor()
    cursor.execute("""
        SELECT column_name, data_type, char_length, data_default, virtual_column FROM all_tab_cols
        WHERE owner = :1 AND table_name = :2
        ORDER BY column_id
    """, (schema.upper(), table.upper()))
    columns_full = cursor.fetchall()
    seq_cols = {}
    for row in columns_full:
        col, dtype, length, default, _ = row
        if default:
            default_str = str(default).replace('"', '').replace(' ', '').upper()
            if '.NEXTVAL' in default_str:
                seq_name = default_str.split('.NEXTVAL')[0]
                if '.' in seq_name:
                    seq_name = seq_name.split('.')[-1]
                seq_cols[col] = seq_name
    cursor.execute("""
        SELECT column_name FROM all_tab_identity_cols
        WHERE owner = :1 AND table_name = :2 AND generation_type = 'ALWAYS'
    """, (schema.upper(), table.upper()))
    always_identity_cols = {row[0] for row in cursor.fetchall()}
    def is_user_column(colname):
        return not re.match(r'^SYS_.*|^SYS_C\\d+.*', colname)
    columns = [
        (row[0], row[1], row[2])
        for row in columns_full
        if row[4] != 'YES' and is_user_column(row[0])
    ]
    seen_cols = set()
    deduped_columns = []
    for col in columns:
        if col[0] not in seen_cols:
            deduped_columns.append(col)
            seen_cols.add(col[0])
    columns = deduped_columns
    cursor.execute("""
        SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols
        WHERE cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name
        AND cons.owner = :1 AND cons.table_name = :2
    """, (schema.upper(), table.upper()))
    pk_cols = [row[0] for row in cursor.fetchall()]
    cursor.execute("""
        SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols
        WHERE cons.constraint_type = 'U' AND cons.constraint_name = cols.constraint_name
        AND cons.owner = :1 AND cons.table_name = :2
    """, (schema.upper(), table.upper()))
    uk_cols = [row[0] for row in cursor.fetchall()]
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
    seen_keys = set()
    deduped_key_cols = []
    for col in key_cols:
        if col not in seen_keys:
            deduped_key_cols.append(col)
            seen_keys.add(col)
    key_cols = deduped_key_cols
    return columns, key_type, key_cols, always_identity_cols, seq_cols

def get_table_check_constraints(connection, schema, table):
    """Return a dict: {column_name: set of allowed values} for columns with simple IN-list check constraints."""
    cursor = connection.cursor()
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
        m = re.search(rf"{col} IN \\(([^)]+)\\)", cond, re.IGNORECASE)
        if m:
            vals = m.group(1)
            allowed = [v.strip().strip("'") for v in vals.split(',')]
            check_map.setdefault(col, set()).update(allowed)
    return check_map

def generate_fake_data(col_type, col_len=None):
    """Generate fake data based on Oracle column type and length"""
    if col_type.upper() in ['VARCHAR2', 'CHAR', 'NVARCHAR2']:
        if col_type.upper() == 'CHAR':
            fake_val = fake.word()
        else:
            fake_val = fake.sentence(nb_words=3)
        
        if col_len and isinstance(col_len, int):
            fake_val = fake_val[:col_len]
        return fake_val, 'STRING'
    
    elif col_type.upper() == 'NUMBER':
        return fake.random_int(min=1, max=9999), 'NUMBER'
    
    elif col_type.upper() == 'DATE':
        return fake.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S'), 'DATE'
    
    elif col_type.upper().startswith('TIMESTAMP'):
        return fake.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S.%f'), 'TIMESTAMP'
    
    elif col_type.upper() == 'CLOB':
        return fake.text(max_nb_chars=100), 'CLOB'
    
    else:
        return fake.word(), 'STRING'

def main():
    gen_final_insert = 'Y'
    try:
        source_connection = oracledb.connect(user=source_user, password=source_password, dsn=source_dsn)
        dest_connection = oracledb.connect(user=dest_user, password=dest_password, dsn=dest_dsn)

        source_service_name = get_oracle_service_name(source_connection)
        dest_service_name = get_oracle_service_name(dest_connection)
        print(f"[INFO] Source Oracle service: {source_service_name}")
        print(f"[INFO] Destination Oracle service: {dest_service_name}")

        if use_custom_query == 'Y':
            src_columns = []
            src_key_type = None
            src_key_cols = []
            src_always_identity_cols = set()
            src_seq_cols = {}
        else:
            src_columns, src_key_type, src_key_cols, src_always_identity_cols, src_seq_cols = get_table_metadata(source_connection, source_schema, source_table)
        tgt_columns, tgt_key_type, tgt_key_cols, tgt_always_identity_cols, tgt_seq_cols = get_table_metadata(dest_connection, dest_schema, dest_table)
        if not tgt_columns:
            print(f"No columns found for table {dest_table} in schema {dest_schema}.")
            return
        if use_custom_query == 'Y':
            src_col_names = []
            tgt_col_names = [col for col, _, _ in tgt_columns]
            common_cols = []  
            missing_in_source = [] 
        else:
            src_col_names = [col for col, _, _ in src_columns]
            tgt_col_names = [col for col, _, _ in tgt_columns]
            
            if selected_source_columns:
                invalid_cols = [col for col in selected_source_columns if col.upper() not in [c.upper() for c in src_col_names]]
                if invalid_cols:
                    print(f"[ERROR] The following columns do not exist in source table {source_schema}.{source_table}: {invalid_cols}")
                    print(f"Available columns in source: {src_col_names}")
                    sys.exit(1)
                
                target_pk_cols = [col for col, _, _ in tgt_columns if col in tgt_key_cols]
                selected_upper = [col.upper() for col in selected_source_columns]
                missing_pk_cols = [col for col in target_pk_cols if col.upper() not in selected_upper]
                if missing_pk_cols:
                    print(f"[WARNING] Primary key columns missing from selection: {missing_pk_cols}")
                    print(f"[INFO] Adding primary key columns to selection for proper DML generation.")
                    for pk_col in missing_pk_cols:
                        if pk_col.upper() in [c.upper() for c in src_col_names]:
                            selected_source_columns.append(pk_col)
                
                common_cols = []
                for user_col in selected_source_columns:
                    matching_tgt_col = next((tgt_col for tgt_col in tgt_col_names if tgt_col.upper() == user_col.upper()), None)
                    if matching_tgt_col:
                        common_cols.append(matching_tgt_col)
                
                not_in_target = []
                for user_col in selected_source_columns:
                    if not any(tgt_col.upper() == user_col.upper() for tgt_col in tgt_col_names):
                        not_in_target.append(user_col)
                if not_in_target:
                    print(f"[WARNING] The following selected columns are not present in target table: {not_in_target}. They will be ignored.")
                
                selected_upper = [col.upper() for col in selected_source_columns]
                target_only = [col for col in tgt_col_names if col.upper() not in selected_upper]
                if target_only:
                    missing_in_source = target_only
                    print(f"[WARNING] The following target columns were not selected from source: {target_only}. They will be filled with fake data in INSERTs.")
                else:
                    missing_in_source = []
                
                print(f"[INFO] Final selected columns for DML: {common_cols}")
            else:
                common_cols = [col for col in tgt_col_names if col in src_col_names]
                missing_in_source = [col for col in tgt_col_names if col not in src_col_names]
                if missing_in_source:
                    print(f"[WARNING] The following columns are present in target but missing in source: {missing_in_source}. They will be filled with fake data in INSERTs.")
        columns = tgt_columns
        key_cols = tgt_key_cols
        always_identity_cols = tgt_always_identity_cols
        seq_cols = tgt_seq_cols
        col_names = [col for col, _, _ in columns]
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
        def get_foreign_keys(connection, schema, table):
            cursor = connection.cursor()
            cursor.execute('''
                SELECT acc.column_name, ac_r.table_name AS r_table
                FROM all_constraints ac
                JOIN all_cons_columns acc ON ac.owner = acc.owner AND ac.constraint_name = acc.constraint_name
                JOIN all_constraints ac_r ON ac.r_owner = ac_r.owner AND ac_r.constraint_name = ac_r.constraint_name
                WHERE ac.constraint_type = 'R' AND ac.owner = :1 AND ac.table_name = :2
            ''', (schema.upper(), table.upper()))
            return [(row[0], row[1]) for row in cursor.fetchall()]
        fk_info = []
        if use_custom_query != 'Y' and source_table:
            fk_info = get_foreign_keys(source_connection, source_schema, source_table)
            if fk_info:
                print("Foreign key columns:")
                for col, ref_table in fk_info:
                    print(f"  {col} -> {ref_table}")
            else:
                print("Foreign key columns: None")
        else:
            print("Foreign key columns: Skipped (custom source query in use)")
        virtual_cols = []
        if use_custom_query != 'Y' and source_table:
            cursor = source_connection.cursor()
            cursor.execute("""
                SELECT column_name FROM all_tab_cols
                WHERE owner = :1 AND table_name = :2 AND virtual_column = 'YES'""", (source_schema.upper(), source_table.upper()))
            virtual_cols = [row[0] for row in cursor.fetchall()]
            print(f"Virtual columns: {virtual_cols if virtual_cols else 'None'}")
        else:
            print("Virtual columns: Skipped (custom source query in use)")
        print()
        updatable_cols = [col for col in col_names if col not in key_cols and col not in always_identity_cols and col not in seq_cols]
        updatable_cols = [col for col, dtype, _ in columns if col not in key_cols and col not in always_identity_cols and col not in seq_cols and dtype.upper() != 'CLOB']
        if not updatable_cols:
            print("No updatable columns available (all are PK, identity, sequence, or CLOB columns). Exiting.")
            return

        router_conditions = []
        
        if use_custom_query == 'Y':
            print("\n[Custom Query Mode] Creating single DML group for custom query data.")
            router_conditions.append({
                'group': 'CUSTOM_DATA',
                'base_where': '',
                'user_where': '',
                'num_records': custom_query_limit,
                'order_cols': key_cols.copy(),
                'num_update_records': 0,
                'update_cols': [],
                'update_values': []
            })
        else:
            print("\n[Router Transformation] Enter routing conditions (like Informatica Router). For each group, you will be prompted for group name, WHERE condition, and all DML options. Type 'done' as the group name to finish.")
            first_group = True
            while True:
                if not first_group:
                    print("\n")
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
                
                while True:
                    try:
                        group_num_records = int(input(f"\nEnter number of records to extract for group '{group_name}': \n").strip())
                        break
                    except ValueError:
                        print("Please enter a valid integer.\n")
                
                orderable_cols = [col for col, dtype, _ in src_columns if dtype.upper() != 'CLOB']
                while True:
                    order_col_input = input(f"\nEnter order by column(s) for group '{group_name}', comma-separated (choose from {orderable_cols}, or leave blank for default PK order): \n").strip().upper()
                    if not order_col_input:
                        group_order_cols = src_key_cols.copy()
                        print(f"[INFO] No order by column provided. Using source primary key columns: {group_order_cols}\n")
                        break
                    group_order_cols = [c.strip() for c in order_col_input.split(',') if c.strip()]
                    if all(any(c.upper() == col.upper() for col in orderable_cols) for c in group_order_cols) and group_order_cols:
                        break
                    print(f"Invalid column(s). Choose from {orderable_cols}.\n")
                
                updatable_cols = [col for col, dtype, _ in columns if col not in key_cols and col not in always_identity_cols and col not in seq_cols and dtype.upper() != 'CLOB']
                if not updatable_cols:
                    print("No updatable columns available (all are PK, identity, sequence, or CLOB columns). Skipping group.\n")
                    continue
                
                while True:
                    try:
                        group_num_update_records = int(input(f"\nEnter number of records to update for group '{group_name}' (1 to {group_num_records}): \n").strip())
                        if 1 <= group_num_update_records <= group_num_records:
                            break
                        else:
                            print(f"Please enter a value between 1 and {group_num_records}.\n")
                    except ValueError:
                        print("Please enter a valid integer.\n")
                
                update_cols = []
                update_values = []
                available_cols = [col for col, dtype, _ in columns if col not in key_cols and col not in always_identity_cols and col not in seq_cols and dtype.upper() != 'CLOB']
                for i in range(int(group_num_update_records)):
                    print(f"\nUpdate {i+1} of {group_num_update_records}")
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
                
                router_conditions.append({
                    'group': group_name,
                    'base_where': where_cond,
                    'user_where': '',
                    'num_records': group_num_records,
                    'order_cols': group_order_cols,
                    'num_update_records': int(group_num_update_records),
                    'update_cols': update_cols,
                    'update_values': update_values
                })
        
        custom_update_statements = []
        create_updates = 'N'
        if use_custom_query == 'Y':
            create_updates = input("\n[Custom Query Updates] Do you want to create UPDATE statements? (Y/N): ").strip().upper()
            if create_updates == 'Y':
                pass
        
        backup_table = f"{dest_table}_adtf_bkp"
        insert_cols = ', '.join([col for col, _, _ in columns])
        pk_cols = key_cols
        backup_ids = []
        backup_id_set = set()
        backup_id_map = {}
        group_rows_map = {}
        used_pk_values = set()
        for group in router_conditions:
            cursor = source_connection.cursor()
            
            if use_custom_query == 'Y':
                print(f"\n[INFO] Executing custom source query for group '{group['group']}'")
                print(f"Original Query: {user_source_query[:200]}..." if len(user_source_query) > 200 else f"Original Query: {user_source_query}")
                
                if custom_query_limit is None:
                    print(f"[INFO] Executing original query with existing row limiting")
                    limited_query = user_source_query
                else:
                    limited_query = f"""
SELECT * FROM (
{user_source_query}
) WHERE ROWNUM <= {custom_query_limit}"""
                    print(f"[INFO] Wrapping query with ROWNUM limit of {custom_query_limit} records")
                
                cursor.execute(limited_query)
                rows = cursor.fetchall()
                
                if use_custom_query == 'Y' and not common_cols:
                    query_column_names = [desc[0].upper() for desc in cursor.description]
                    
                    tgt_col_names = [col for col, _, _ in tgt_columns]
                    common_cols = []
                    for query_col in query_column_names:
                        matching_tgt_col = next((tgt_col for tgt_col in tgt_col_names if tgt_col.upper() == query_col.upper()), None)
                        if matching_tgt_col:
                            common_cols.append(matching_tgt_col)
                        else:
                            print(f"[WARNING] Query column '{query_col}' does not exist in target table and will be ignored.")
                    
                    common_cols_upper = [col.upper() for col in common_cols]
                    missing_in_source = [col for col in tgt_col_names if col.upper() not in common_cols_upper]
                    
                    print(f"[INFO] Columns available from custom query: {common_cols}")
                    if missing_in_source:
                        print(f"[INFO] Target columns missing from query (will use fake data): {missing_in_source}")
                
                group_rows_map[group['group']] = rows
                
                if custom_query_limit is None:
                    print(f"[INFO] Custom query returned {len(rows)} rows (with existing limits)")
                else:
                    print(f"[INFO] Custom query returned {len(rows)} rows (limited to {custom_query_limit})")
                
                for row in rows:
                    pk_values = []
                    for pk_col in pk_cols:
                        if pk_col in common_cols:
                            pk_idx = common_cols.index(pk_col)
                            if pk_idx < len(row):
                                pk_values.append(row[pk_idx])
                            else:
                                pk_values.append(None)
                        else:
                            pk_values.append(None)
                    
                    pk_tuple = tuple(pk_values)
                    if pk_tuple not in backup_id_set and None not in pk_tuple:
                        backup_ids.append(pk_tuple)
                        backup_id_set.add(pk_tuple)
                
                backup_id_map[group['group']] = list(backup_id_set)
                
            else:
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
                
                if used_pk_values:
                    exclude_pks = []
                    for pk_tuple in used_pk_values:
                        cond = ' AND '.join(
                            f"{col} = '{val}'" if isinstance(val, str) else f"{col} = {val}"
                            for col, val in zip(pk_cols, pk_tuple)
                        )
                        exclude_pks.append(f"({cond})")
                    where_clauses.append('NOT (' + ' OR '.join(exclude_pks) + ')')
                
                where_str = ''
                if where_clauses:
                    where_str = ' WHERE ' + ' AND '.join(where_clauses)
                order_str = f" ORDER BY {', '.join(group['order_cols'])} DESC FETCH FIRST {group['num_records']} ROWS ONLY"
                
                if not common_cols:
                    print(f"[ERROR] No columns in common between source and target tables for group '{group['group']}'. Skipping DML generation for this group.")
                    group_rows_map[group['group']] = []
                    continue
                
                select_cols = ', '.join(common_cols)
                select_sql = f"SELECT {select_cols} FROM {source_schema}.{source_table}{where_str}{order_str}"
                cursor.execute(select_sql)
                rows = cursor.fetchall()
                group_rows_map[group['group']] = rows
                
                dest_pk_conditions = []
                for row in rows:
                    pk_values = []
                    for pk_col in pk_cols:
                        if pk_col in common_cols:
                            pk_idx = common_cols.index(pk_col)
                            if pk_idx < len(row):
                                val = row[pk_idx]
                                if val is None:
                                    pk_values.append(f"{pk_col} IS NULL")
                                elif isinstance(val, str):
                                    pk_values.append(f"{pk_col} = '" + val.replace("'", "''") + "'")
                                else:
                                    pk_values.append(f"{pk_col} = {val}")
                    if pk_values:
                        dest_pk_conditions.append('(' + ' AND '.join(pk_values) + ')')
                
                for row in rows:
                    pk_tuple = tuple(row[common_cols.index(pk)] for pk in pk_cols if pk in common_cols)
                    if pk_tuple not in backup_id_set:
                        backup_ids.append(pk_tuple)
                        backup_id_set.add(pk_tuple)
                        used_pk_values.add(pk_tuple)
                
                backup_id_map[group['group']] = [tuple(row[common_cols.index(pk)] for pk in pk_cols if pk in common_cols) for row in rows]
        
        if use_custom_query == 'Y' and create_updates == 'Y':
            all_rows = []
            for group in router_conditions:
                all_rows.extend(group_rows_map[group['group']])
            
            if all_rows:
                print(f"\n[Custom Query Updates] Found {len(all_rows)} rows from custom query.")
                updatable_cols = [col for col, dtype, _ in columns if col not in key_cols and col not in always_identity_cols and col not in seq_cols and dtype.upper() != 'CLOB']
                
                if updatable_cols:
                    while True:
                        try:
                            num_updates = int(input(f"How many records do you want to update (1 to {len(all_rows)}): ").strip())
                            if 1 <= num_updates <= len(all_rows):
                                break
                            else:
                                print(f"Please enter a value between 1 and {len(all_rows)}.")
                        except ValueError:
                            print("Please enter a valid integer.")
                    
                    for i in range(num_updates):
                        print(f"\nUpdate {i+1} of {num_updates}")
                        while True:
                            col_input = input(f"Choose a column to update (choose from {updatable_cols}): ").strip().upper()
                            matches = [c for c in updatable_cols if c.upper() == col_input]
                            if matches:
                                chosen_col = matches[0]
                                break
                            print(f"Invalid column. Choose from {updatable_cols}.")
                        
                        val = input(f"Enter a value or calculation for column '{chosen_col}' (leave blank to auto-generate): ").strip()
                        
                        custom_update_statements.append({
                            'row_index': i,
                            'column': chosen_col,
                            'value': val
                        })
                else:
                    print("No updatable columns available for custom query updates.")
        
        from collections import OrderedDict
        backup_ids_dict = OrderedDict()
        for group in router_conditions:
            rows = group_rows_map[group['group']]
            for row in rows:
                pk_tuple = tuple(row[[col for col, _, _ in columns].index(pk)] for pk in pk_cols)
                if pk_tuple not in backup_ids_dict:
                    backup_ids_dict[pk_tuple] = True
        backup_ids = list(backup_ids_dict.keys())
        seen_pk_cols = set()
        deduped_pk_cols = []
        for col in pk_cols:
            if col not in seen_pk_cols:
                deduped_pk_cols.append(col)
                seen_pk_cols.add(col)
        pk_cols = deduped_pk_cols
        def pk_where_clause(pk_cols, pk_tuple):
            return ' AND '.join(
                f"{col} = '{val}'" if isinstance(val, str) else f"{col} = {val}"
                for col, val in zip(pk_cols, pk_tuple)
            )
        ids_list = ' OR '.join(f"({pk_where_clause(pk_cols, pk_tuple)})" for pk_tuple in backup_ids)
        select_cols = ', '.join([f'"{col}"' for col, _, _ in columns])
        backup_ctas_sql = f"BEGIN\n  EXECUTE IMMEDIATE 'DROP TABLE {dest_schema}.{backup_table} PURGE';\nEXCEPTION\n  WHEN OTHERS THEN\n    IF SQLCODE != -942 THEN\n      RAISE;\n    END IF;\nEND;\n/\n\nBEGIN\n  EXECUTE IMMEDIATE 'CREATE TABLE {dest_schema}.{backup_table} AS SELECT {select_cols} FROM {dest_schema}.{dest_table} WHERE {ids_list}';\nEXCEPTION\n  WHEN OTHERS THEN\n    RAISE;\nEND;\n/\n"
        output_filename = f"mock_{dest_table.lower()}.adtf.yaml"
        
        yaml_data = {
            'seeds': {},
            'files': {},
            'sql-checks': {},
            'sql': []  
        }
        
        yaml_data['sql'].append({
            'description': f'Drop backup table {dest_schema}.{backup_table} if exists',
            'sql': f"DROP TABLE {dest_schema}.{backup_table} PURGE",
            'dml': 'Yes',
            'schema': dest_schema,
            'db': dest_service_name,
            'dbtype': 'oracle',
            'connection_name': dest_connection_name
        })
        
        yaml_data['sql'].append({
            'description': f'Create backup table {dest_schema}.{backup_table} with existing data',
            'sql': f"CREATE TABLE {dest_schema}.{backup_table} AS SELECT {select_cols} FROM {dest_schema}.{dest_table} WHERE {ids_list}",
            'dml': 'Yes',
            'schema': dest_schema,
            'db': dest_service_name,
            'dbtype': 'oracle',
            'connection_name': dest_connection_name
        })
        
        if use_custom_query == 'Y':
            yaml_data['sql'].append({
                'description': f'Count records from custom source query (reference only)',
                'sql': f"-- Custom query returned {sum(len(group_rows_map[group['group']]) for group in router_conditions)} records",
                'dml': 'Yes',
                'schema': source_schema if source_schema else 'N/A',
                'db': source_service_name if 'source_service_name' in locals() else 'custom_query',
                'dbtype': 'oracle',
                'connection_name': source_connection_name
            })
        else:
            yaml_data['sql'].append({
                'description': f'Count records from source table {source_schema}.{source_table}',
                'sql': f"SELECT COUNT(1) FROM {source_schema}.{source_table}",
                'dml': 'Yes',
                'schema': source_schema,
                'db': source_service_name if 'source_service_name' in locals() else 'unknown',
                'dbtype': 'oracle',
                'connection_name': source_connection_name
            })
        
        yaml_data['sql'].append({
            'description': f'Count records from target table {dest_schema}.{dest_table}',
            'sql': f"SELECT COUNT(1) FROM {dest_schema}.{dest_table}",
            'dml': 'Yes',
            'schema': dest_schema,
            'db': dest_service_name,
            'dbtype': 'oracle',
            'connection_name': dest_connection_name
        })
        
        if use_custom_query == 'Y':
            for group in router_conditions:
                group_rows = group_rows_map[group['group']]
                
                if not group_rows:
                    print(f"[INFO] No data found for group {group['group']}.")
                else:
                    delete_conditions = []
                    for row_idx, row in enumerate(group_rows):
                        row_conditions = []
                        for key_col in key_cols:
                            if key_col in common_cols:
                                idx = common_cols.index(key_col)
                                if idx < len(row):
                                    val = row[idx]
                                    if val is None:
                                        row_conditions.append(f"{key_col} IS NULL")
                                    elif isinstance(val, str):
                                        escaped_val = val.replace("'", "''")
                                        row_conditions.append(f"{key_col} = '{escaped_val}'")
                                    else:
                                        row_conditions.append(f"{key_col} = {val}")
                        
                        if row_conditions:
                            delete_conditions.append('(' + ' AND '.join(row_conditions) + ')')
                    
                    if delete_conditions:
                        where_clause = ' OR '.join(delete_conditions)
                        
                        yaml_data['sql'].append({
                            'description': f'Count records to be deleted from group {group["group"]} in target table {dest_schema}.{dest_table}',
                            'sql': f"SELECT COUNT(1) FROM {dest_schema}.{dest_table} WHERE {where_clause}",
                            'dml': 'Yes',
                            'schema': dest_schema,
                            'db': dest_service_name,
                            'dbtype': 'oracle',
                            'connection_name': dest_connection_name
                        })
                        
                        yaml_data['sql'].append({
                            'description': f'Delete {len(group_rows)} records from group {group["group"]} in {dest_schema}.{dest_table}',
                            'sql': f"DELETE FROM {dest_schema}.{dest_table} WHERE {where_clause}",
                            'dml': 'Yes',
                            'schema': dest_schema,
                            'db': dest_service_name,
                            'dbtype': 'oracle',
                            'connection_name': dest_connection_name
                        })
                
                for row_idx, row in enumerate(all_rows):
                    insert_columns = [col for col, _, _ in columns]
                    insert_values = []
                    
                    src_col_idx_map = {col: idx for idx, col in enumerate(common_cols)}
                    for tgt_idx, (col_name, col_type, col_len) in enumerate(columns):
                        if col_name in common_cols:
                            idx = src_col_idx_map.get(col_name)
                            if idx is not None and idx < len(row):
                                val = row[idx]
                            else:
                                val = None
                            
                            if val is None:
                                insert_values.append('NULL')
                            elif col_type.upper() == 'DATE':
                                if isinstance(val, str):
                                    date_str = val.split('.')[0] if '.' in val else val
                                else:
                                    date_str = val.strftime('%Y-%m-%d %H:%M:%S')
                                insert_values.append(f"TO_DATE('{date_str}', 'YYYY-MM-DD HH24:MI:SS')")
                            elif col_type.upper().startswith('TIMESTAMP'):
                                if isinstance(val, str):
                                    ts_str = val
                                    if '.' not in ts_str:
                                        ts_str += '.000000'
                                else:
                                    ts_str = val.strftime('%Y-%m-%d %H:%M:%S.%f')
                                insert_values.append(f"TO_TIMESTAMP('{ts_str}', 'YYYY-MM-DD HH24:MI:SS.FF')")
                            elif isinstance(val, str):
                                if col_len and isinstance(col_len, int):
                                    val = val[:col_len]
                                escaped_val = val.replace("'", "''")
                                insert_values.append(f"'{escaped_val}'")
                            else:
                                insert_values.append(str(val))
                        else:
                            fake_val, val_type = generate_fake_data(col_type, col_len)
                            if val_type == 'STRING':
                                escaped_fake = fake_val.replace("'", "''")
                                insert_values.append(f"'{escaped_fake}'")
                            elif val_type == 'DATE':
                                insert_values.append(f"TO_DATE('{fake_val}', 'YYYY-MM-DD HH24:MI:SS')")
                            elif val_type == 'TIMESTAMP':
                                insert_values.append(f"TO_TIMESTAMP('{fake_val}', 'YYYY-MM-DD HH24:MI:SS.FF')")
                            else:
                                insert_values.append(str(fake_val))
                    
                    values_clause = ', '.join(insert_values)
                    columns_clause = ', '.join(insert_columns)
                    
                    yaml_data['sql'].append({
                        'description': f'Insert record {row_idx + 1} into {dest_schema}.{dest_table}',
                        'sql': f"INSERT INTO {dest_schema}.{dest_table} ({columns_clause}) VALUES ({values_clause})",
                        'dml': 'Yes',
                        'schema': dest_schema,
                        'db': dest_service_name,
                        'dbtype': 'oracle',
                        'connection_name': dest_connection_name
                    })
                
                if custom_update_statements:
                    for update_spec in custom_update_statements:
                        row_idx = update_spec['row_index']
                        if row_idx < len(all_rows):
                            row = all_rows[row_idx]
                            update_col = update_spec['column']
                            update_val = update_spec['value']
                            
                            pre_count_statement, update_statement, post_count_statement = generate_update_statement(
                                row, update_col, update_val, columns, key_cols, common_cols,
                                dest_schema, dest_table, dest_connection_name, dest_service_name, f'record {row_idx + 1}'
                            )
                            
                            if pre_count_statement and update_statement and post_count_statement:
                                yaml_data['sql'].append(pre_count_statement)
                                yaml_data['sql'].append(update_statement)
                                yaml_data['sql'].append(post_count_statement)
            
        else:
            for group in router_conditions:
                rows = group_rows_map[group['group']]
                
                if not rows:
                    print(f"[INFO] No data found for group {group['group']}.")
                else:
                    group_delete_conditions = []
                    for row_idx, row in enumerate(rows):
                        row_conditions = []
                        for key_col in key_cols:
                            if key_col in common_cols:
                                idx = common_cols.index(key_col)
                                if idx < len(row):
                                    val = row[idx]
                                    if val is None:
                                        row_conditions.append(f"{key_col} IS NULL")
                                    elif isinstance(val, str):
                                        escaped_val = val.replace("'", "''")
                                        row_conditions.append(f"{key_col} = '{escaped_val}'")
                                    else:
                                        row_conditions.append(f"{key_col} = {val}")
                        
                        if row_conditions:
                            group_delete_conditions.append('(' + ' AND '.join(row_conditions) + ')')
                    
                    if group_delete_conditions:
                        where_clause = ' OR '.join(group_delete_conditions)
                        
                        yaml_data['sql'].append({
                            'description': f'Count records to be deleted from group {group["group"]} in target table {dest_schema}.{dest_table}',
                            'sql': f"SELECT COUNT(1) FROM {dest_schema}.{dest_table} WHERE {where_clause}",
                            'dml': 'Yes',
                            'schema': dest_schema,
                            'db': dest_service_name,
                            'dbtype': 'oracle',
                            'connection_name': dest_connection_name
                        })
                        
                        yaml_data['sql'].append({
                            'description': f'Delete {len(rows)} records from group {group["group"]} in {dest_schema}.{dest_table}',
                            'sql': f"DELETE FROM {dest_schema}.{dest_table} WHERE {where_clause}",
                            'dml': 'Yes',
                            'schema': dest_schema,
                            'db': dest_service_name,
                            'dbtype': 'oracle',
                            'connection_name': dest_connection_name
                        })
            
            for group in router_conditions:
                rows = group_rows_map[group['group']]
                
                if not rows:
                    continue
                
                for row_idx, row in enumerate(rows):
                    insert_columns = [col for col, _, _ in columns]
                    insert_values = []
                    
                    src_col_idx_map = {col: idx for idx, col in enumerate(common_cols)}
                    for tgt_idx, (col_name, col_type, col_len) in enumerate(columns):
                            if col_name in common_cols:
                                idx = src_col_idx_map.get(col_name)
                                if idx is not None and idx < len(row):
                                    val = row[idx]
                                else:
                                    val = None
                                
                                if val is None:
                                    insert_values.append('NULL')
                                elif col_type.upper() == 'DATE':
                                    if isinstance(val, str):
                                        date_str = val.split('.')[0] if '.' in val else val
                                    else:
                                        date_str = val.strftime('%Y-%m-%d %H:%M:%S')
                                    insert_values.append(f"TO_DATE('{date_str}', 'YYYY-MM-DD HH24:MI:SS')")
                                elif col_type.upper().startswith('TIMESTAMP'):
                                    if isinstance(val, str):
                                        ts_str = val
                                        if '.' not in ts_str:
                                            ts_str += '.000000'
                                    else:
                                        ts_str = val.strftime('%Y-%m-%d %H:%M:%S.%f')
                                    insert_values.append(f"TO_TIMESTAMP('{ts_str}', 'YYYY-MM-DD HH24:MI:SS.FF')")
                                elif isinstance(val, str):
                                    if col_len and isinstance(col_len, int):
                                        val = val[:col_len]
                                    escaped_val = val.replace("'", "''")
                                    insert_values.append(f"'{escaped_val}'")
                                else:
                                    insert_values.append(str(val))
                            else:
                                fake_val, val_type = generate_fake_data(col_type, col_len)
                                if val_type == 'STRING':
                                    escaped_fake = fake_val.replace("'", "''")
                                    insert_values.append(f"'{escaped_fake}'")
                                elif val_type == 'DATE':
                                    insert_values.append(f"TO_DATE('{fake_val}', 'YYYY-MM-DD HH24:MI:SS')")
                                elif val_type == 'TIMESTAMP':
                                    insert_values.append(f"TO_TIMESTAMP('{fake_val}', 'YYYY-MM-DD HH24:MI:SS.FF')")
                                else:
                                    insert_values.append(str(fake_val))
                    
                    values_clause = ', '.join(insert_values)
                    columns_clause = ', '.join(insert_columns)
                    
                    yaml_data['sql'].append({
                        'description': f'Insert record {row_idx + 1} from group {group["group"]} into {dest_schema}.{dest_table}',
                        'sql': f"INSERT INTO {dest_schema}.{dest_table} ({columns_clause}) VALUES ({values_clause})",
                        'dml': 'Yes',
                        'schema': dest_schema,
                        'db': dest_service_name,
                        'dbtype': 'oracle',
                        'connection_name': dest_connection_name
                    })
                
                for i in range(group['num_update_records']):
                    if i < len(rows) and i < len(group['update_cols']):
                        row = rows[i]
                        update_col = group['update_cols'][i]
                        update_val = group['update_values'][i]
                        
                        pre_count_statement, update_statement, post_count_statement = generate_update_statement(
                            row, update_col, update_val, columns, key_cols, common_cols,
                            dest_schema, dest_table, dest_connection_name, dest_service_name, f'record {i + 1} from group {group["group"]}'
                        )
                        
                        if pre_count_statement and update_statement and post_count_statement:
                            yaml_data['sql'].append(pre_count_statement)
                            yaml_data['sql'].append(update_statement)
                            yaml_data['sql'].append(post_count_statement)
        
        yaml_data['sql'].append({
            'description': f'Restore data from backup table {dest_schema}.{backup_table} to {dest_schema}.{dest_table}',
            'sql': f"INSERT INTO {dest_schema}.{dest_table} (SELECT * FROM {dest_schema}.{backup_table})",
            'dml': 'Yes',
            'schema': dest_schema,
            'db': dest_service_name,
            'dbtype': 'oracle',
            'connection_name': dest_connection_name
        })
        
        write_custom_yaml(yaml_data, output_filename)
        
        print(f"\n[INFO] All DML operations have been written to YAML file: {output_filename}")
        print(f"[INFO] Generated {len(yaml_data['sql'])} SQL statements in new format")
        print(f"[INFO] Column mapping summary:")
        print(f"  - Total target table columns: {len([col for col, _, _ in columns])}")
        print(f"  - Columns from source/query: {len(common_cols)} ({common_cols})")
        if missing_in_source:
            print(f"  - Columns populated with fake data: {len(missing_in_source)} ({missing_in_source})")
        else:
            print(f"  - All target columns available from source - no fake data needed")
        print()

    except oracledb.Error as e:
        print(f"Error connecting to Oracle Database: {e}")
    finally:
        if source_connection:
            source_connection.close()
            print("Oracle Database source connection closed.")
        if dest_connection:
            dest_connection.close()
            print("Oracle Database destination connection closed.")

def generate_update_statement(row, update_col, update_val, columns, key_cols, common_cols, dest_schema, dest_table, dest_connection_name, dest_service_name, record_description=""):
    """Generate UPDATE statement with pre-update count, update statement, and post-update count for a given row and column"""
    # Get column metadata
    update_col_idx = [col for col, _, _ in columns].index(update_col)
    update_col_type = columns[update_col_idx][1]
    update_col_length = columns[update_col_idx][2]
    
    if not update_val:
        fake_val, val_type = generate_fake_data(update_col_type, update_col_length)
        if val_type == 'STRING':
            escaped_fake = fake_val.replace("'", "''")
            set_value = f"'{escaped_fake}'"
        elif val_type == 'DATE':
            set_value = f"TO_DATE('{fake_val}', 'YYYY-MM-DD HH24:MI:SS')"
        elif val_type == 'TIMESTAMP':
            set_value = f"TO_TIMESTAMP('{fake_val}', 'YYYY-MM-DD HH24:MI:SS.FF')"
        else:
            set_value = str(fake_val)
    else:
        if update_col_type.upper() in ['VARCHAR2', 'CHAR', 'NVARCHAR2']:
            escaped_val = update_val.replace("'", "''")
            set_value = f"'{escaped_val}'"
        elif update_col_type.upper() == 'DATE':
            set_value = f"TO_DATE('{update_val}', 'YYYY-MM-DD HH24:MI:SS')"
        elif update_col_type.upper().startswith('TIMESTAMP'):
            set_value = f"TO_TIMESTAMP('{update_val}', 'YYYY-MM-DD HH24:MI:SS.FF')"
        else:
            set_value = str(update_val)
    
    where_conditions = []
    for key_col in key_cols:
        if key_col in common_cols:
            idx = common_cols.index(key_col)
            if idx < len(row):
                val = row[idx]
                if val is None:
                    where_conditions.append(f"{key_col} IS NULL")
                elif isinstance(val, str):
                    escaped_val = val.replace("'", "''")
                    where_conditions.append(f"{key_col} = '{escaped_val}'")
                else:
                    where_conditions.append(f"{key_col} = {val}")
    
    if where_conditions:
        where_clause = ' AND '.join(where_conditions)
        
        current_value = None
        if update_col in common_cols:
            idx = common_cols.index(update_col)
            if idx < len(row):
                current_value = row[idx]
        
        # Pre-update count: count records with current value
        pre_count_where_conditions = where_conditions.copy()
        if current_value is not None:
            if isinstance(current_value, str):
                escaped_current = current_value.replace("'", "''")
                pre_count_where_conditions.append(f"{update_col} = '{escaped_current}'")
            else:
                pre_count_where_conditions.append(f"{update_col} = {current_value}")
        else:
            pre_count_where_conditions.append(f"{update_col} IS NULL")
        
        pre_count_where_clause = ' AND '.join(pre_count_where_conditions)
        
        pre_count_statement = {
            'description': f'PRE-UPDATE: Count records to be updated for {update_col} = {set_value} in {record_description} of {dest_schema}.{dest_table}',
            'sql': f"SELECT COUNT(1) FROM {dest_schema}.{dest_table} WHERE {pre_count_where_clause}",
            'dml': 'Yes',
            'schema': dest_schema,
            'db': dest_service_name,
            'dbtype': 'oracle',
            'connection_name': dest_connection_name
        }
        
        update_statement = {
            'description': f'Update {update_col} in {record_description} of {dest_schema}.{dest_table}',
            'sql': f"UPDATE {dest_schema}.{dest_table} SET {update_col} = {set_value} WHERE {where_clause}",
            'dml': 'Yes',
            'schema': dest_schema,
            'db': dest_service_name,
            'dbtype': 'oracle',
            'connection_name': dest_connection_name
        }
        
        # Post-update count: count records with new value and same PK
        post_count_where_conditions = where_conditions.copy()
        post_count_where_conditions.append(f"{update_col} = {set_value}")
        post_count_where_clause = ' AND '.join(post_count_where_conditions)
        
        post_count_statement = {
            'description': f'POST-UPDATE: Verify records updated with {update_col} = {set_value} in {record_description} of {dest_schema}.{dest_table}',
            'sql': f"SELECT COUNT(1) FROM {dest_schema}.{dest_table} WHERE {post_count_where_clause}",
            'dml': 'Yes',
            'schema': dest_schema,
            'db': dest_service_name,
            'dbtype': 'oracle',
            'connection_name': dest_connection_name
        }
        
        return pre_count_statement, update_statement, post_count_statement
    else:
        return None, None, None

if __name__ == "__main__":
    main()


