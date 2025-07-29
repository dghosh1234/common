import os
import oracledb
import random
import string
from dotenv import load_dotenv, dotenv_values
import sys
from faker import Faker
import yaml
from datetime import datetime
#import readline  
import re

def format_date_for_oracle(date_value, include_time=False):
    try:
        if isinstance(date_value, str):
            input_formats = [
                '%Y-%m-%d %H:%M:%S', 
                '%Y-%m-%d %H:%M:%S.%f', 
                '%Y-%m-%d', 
                '%d-%b-%Y',      # DD-Mon-YYYY (e.g., 25-Dec-2024)
                '%d-%B-%Y',      # DD-Month-YYYY (e.g., 25-December-2024)
                '%d-%m-%Y',      # DD-MM-YYYY
                '%d/%m/%Y',      # DD/MM/YYYY
                '%m/%d/%Y',      # MM/DD/YYYY
                '%Y/%m/%d',      # YYYY/MM/DD
                '%d-%b-%Y %H:%M:%S',  # DD-Mon-YYYY with time
                '%d-%B-%Y %H:%M:%S'   # DD-Month-YYYY with time
            ]
            
            for fmt in input_formats:
                try:
                    dt = datetime.strptime(date_value.split('.')[0], fmt)
                    break
                except ValueError:
                    continue
            else:
                return date_value
        elif hasattr(date_value, 'strftime'):
            dt = date_value
        else:
            return format_date_for_oracle(str(date_value), include_time)
        
        if include_time:
            return dt.strftime('%d-%b-%Y %H:%M:%S')
        else:
            return dt.strftime('%d-%b-%Y')
            
    except (ValueError, AttributeError):
        return str(date_value)

def format_date_for_sql_insert(date_value, col_type):
    if date_value is None:
        return 'NULL'
    
    oracle_date = format_date_for_oracle(date_value, include_time=False)
    
    if col_type.upper() == 'DATE':
        return f"TO_DATE('{oracle_date}', 'DD-Mon-YYYY')"
    elif col_type.upper().startswith('TIMESTAMP'):
        return f"TO_DATE('{oracle_date}', 'DD-Mon-YYYY')"
    else:
        return f"TO_DATE('{oracle_date}', 'DD-Mon-YYYY')"

def format_date_for_where_clause(date_value, column_name):
    if date_value is None:
        return f"{column_name} IS NULL"
    
    oracle_date = format_date_for_oracle(date_value, include_time=False)
    return f"TRUNC({column_name}) = TO_DATE('{oracle_date}', 'DD-Mon-YYYY')"

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
    with open(filename, 'w', encoding='utf-8') as f:
        
        
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


def clean_where_condition(where_condition):
    if not where_condition:
        return ""
    
    where_condition = where_condition.strip()
    
    if where_condition.lower().startswith('where '):
        where_condition = where_condition[6:].lstrip()  
    elif where_condition.lower() == 'where':
        where_condition = "" 
    
    return where_condition


def build_pk_exclusion_clause(used_pk_values, pk_cols):
    if not used_pk_values:
        return ""
    
    exclude_pks = []
    for pk_tuple in used_pk_values:
        pk_conditions = []
        for col, val in zip(pk_cols, pk_tuple):
            if val is None:
                pk_conditions.append(f"{col} IS NULL")
            elif isinstance(val, str):
                escaped_val = val.replace("'", "''")
                pk_conditions.append(f"{col} = '{escaped_val}'")
            else:
                pk_conditions.append(f"{col} = {val}")
        
        if pk_conditions:  
            if len(pk_conditions) == 1:
                exclude_pks.append(pk_conditions[0])
            else:
                exclude_pks.append('(' + ' AND '.join(pk_conditions) + ')')
    
    if exclude_pks:
        if len(exclude_pks) == 1:
            return f'NOT ({exclude_pks[0]})'
        else:
            return 'NOT (' + ' OR '.join(exclude_pks) + ')'
    else:
        return ""


ENV_BASE_PATH = "C:\\Users\\dghos\\Desktop\\GitHub\\table_dml_generation"
def prompt_env_filename(role):
    while True:
        print(f"\nPlease choose connection name - BDI_DW, WAR_DW")
        filename = input(f"Enter the {role} .env filename (e.g., bdi_dw): ").strip()
        if not filename.endswith('.env'):
            filename = filename + '.env'
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
        custom_query_limit = None  
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
    print()
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
    print()
    print(f"[INFO] Source Oracle service: {source_service_name}")
    print()
    print(f"[INFO] Destination Oracle service: {dest_service_name}")
    print()
    
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
        fake_date = fake.date_time_this_decade()
        return format_date_for_oracle(fake_date, include_time=False), 'DATE'
    
    elif col_type.upper().startswith('TIMESTAMP'):
        fake_date = fake.date_time_this_decade()
        return format_date_for_oracle(fake_date, include_time=False), 'TIMESTAMP'
    
    elif col_type.upper() == 'CLOB':
        return fake.text(max_nb_chars=100), 'CLOB'
    
    else:
        return fake.word(), 'STRING'

def get_column_info(columns, col_name):
    for col, dtype, length in columns:
        if col == col_name:
            return dtype, length
    return None, None

def validate_input_for_datatype(user_input, col_type, col_length):
    if not user_input.strip():
        return True, ""  
    
    user_input = user_input.strip()
    
    try:
        if col_type.upper() == 'NUMBER':
            if '.' in user_input:
                float(user_input)
            else:
                int(user_input)
            return True, ""
            
        elif col_type.upper() in ['VARCHAR2', 'CHAR', 'NVARCHAR2']:
            if col_length and len(user_input) > col_length:
                return False, f"Input too long. Maximum length is {col_length} characters."
            return True, ""
            
        elif col_type.upper() == 'DATE':
            from datetime import datetime
            date_formats = [
                '%Y-%m-%d',
                '%Y-%m-%d %H:%M:%S',
                '%d-%m-%Y',
                '%d/%m/%Y',
                '%m/%d/%Y',
                '%Y/%m/%d',
                '%d-%b-%Y',  # DD-Mon-YYYY format (e.g., 25-Dec-2024)
                '%d-%B-%Y'   # DD-Month-YYYY format (e.g., 25-December-2024)
            ]
            
            parsed = False
            for fmt in date_formats:
                try:
                    datetime.strptime(user_input, fmt)
                    parsed = True
                    break
                except ValueError:
                    continue
                    
            if not parsed:
                return False, "Invalid date format. Use formats like: DD-Mon-YYYY (25-Dec-2024), YYYY-MM-DD, DD/MM/YYYY, etc."
            return True, ""
            
        elif col_type.upper().startswith('TIMESTAMP'):
            from datetime import datetime
            timestamp_formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M:%S.%f',
                '%Y-%m-%d',
                '%d-%m-%Y %H:%M:%S',
                '%d/%m/%Y %H:%M:%S',
                '%d-%b-%Y',  # DD-Mon-YYYY format (e.g., 25-Dec-2024)
                '%d-%B-%Y',  # DD-Month-YYYY format (e.g., 25-December-2024)
                '%d-%b-%Y %H:%M:%S',  # DD-Mon-YYYY with time
                '%d-%B-%Y %H:%M:%S'   # DD-Month-YYYY with time
            ]
            
            parsed = False
            for fmt in timestamp_formats:
                try:
                    datetime.strptime(user_input, fmt)
                    parsed = True
                    break
                except ValueError:
                    continue
                    
            if not parsed:
                return False, "Invalid timestamp format. Use formats like: DD-Mon-YYYY (25-Dec-2024), YYYY-MM-DD HH:MM:SS, etc."
            return True, ""
            
        else:
            return True, ""
            
    except ValueError:
        if col_type.upper() == 'NUMBER':
            return False, "Invalid number format. Please enter a valid number."
        else:
            return False, f"Invalid input for {col_type} column."

def format_datatype_info(col_type, col_length):
    if col_type.upper() in ['VARCHAR2', 'CHAR', 'NVARCHAR2'] and col_length:
        return f"{col_type}({col_length})"
    else:
        return col_type

def get_input_examples(col_type):
    examples = {
        'NUMBER': "Examples: 123, 45.67, -89",
        'VARCHAR2': "Examples: 'Hello World', 'Text value'",
        'CHAR': "Examples: 'A', 'Fixed text'", 
        'NVARCHAR2': "Examples: 'Unicode text', 'Special chars'",
        'DATE': "Examples: 25-Dec-2024, 01-Jan-2025, 15-Jun-2024",
        'TIMESTAMP': "Examples: 25-Dec-2024, 01-Jan-2025, 15-Jun-2024"
    }
    
    col_type_upper = col_type.upper()
    if col_type_upper.startswith('TIMESTAMP'):
        return examples.get('TIMESTAMP', f"Example values for {col_type}")
    
    return examples.get(col_type_upper, f"Enter appropriate value for {col_type}")

def get_target_foreign_keys(connection, schema, table):
    cursor = connection.cursor()
    cursor.execute('''
        SELECT 
            fk.column_name,
            pk.owner as ref_schema,
            pk.table_name as ref_table,
            pk.column_name as ref_column
        FROM all_cons_columns fk
        JOIN all_constraints fc ON fk.owner = fc.owner AND fk.constraint_name = fc.constraint_name
        JOIN all_constraints pc ON fc.r_owner = pc.owner AND fc.r_constraint_name = pc.constraint_name
        JOIN all_cons_columns pk ON pc.owner = pk.owner AND pc.constraint_name = pk.constraint_name
        WHERE fc.constraint_type = 'R' 
        AND fk.owner = :1 
        AND fk.table_name = :2
        ORDER BY fk.column_name
    ''', (schema.upper(), table.upper()))
    return [(row[0], row[1], row[2], row[3]) for row in cursor.fetchall()]

def get_target_unique_constraints(connection, schema, table):
    cursor = connection.cursor()
    cursor.execute('''
        SELECT DISTINCT acc.column_name
        FROM all_constraints ac
        JOIN all_cons_columns acc ON ac.owner = acc.owner AND ac.constraint_name = acc.constraint_name
        WHERE ac.constraint_type IN ('U', 'P') 
        AND ac.owner = :1 
        AND ac.table_name = :2
        ORDER BY acc.column_name
    ''', (schema.upper(), table.upper()))
    return [row[0] for row in cursor.fetchall()]

def get_fk_reference_data(connection, ref_schema, ref_table, ref_column, limit=1000):
    cursor = connection.cursor()
    try:
        cursor.execute(f'''
            SELECT DISTINCT {ref_column} 
            FROM {ref_schema}.{ref_table} 
            WHERE {ref_column} IS NOT NULL 
            AND ROWNUM <= {limit}
            ORDER BY {ref_column}
        ''')
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"[WARNING] Could not fetch FK reference data from {ref_schema}.{ref_table}.{ref_column}: {e}")
        return []

def generate_unique_values(col_type, col_length, existing_values, count=100):
    unique_values = set()
    max_attempts = count * 10  
    attempts = 0
    
    while len(unique_values) < count and attempts < max_attempts:
        fake_val, _ = generate_fake_data(col_type, col_length)
        
        if isinstance(fake_val, str):
            comparable_val = fake_val.upper()
        else:
            comparable_val = fake_val
            
        is_duplicate = False
        for existing in existing_values:
            if isinstance(existing, str) and isinstance(comparable_val, str):
                if existing.upper() == comparable_val:
                    is_duplicate = True
                    break
            elif existing == comparable_val:
                is_duplicate = True
                break
                
        if not is_duplicate and comparable_val not in unique_values:
            unique_values.add(fake_val)
            
        attempts += 1
    
    return list(unique_values)

def prepare_smart_column_data(connection, dest_schema, dest_table, missing_in_source, columns):
    smart_data = {}
    
    target_fks = get_target_foreign_keys(connection, dest_schema, dest_table)
    target_unique_cols = get_target_unique_constraints(connection, dest_schema, dest_table)
    
    
    for col_name in missing_in_source:
        col_metadata = next((col for col in columns if col[0] == col_name), None)
        if not col_metadata:
            continue
            
        col, col_type, col_length = col_metadata
        
        fk_info = next((fk for fk in target_fks if fk[0] == col_name), None)
        
        if fk_info:
            fk_col, ref_schema, ref_table, ref_column = fk_info
            print(f"  - Column '{col_name}' is FK -> {ref_schema}.{ref_table}.{ref_column}")
            
            fk_values = get_fk_reference_data(connection, ref_schema, ref_table, ref_column)
            if fk_values:
                smart_data[col_name] = {
                    'type': 'FK',
                    'values': fk_values,
                    'ref_info': f"{ref_schema}.{ref_table}.{ref_column}"
                }
            else:
                print(f"    No reference values found, will use fake data")
                smart_data[col_name] = {'type': 'FAKE', 'values': []}
                
        elif col_name in target_unique_cols:
            print(f"  - Column '{col_name}' has UNIQUE constraint")
            
            cursor = connection.cursor()
            try:
                cursor.execute(f"SELECT DISTINCT {col_name} FROM {dest_schema}.{dest_table} WHERE {col_name} IS NOT NULL")
                existing_values = [row[0] for row in cursor.fetchall()]
            except:
                existing_values = []
            
            unique_vals = generate_unique_values(col_type, col_length, existing_values, 1000)
            smart_data[col_name] = {
                'type': 'UNIQUE',
                'values': unique_vals,
                'existing_count': len(existing_values)
            }
            print(f"    Generated {len(unique_vals)} unique values (avoiding {len(existing_values)} existing)")
            
        else:
            smart_data[col_name] = {'type': 'FAKE', 'values': []}
    
    return smart_data

def get_smart_column_value(smart_data, col_name, col_type, col_length, row_index=0):
    if col_name not in smart_data:
        fake_val, _ = generate_fake_data(col_type, col_length)
        return fake_val
    
    col_data = smart_data[col_name]
    
    if col_data['type'] == 'FK' and col_data['values']:
        fk_values = col_data['values']
        return fk_values[row_index % len(fk_values)]
        
    elif col_data['type'] == 'UNIQUE' and col_data['values']:
        unique_values = col_data['values']
        if row_index < len(unique_values):
            return unique_values[row_index]
        else:
            existing = col_data['values'] + [unique_values[i] for i in range(min(row_index, len(unique_values)))]
            new_unique = generate_unique_values(col_type, col_length, existing, 1)
            if new_unique:
                col_data['values'].extend(new_unique)
                return new_unique[0]
    
    fake_val, _ = generate_fake_data(col_type, col_length)
    return fake_val

def generate_mock_records(connection, dest_schema, dest_table, columns, pk_cols, common_cols, missing_in_source, 
                         smart_column_data, num_records, used_pk_values=None, group_name="MOCK", foreign_keys=None):
    if used_pk_values is None:
        used_pk_values = set()
    
    
    mock_records = []
    max_attempts = num_records * 10 
    attempts = 0
    
    while len(mock_records) < num_records and attempts < max_attempts:
        attempts += 1
        mock_row = []
        
        for col_idx, (col_name, col_type, col_length) in enumerate(columns):
            if col_name in common_cols:
                if col_name in pk_cols:
                    if col_type.upper() == 'NUMBER':
                        max_existing = 0
                        if used_pk_values:
                            try:
                                max_existing = max([pk_tuple[pk_cols.index(col_name)] 
                                                  for pk_tuple in used_pk_values 
                                                  if len(pk_tuple) > pk_cols.index(col_name) 
                                                  and isinstance(pk_tuple[pk_cols.index(col_name)], (int, float))])
                            except (ValueError, IndexError, TypeError):
                                max_existing = 0
                        
                        fake_val = max_existing + len(mock_records) + 1000 + fake.random_int(min=1, max=100)
                    else:
                        fake_val = f"{group_name}_{col_name}_{len(mock_records) + 1}_{fake.random_int(min=100, max=999)}"
                        if col_length and len(fake_val) > col_length:
                            fake_val = fake_val[:col_length]
                else:
                    fake_val, _ = generate_fake_data(col_type, col_length)
                
                mock_row.append(fake_val)
                
            elif col_name in missing_in_source:
                smart_val = get_smart_column_value(smart_column_data, col_name, col_type, col_length, len(mock_records))
                mock_row.append(smart_val)
                
            else:
                fake_val, _ = generate_fake_data(col_type, col_length)
                mock_row.append(fake_val)
        
        pk_values = []
        for pk_col in pk_cols:
            if pk_col in [col for col, _, _ in columns]:
                pk_idx = [col for col, _, _ in columns].index(pk_col)
                if pk_idx < len(mock_row):
                    pk_values.append(mock_row[pk_idx])
        
        pk_tuple = tuple(pk_values) if pk_values else None
        
        if pk_tuple and pk_tuple not in used_pk_values:
            if foreign_keys:
                try:
                    corrected_mock_row, fk_corrections_made, fk_messages = validate_and_correct_fk_values(
                        connection, foreign_keys, columns, list(mock_row), "INSERT"
                    )
                    if fk_corrections_made:
                        print(f"[MOCK DATA FK] Applied FK corrections for record {len(mock_records) + 1}: {fk_messages}")
                        mock_row = corrected_mock_row
                except Exception as e:
                    print(f"[MOCK DATA FK] Warning: FK validation failed for record {len(mock_records) + 1}: {e}")
            
            mock_records.append(tuple(mock_row))
            used_pk_values.add(pk_tuple)
            print(f"[MOCK DATA] Generated record {len(mock_records)}: PK={pk_tuple}")
        elif attempts < max_attempts // 2:
            continue
        else:
            for pk_col in pk_cols:
                if pk_col in [col for col, _, _ in columns]:
                    pk_idx = [col for col, _, _ in columns].index(pk_col)
                    col_type = columns[pk_idx][1]
                    if col_type.upper() == 'NUMBER':
                        mock_row[pk_idx] = fake.random_int(min=10000, max=99999) + attempts
                    else:
                        mock_row[pk_idx] = f"{group_name}_FORCE_{attempts}_{fake.random_int(min=100, max=999)}"
            
            pk_values = []
            for pk_col in pk_cols:
                if pk_col in [col for col, _, _ in columns]:
                    pk_idx = [col for col, _, _ in columns].index(pk_col)
                    if pk_idx < len(mock_row):
                        pk_values.append(mock_row[pk_idx])
            
            pk_tuple = tuple(pk_values) if pk_values else None
            if pk_tuple and pk_tuple not in used_pk_values:
                if foreign_keys:
                    try:
                        corrected_mock_row, fk_corrections_made, fk_messages = validate_and_correct_fk_values(
                            connection, foreign_keys, columns, list(mock_row), "INSERT"
                        )
                        if fk_corrections_made:
                            print(f"[MOCK DATA FK] Applied FK corrections for forced record {len(mock_records) + 1}: {fk_messages}")
                            mock_row = corrected_mock_row
                    except Exception as e:
                        print(f"[MOCK DATA FK] Warning: FK validation failed for forced record {len(mock_records) + 1}: {e}")
                
                mock_records.append(tuple(mock_row))
                used_pk_values.add(pk_tuple)
                print(f"[MOCK DATA] Generated record {len(mock_records)} (forced unique): PK={pk_tuple}")
    
    return mock_records

def generate_mock_update_data(columns, updatable_cols, num_updates, group_name="MOCK"):
    print(f"\n[MOCK UPDATE] Generating {num_updates} mock UPDATE operations for group '{group_name}'")
    
    if not updatable_cols:
        print(f"[MOCK UPDATE] No updatable columns available")
        return [], []
    
    update_cols = []
    update_values = []
    
    updatable_metadata = [(col, dtype, length) for col, dtype, length in columns if col in updatable_cols]
    
    for i in range(min(num_updates, len(updatable_metadata))):
        col_name, col_type, col_length = updatable_metadata[i]
        
        if 'NAME' in col_name.upper():
            if 'FIRST' in col_name.upper():
                mock_val = fake.first_name()
            elif 'LAST' in col_name.upper():
                mock_val = fake.last_name()
            else:
                mock_val = fake.name()
        elif 'EMAIL' in col_name.upper():
            mock_val = fake.email()
        elif 'PHONE' in col_name.upper():
            mock_val = fake.phone_number()
        elif 'ADDRESS' in col_name.upper():
            mock_val = fake.address().replace('\n', ' ')
        elif 'CITY' in col_name.upper():
            mock_val = fake.city()
        elif 'STATE' in col_name.upper():
            mock_val = fake.state_abbr()
        elif 'COUNTRY' in col_name.upper():
            mock_val = fake.country_code()
        elif 'SALARY' in col_name.upper() or 'AMOUNT' in col_name.upper():
            mock_val = fake.random_int(min=30000, max=150000)
        elif 'DATE' in col_name.upper():
            mock_val = format_date_for_oracle(fake.date_this_decade(), include_time=False)
        else:
            mock_val, _ = generate_fake_data(col_type, col_length)
        
        if col_type.upper() in ['VARCHAR2', 'CHAR', 'NVARCHAR2'] and col_length:
            if isinstance(mock_val, str) and len(mock_val) > col_length:
                mock_val = mock_val[:col_length]
        
        update_cols.append(col_name)
        update_values.append(mock_val)
        
        print(f"[MOCK UPDATE] Column '{col_name}' ({col_type}) → '{mock_val}'")
    
    return update_cols, update_values

def check_target_records_exist(connection, schema, table, where_condition, pk_cols, common_cols):
    cursor = connection.cursor()
    
    where_condition = clean_where_condition(where_condition)
    
    if not where_condition:
        return [], []
    
    select_cols = ', '.join([col for col in pk_cols if col in common_cols])
    if not select_cols:
        print("[WARNING] No primary key columns available in common columns for target table check")
        return [], []
    
    check_sql = f"SELECT {select_cols} FROM {schema}.{table} WHERE {where_condition}"
    
    try:
        print(f"[INFO] Checking target table for existing records: {check_sql}")
        cursor.execute(check_sql)
        existing_records = cursor.fetchall()
        
        if existing_records:
            print(f"[INFO] Found {len(existing_records)} matching records in target table")
            return existing_records, [col for col in pk_cols if col in common_cols]
        else:
            print(f"[INFO] No matching records found in target table")
            return [], []
            
    except Exception as e:
        print(f"[ERROR] Failed to check target table: {e}")
        return [], []

def check_individual_record_exists(connection, schema, table, pk_cols, common_cols, row):
    cursor = connection.cursor()
    
    pk_conditions = []
    for pk_col in pk_cols:
        if pk_col in common_cols:
            pk_idx = common_cols.index(pk_col)
            if pk_idx < len(row):
                val = row[pk_idx]
                if val is None:
                    pk_conditions.append(f"{pk_col} IS NULL")
                elif isinstance(val, str):
                    escaped_val = val.replace("'", "''")
                    pk_conditions.append(f"{pk_col} = '{escaped_val}'")
                else:
                    pk_conditions.append(f"{pk_col} = {val}")
    
    if not pk_conditions:
        return False
    
    where_clause = ' AND '.join(pk_conditions)
    check_sql = f"SELECT 1 FROM {schema}.{table} WHERE {where_clause}"
    
    try:
        cursor.execute(check_sql)
        result = cursor.fetchone()
        exists = result is not None
        if exists:
            print(f"[DEBUG] Record exists in target table: {where_clause}")
        else:
            print(f"[DEBUG] Record NOT found in target table: {where_clause}")
        return exists
    except Exception as e:
        print(f"[ERROR] Failed to check individual record: {e}")
        return False

def get_foreign_key_constraints(connection, schema, table):
    cursor = connection.cursor()
    
    fk_query = """
    SELECT 
        cc.column_name,
        rc.table_name as ref_table,
        rc.column_name as ref_column,
        cc.constraint_name,
        c.r_owner as ref_owner
    FROM all_cons_columns cc
    JOIN all_constraints c ON cc.constraint_name = c.constraint_name AND cc.owner = c.owner
    JOIN all_cons_columns rc ON c.r_constraint_name = rc.constraint_name AND c.r_owner = rc.owner
    WHERE cc.table_name = UPPER(:1) 
    AND cc.owner = UPPER(:2)
    AND c.constraint_type = 'R'
    ORDER BY cc.column_name, cc.position
    """
    
    
    try:
        cursor.execute(fk_query, [table, schema])
        fk_data = cursor.fetchall()
        
        if not fk_data:
            print(f"[DEBUG_FK] No results with all_cons_columns, trying user_cons_columns")
            user_fk_query = """
            SELECT 
                cc.column_name,
                rc.table_name as ref_table,
                rc.column_name as ref_column,
                cc.constraint_name,
                c.owner as ref_owner
            FROM user_cons_columns cc
            JOIN user_constraints c ON cc.constraint_name = c.constraint_name
            JOIN user_cons_columns rc ON c.r_constraint_name = rc.constraint_name
            WHERE cc.table_name = UPPER(:1)
            AND c.constraint_type = 'R'
            ORDER BY cc.column_name, cc.position
            """
            cursor.execute(user_fk_query, [table])
            fk_data = cursor.fetchall()
        
        
        foreign_keys = {}
        for row in fk_data:
            column_name, ref_table, ref_column, constraint_name, ref_owner = row
            foreign_keys[column_name.upper()] = {
                'ref_table': ref_table.upper(),
                'ref_column': ref_column.upper(),
                'constraint_name': constraint_name,
                'ref_owner': ref_owner if ref_owner else schema.upper()
            }
            
        if foreign_keys:
            print(f"Found {len(foreign_keys)} foreign key constraints for table {schema}.{table}")
            for col, fk_info in foreign_keys.items():
                print(f"  - {col} references {fk_info['ref_owner']}.{fk_info['ref_table']}.{fk_info['ref_column']}")
        else:
            print(f"[INFO] No foreign key constraints found for table {schema}.{table}")
            
        return foreign_keys
        
    except Exception as e:
        print(f"[ERROR] Failed to retrieve foreign key constraints: {e}")
        print(f"Exception details: {str(e)}")
        return {}

def validate_foreign_key_value(connection, fk_info, value, column_name):
    if value is None:
        return True, None, f"NULL value allowed for FK column {column_name}"
    
    cursor = connection.cursor()
    ref_table = fk_info['ref_table']
    ref_column = fk_info['ref_column']
    ref_owner = fk_info['ref_owner']
    
    check_query = f"SELECT 1 FROM {ref_owner}.{ref_table} WHERE {ref_column} = :1"
    
    try:
        cursor.execute(check_query, [value])
        result = cursor.fetchone()
        
        if result:
            return True, value, f"FK value '{value}' is valid for {column_name}"
        else:
            print(f"[WARNING] FK value '{value}' not found in {ref_owner}.{ref_table}.{ref_column}")
            
            fallback_query = f"SELECT {ref_column} FROM {ref_owner}.{ref_table} WHERE ROWNUM = 1"
            cursor.execute(fallback_query)
            fallback_result = cursor.fetchone()
            
            if fallback_result:
                corrected_value = fallback_result[0]
                print(f"[FIX] Using fallback FK value '{corrected_value}' for {column_name}")
                return False, corrected_value, f"FK value corrected from '{value}' to '{corrected_value}' for {column_name}"
            else:
                print(f"[ERROR] No valid values found in referenced table {ref_owner}.{ref_table}")
                return False, None, f"No valid FK values available for {column_name}"
                
    except Exception as e:
        print(f"[ERROR] Failed to validate FK value '{value}' for {column_name}: {e}")
        return False, value, f"FK validation failed for {column_name}: {e}"

def get_valid_foreign_key_values(connection, fk_info, limit=10):
    cursor = connection.cursor()
    ref_table = fk_info['ref_table']
    ref_column = fk_info['ref_column']
    ref_owner = fk_info['ref_owner']
    
    query = f"SELECT DISTINCT {ref_column} FROM {ref_owner}.{ref_table} WHERE {ref_column} IS NOT NULL AND ROWNUM <= :1"
    
    try:
        cursor.execute(query, [limit])
        results = cursor.fetchall()
        valid_values = [row[0] for row in results]
        return valid_values
    except Exception as e:
        return []

def validate_and_correct_fk_values(connection, foreign_keys, columns, values, operation_type="INSERT"):
    
    if not foreign_keys:
        return values, False, []
    
    corrected_values = values.copy()
    corrections_made = False
    correction_messages = []
    
    column_names = [col[0].upper() for col in columns]
    
    for i, (col_name, col_type, col_len) in enumerate(columns):
        col_name_upper = col_name.upper()
        
        if col_name_upper in foreign_keys and i < len(values):
            fk_info = foreign_keys[col_name_upper]
            current_value = values[i]
            
            if current_value is None or str(current_value).strip() == '':
                continue
                
            is_valid, corrected_value, message = validate_foreign_key_value(
                connection, fk_info, current_value, col_name
            )
            
            if not is_valid and corrected_value is not None:
                corrected_values[i] = corrected_value
                corrections_made = True
                correction_messages.append(f"{operation_type}: {message}")
                print(f"[FK_CORRECTION] {message}")
            elif not is_valid:
                correction_messages.append(f"{operation_type}: FK validation failed for {col_name}")
                print(f"[FK_ERROR] FK validation failed for {col_name}")
    
    return corrected_values, corrections_made, correction_messages

def main():
    gen_final_insert = 'Y'
    try:
        source_connection = oracledb.connect(user=source_user, password=source_password, dsn=source_dsn)
        dest_connection = oracledb.connect(user=dest_user, password=dest_password, dsn=dest_dsn)

        source_service_name = get_oracle_service_name(source_connection)
        dest_service_name = get_oracle_service_name(dest_connection)

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
            
        foreign_keys = get_foreign_key_constraints(dest_connection, dest_schema, dest_table)
        
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
                    print(f"[INFO] The following target columns were not selected from source: {target_only}.")
                    print(f"[INFO] These columns will be intelligently populated considering FK and UNIQUE constraints.")
                else:
                    missing_in_source = []
                
            else:
                common_cols = [col for col in tgt_col_names if col in src_col_names]
                missing_in_source = [col for col in tgt_col_names if col not in src_col_names]
                if missing_in_source:
                    print(f"[INFO] The following columns are present in target but missing in source: {missing_in_source}.")
                    print(f"[INFO] These columns will be intelligently populated considering FK and UNIQUE constraints.")
        columns = tgt_columns
        key_cols = tgt_key_cols
        always_identity_cols = tgt_always_identity_cols
        seq_cols = tgt_seq_cols
        col_names = [col for col, _, _ in columns]
        
        smart_column_data = {}
        if missing_in_source:
            smart_column_data = prepare_smart_column_data(dest_connection, dest_schema, dest_table, missing_in_source, columns)
        
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
            
            cursor = source_connection.cursor()
            
            if custom_query_limit is None:
                limited_query = user_source_query
            else:
                limited_query = f"""
SELECT * FROM (
{user_source_query}
) WHERE ROWNUM <= {custom_query_limit}"""
            
            cursor.execute(limited_query)
            temp_rows = cursor.fetchall()
            total_query_records = len(temp_rows)
            
            print(f"Custom query returned {total_query_records} records")
            
            update_configs = []
            want_updates = input(f"\n[Custom Query Updates] Do you want to create UPDATE statements? (Y/N): ").strip().upper()
            
            if want_updates == 'Y' and total_query_records > 0:
                while True:
                    try:
                        num_update_records = int(input(f"Enter the number of records to update for group 'CUSTOM_DATA' (1 to {total_query_records}): ").strip())
                        if 1 <= num_update_records <= total_query_records:
                            break
                        else:
                            print(f"Please enter a value between 1 and {total_query_records}.")
                    except ValueError:
                        print("Please enter a valid integer.")
                
                updatable_cols = [col for col, dtype, _ in columns if col not in key_cols and col not in always_identity_cols and col not in seq_cols and dtype.upper() != 'CLOB']
                
                if updatable_cols:
                    update_cols = []
                    update_values = []
                    
                    available_cols = [col for col in updatable_cols]
                    for i in range(num_update_records):
                        print(f"\nUpdate {i+1} of {num_update_records}")
                        
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
                        
                        col_type, col_length = get_column_info(columns, chosen_col)
                        formatted_type = format_datatype_info(col_type, col_length)
                        print(f"Column '{chosen_col}' is of type: {formatted_type}")
                        print(get_input_examples(col_type))
                        
                        while True:
                            val = input(f"Enter a value for column '{chosen_col}' (leave blank to auto-generate): ").strip()
                            
                            is_valid, error_msg = validate_input_for_datatype(val, col_type, col_length)
                            if is_valid:
                                update_values.append(val)
                                break
                            else:
                                print(f"ERROR: {error_msg}")
                                print("Please try again or leave blank for auto-generation.")
                    
                    update_configs.append({
                        'num_records': num_update_records,
                        'update_cols': update_cols,
                        'update_values': update_values
                    })
                else:
                    print("No updatable columns available for custom query updates.")
            
            router_conditions.append({
                'group': 'CUSTOM_DATA',
                'base_where': '',
                'user_where': '',
                'num_records': custom_query_limit if custom_query_limit else total_query_records,
                'order_cols': key_cols.copy(),
                'update_configs': update_configs
            })
        else:
            print("\nCreate Groups with separate control over INSERT and UPDATE records.")
            print("🔹 INSERT Records: Define selection criteria from source table")
            print("🔹 UPDATE Records: Define selection criteria from target/source table")
            
            first_group = True
            while True:
                if not first_group:
                    print("\n" + "="*60)
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
                
                print(f"\n🔹 INSERT RECORD CONFIGURATION for group '{group_name}'")
                print("-" * 50)
                
                where_cond = clean_where_condition(input(f"Enter WHERE condition for INSERT records for group '{group_name}': \n"))
                
                while True:
                    try:
                        group_num_records = int(input(f"Enter number of INSERT records to extract for group '{group_name}': \n").strip())
                        if group_num_records > 0:
                            break
                        else:
                            print("Please enter a positive number.\n")
                    except ValueError:
                        print("Please enter a valid integer.\n")
                
                orderable_cols = [col for col, dtype, _ in src_columns if dtype.upper() != 'CLOB']
                while True:
                    order_col_input = input(f"Enter order by column(s) for INSERT records, comma-separated (choose from {orderable_cols}, or leave blank for default PK order): \n").strip().upper()
                    if not order_col_input:
                        group_order_cols = src_key_cols.copy()
                        print(f"[INFO] Using source primary key columns for ordering: {group_order_cols}\n")
                        break
                    group_order_cols = [c.strip() for c in order_col_input.split(',') if c.strip()]
                    if all(any(c.upper() == col.upper() for col in orderable_cols) for c in group_order_cols) and group_order_cols:
                        break
                    print(f"Invalid column(s). Choose from {orderable_cols}.\n")
                
                print(f"\n🔹 UPDATE RECORD CONFIGURATION for group '{group_name}'")
                print("-" * 50)
                
                want_updates = input(f"Do you want to create UPDATE operations for group '{group_name}'? (Y/N): ").strip().upper()
                
                update_configs = []
                
                if want_updates == 'Y':
                    updatable_cols = [col for col, dtype, _ in columns if col not in key_cols and col not in always_identity_cols and col not in seq_cols and dtype.upper() != 'CLOB']
                    if not updatable_cols:
                        print("No updatable columns available (all are PK, identity, sequence, or CLOB columns). Skipping updates for this group.\n")
                    else:
                        
                        update_where_condition = clean_where_condition(input(f"\nPlease enter the \"Update\" records selection criteria (WHERE condition): \n"))
                        
                        while True:
                            try:
                                num_update_records = int(input(f"Enter the number of records you want to update: \n").strip())
                                if num_update_records > 0:
                                    break
                                else:
                                    print("Please enter a positive number.\n")
                            except ValueError:
                                print("Please enter a valid integer.\n")
                        
                        update_cols = []
                        update_values = []
                        
                        available_cols = [col for col in updatable_cols]
                        for i in range(num_update_records):
                            print(f"\n📝 UPDATE Record {i+1} of {num_update_records}")
                            
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
                            
                            col_type, col_length = get_column_info(columns, chosen_col)
                            formatted_type = format_datatype_info(col_type, col_length)
                            print(f"Column '{chosen_col}' is of type: {formatted_type}")
                            print(get_input_examples(col_type))
                            
                            while True:
                                val = input(f"Enter a value for column '{chosen_col}' (leave blank to auto-generate): ").strip()
                                
                                is_valid, error_msg = validate_input_for_datatype(val, col_type, col_length)
                                if is_valid:
                                    update_values.append(val)
                                    break
                                else:
                                    print(f"ERROR: {error_msg}")
                                    print("Please try again or leave blank for auto-generation.")
                        
                        update_configs.append({
                            'num_records': num_update_records,
                            'where_condition': update_where_condition,
                            'target_first': True,  
                            'update_cols': update_cols,
                            'update_values': update_values
                        })
                
                router_conditions.append({
                    'group': group_name,
                    'base_where': where_cond,
                    'user_where': '',
                    'num_records': group_num_records,
                    'order_cols': group_order_cols,
                    'update_configs': update_configs
                })
        
        backup_table = f"{dest_table}_adtf_bkp"
        insert_cols = ', '.join([col for col, _, _ in columns])
        pk_cols = key_cols
        backup_ids = []
        backup_id_set = set()
        backup_id_map = {}
        group_rows_map = {}
        used_pk_values = set()
        
        all_processed_pk_values = set()
        
        for group in router_conditions:
            cursor = source_connection.cursor()
            
            if use_custom_query == 'Y':
                
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
                
                if not rows and use_custom_query == 'Y':
                    print(f"[FALLBACK] Custom query returned no records for group '{group['group']}'")
                    requested_records = custom_query_limit if custom_query_limit else group.get('num_records', 5)
                    print(f"[FALLBACK] Generating {requested_records} mock records using faker library")
                    
                    if not common_cols:
                        tgt_col_names = [col for col, _, _ in tgt_columns]
                        common_cols = tgt_col_names.copy()
                        missing_in_source = []
                        print(f"[FALLBACK] Using all target columns for mock data: {common_cols}")
                    
                    mock_records = generate_mock_records(
                        connection=dest_connection,
                        dest_schema=dest_schema,
                        dest_table=dest_table,
                        columns=columns,
                        pk_cols=pk_cols,
                        common_cols=common_cols,
                        missing_in_source=missing_in_source,
                        smart_column_data=smart_column_data,
                        num_records=requested_records,
                        used_pk_values=used_pk_values.copy(),
                        group_name=f"{group['group']}_CUSTOM_FALLBACK",
                        foreign_keys=foreign_keys
                    )
                    
                    rows = []
                    for mock_record in mock_records:
                        if common_cols and len(common_cols) < len(mock_record):
                            common_row = []
                            for common_col in common_cols:
                                col_idx = [col for col, _, _ in columns].index(common_col)
                                if col_idx < len(mock_record):
                                    common_row.append(mock_record[col_idx])
                            rows.append(tuple(common_row))
                        else:
                            rows.append(mock_record)
                    
                
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
                        print(f"[INFO] Target columns missing from query (will use smart data): {missing_in_source}")
                
                group_rows_map[group['group']] = rows
                
                
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
                base_where = clean_where_condition(group['base_where'])
                user_where = clean_where_condition(group['user_where'])
                
                where_clauses = []
                if base_where:
                    where_clauses.append(f"({base_where})")
                if user_where:
                    where_clauses.append(f"({user_where})")
                
                exclusion_clause = build_pk_exclusion_clause(used_pk_values, pk_cols)
                if exclusion_clause:
                    where_clauses.append(exclusion_clause)
                
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
                
                if not rows:
                    print(f"[FALLBACK] No records found for group '{group['group']}' with criteria: {where_str}")
                    print(f"[FALLBACK] Generating {group['num_records']} mock records using faker library")
                    
                    mock_records = generate_mock_records(
                        connection=dest_connection,
                        dest_schema=dest_schema,
                        dest_table=dest_table,
                        columns=columns,
                        pk_cols=pk_cols,
                        common_cols=common_cols,
                        missing_in_source=missing_in_source,
                        smart_column_data=smart_column_data,
                        num_records=group['num_records'],
                        used_pk_values=used_pk_values.copy(),
                        group_name=group['group'],
                        foreign_keys=foreign_keys
                    )
                    
                    rows = []
                    for mock_record in mock_records:
                        common_row = []
                        for common_col in common_cols:
                            col_idx = [col for col, _, _ in columns].index(common_col)
                            if col_idx < len(mock_record):
                                common_row.append(mock_record[col_idx])
                            else:
                                col_type, col_length = get_column_info(columns, common_col)
                                fake_val, _ = generate_fake_data(col_type, col_length)
                                common_row.append(fake_val)
                        rows.append(tuple(common_row))
                    
                
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
                
                if 'update_configs' in group and group['update_configs']:
                    for update_config in group['update_configs']:
                        update_name = f"{group['group']}_UPDATE_{update_config.get('name', 'DEFAULT')}"
                        
                        update_where_condition = clean_where_condition(update_config.get('where_condition', ''))
                        
                        if not update_where_condition:
                            print(f"[WARNING] No UPDATE criteria specified for '{update_name}'. Using primary key fallback.")
                            pk_fallback_conditions = []
                            for pk_col in pk_cols:
                                if pk_col in common_cols:
                                    pk_fallback_conditions.append(f"{pk_col} IS NOT NULL")
                            update_where_condition = ' AND '.join(pk_fallback_conditions) if pk_fallback_conditions else "1=1"
                            print(f"[INFO] Generated fallback UPDATE condition: {update_where_condition}")
                        
                        batch_size = update_config.get('num_records', 100)
                        
                        exclusion_clause = build_pk_exclusion_clause(used_pk_values, pk_cols)
                        
                        if used_pk_values:
                            print()
                        
                        target_where_clauses = [f"({update_where_condition})"]
                        if exclusion_clause:
                            target_where_clauses.append(exclusion_clause)
                        else:
                            print(f"[DEDUP] No exclusion needed - no records processed yet")
                        
                        target_where_str = ' WHERE ' + ' AND '.join(target_where_clauses)
                        
                        target_select_cols = ', '.join([col for col, _, _ in columns])
                        target_query = f"SELECT {target_select_cols} FROM {dest_schema}.{dest_table}{target_where_str} ORDER BY {', '.join(pk_cols)} FETCH FIRST {batch_size} ROWS ONLY"
                        
                        
                        target_cursor = dest_connection.cursor()
                        target_cursor.execute(target_query)
                        target_rows = target_cursor.fetchall()
                        target_cursor.close()
                        
                        
                        valid_update_records = []
                        skipped_due_to_reuse = []
                        
                        for idx, target_row in enumerate(target_rows):
                            pk_tuple_values = []
                            for pk_col in pk_cols:
                                pk_idx = next((i for i, (col, _, _) in enumerate(columns) if col == pk_col), None)
                                if pk_idx is not None and pk_idx < len(target_row):
                                    pk_tuple_values.append(target_row[pk_idx])
                                else:
                                    pk_tuple_values.append(None)
                            
                            pk_tuple = tuple(pk_tuple_values)
                            
                            if pk_tuple in used_pk_values:
                                skipped_due_to_reuse.append(pk_tuple)
                                print(f"[ERROR] Target record {pk_tuple} found by UPDATE query but already in used_pk_values!")
                                print(f"[ERROR] This indicates the exclusion clause failed - this should not happen")
                            else:
                                valid_update_records.append(target_row)
                        
                        if skipped_due_to_reuse:
                            print(f"[CRITICAL] {len(skipped_due_to_reuse)} records were found by UPDATE query but already processed!")
                            print(f"[CRITICAL] This indicates a bug in the exclusion logic: {skipped_due_to_reuse}")
                            print(f"[CRITICAL] Proceeding with {len(valid_update_records)} valid records instead of {len(target_rows)}")
                            target_rows = valid_update_records
                        
                        final_update_rows = []
                        final_insert_rows = []
                        group_update_backup_ids = []
                        group_insert_backup_ids = []
                        
                        if target_rows:
                            
                            for target_row in target_rows:
                                pk_tuple_values = []
                                for pk_col in pk_cols:
                                    pk_idx = next((i for i, (col, _, _) in enumerate(columns) if col == pk_col), None)
                                    if pk_idx is not None and pk_idx < len(target_row):
                                        pk_tuple_values.append(target_row[pk_idx])
                                    else:
                                        pk_tuple_values.append(None)
                                
                                pk_tuple = tuple(pk_tuple_values)
                                
                                if pk_tuple not in used_pk_values and None not in pk_tuple:
                                    backup_ids.append(pk_tuple)
                                    backup_id_set.add(pk_tuple)
                                    used_pk_values.add(pk_tuple)  
                                    group_update_backup_ids.append(pk_tuple)
                                    final_update_rows.append(target_row)
                                elif pk_tuple in used_pk_values:
                                    print(f"[SKIP] UPDATE record {pk_tuple} already in used_pk_values - this should not happen due to exclusion clause!")
                                elif None in pk_tuple:
                                    print(f"[SKIP] UPDATE record {pk_tuple} has NULL primary key values - invalid record")
                            
                        
                        records_found = len(final_update_rows)
                        records_needed = batch_size - records_found
                        
                        if records_needed > 0:
                            print(f"[STEP 3] MERGE-SYNC: Need {records_needed} more records to reach batch size")
                            print(f"[INFO] Going back to SOURCE table to find additional records for INSERT")
                            
                            all_exclusion_keys = set(used_pk_values)
                            
                            target_pk_query = f"SELECT {', '.join(pk_cols)} FROM {dest_schema}.{dest_table}"
                            if update_where_condition and update_where_condition != "1=1":
                                target_pk_query += f" WHERE {update_where_condition}"
                            
                            print(f"[QUERY] Getting all existing TARGET primary keys to exclude from source query:")
                            print(f"[QUERY] {target_pk_query}")
                            
                            target_pk_cursor = dest_connection.cursor()
                            target_pk_cursor.execute(target_pk_query)
                            existing_target_pks = target_pk_cursor.fetchall()
                            target_pk_cursor.close()
                            
                            for pk_row in existing_target_pks:
                                all_exclusion_keys.add(tuple(pk_row))
                            
                            
                            source_exclusion_clause = build_pk_exclusion_clause(all_exclusion_keys, pk_cols)
                            source_where_clauses = [f"({update_where_condition})"]
                            if source_exclusion_clause:
                                source_where_clauses.append(source_exclusion_clause)
                            
                            source_where_str = ' WHERE ' + ' AND '.join(source_where_clauses)
                            source_order_str = f" ORDER BY {', '.join(group['order_cols'])} DESC FETCH FIRST {records_needed} ROWS ONLY"
                            
                            source_query = f"SELECT {select_cols} FROM {source_schema}.{source_table}{source_where_str}{source_order_str}"
                            
                            print(f"[QUERY] Finding additional SOURCE records for INSERT operations:")
                            print(f"[QUERY] {source_query}")
                            
                            cursor.execute(source_query)
                            source_rows = cursor.fetchall()
                            
                            print(f"[RESULT] Found {len(source_rows)} additional records in source for INSERT")
                            
                            if source_rows:
                                print(f"[STEP 4] Processing {len(source_rows)} SOURCE records as INSERT operations")
                                
                                for row in source_rows:
                                    pk_tuple = tuple(row[common_cols.index(pk)] for pk in pk_cols if pk in common_cols)
                                    if pk_tuple not in backup_id_set and None not in pk_tuple:
                                        backup_ids.append(pk_tuple)
                                        backup_id_set.add(pk_tuple)
                                        used_pk_values.add(pk_tuple)  
                                        print(f"[TRACK] Added MERGE INSERT record to global used_pk_values: {pk_tuple} (group: {group['group']})")
                                        group_insert_backup_ids.append(pk_tuple)
                                        final_insert_rows.append(row)
                                
                                print(f"[INFO] Added {len(final_insert_rows)} SOURCE records for INSERT operations")
                        
                        total_records = len(final_update_rows) + len(final_insert_rows)
                        
                        update_config['processed_pk_values'] = set()
                        
                        for source_row in final_insert_rows:
                            pk_tuple = tuple(source_row[common_cols.index(pk)] for pk in pk_cols if pk in common_cols)
                            update_config['processed_pk_values'].add(pk_tuple)
                        
                        
                        
                        group_rows_map[update_name] = final_update_rows
                        backup_id_map[update_name] = group_update_backup_ids
                        
                        insert_name = f"{update_name}_MERGE_INSERT"
                        group_rows_map[insert_name] = final_insert_rows
                        backup_id_map[insert_name] = group_insert_backup_ids
                        
                        
                        if len(final_update_rows) == 0 and len(final_insert_rows) == 0:
                            print(f"[WARNING] No records found for UPDATE configuration '{update_name}'")
                            
                            num_mock_records = update_config.get('num_records', 1)
                            
                            if 'update_cols' not in update_config or not update_config['update_cols']:
                                updatable_cols = [col for col, dtype, _ in columns if col not in pk_cols and col not in always_identity_cols and col not in seq_cols and dtype.upper() != 'CLOB']
                                mock_update_cols, mock_update_values = generate_mock_update_data(
                                    columns=columns,
                                    updatable_cols=updatable_cols,
                                    num_updates=min(num_mock_records, len(updatable_cols)),
                                    group_name=group['group']
                                )
                                update_config['update_cols'] = mock_update_cols
                                update_config['update_values'] = mock_update_values
                            
                            mock_records = generate_mock_records(
                                connection=dest_connection,
                                dest_schema=dest_schema,
                                dest_table=dest_table,
                                columns=columns,
                                pk_cols=pk_cols,
                                common_cols=common_cols,
                                missing_in_source=missing_in_source,
                                smart_column_data=smart_column_data,
                                num_records=num_mock_records,
                                used_pk_values=used_pk_values.copy(),
                                group_name=f"{group['group']}_UPDATE_FALLBACK",
                                foreign_keys=foreign_keys
                            )
                            
                            mock_insert_rows = []
                            mock_insert_backup_ids = []
                            
                            for mock_record in mock_records:
                                common_row = []
                                for common_col in common_cols:
                                    col_idx = [col for col, _, _ in columns].index(common_col)
                                    if col_idx < len(mock_record):
                                        common_row.append(mock_record[col_idx])
                                    else:
                                        col_type, col_length = get_column_info(columns, common_col)
                                        fake_val, _ = generate_fake_data(col_type, col_length)
                                        common_row.append(fake_val)
                                
                                mock_insert_rows.append(tuple(common_row))
                                
                                pk_tuple = tuple(common_row[common_cols.index(pk)] for pk in pk_cols if pk in common_cols)
                                if pk_tuple not in backup_id_set and None not in pk_tuple:
                                    backup_ids.append(pk_tuple)
                                    backup_id_set.add(pk_tuple)
                                    used_pk_values.add(pk_tuple)
                                    mock_insert_backup_ids.append(pk_tuple)
                                    print(f"[TRACK] Added MOCK MERGE INSERT record to global used_pk_values: {pk_tuple} (group: {group['group']})")
                            
                            insert_name = f"{update_name}_MERGE_INSERT"
                            group_rows_map[insert_name] = mock_insert_rows
                            backup_id_map[insert_name] = mock_insert_backup_ids
                            
                            update_config['processed_pk_values'] = set()
                            for mock_row in mock_insert_rows:
                                pk_tuple = tuple(mock_row[common_cols.index(pk)] for pk in pk_cols if pk in common_cols)
                                update_config['processed_pk_values'].add(pk_tuple)
                            
                            print(f"[FALLBACK] Generated {len(mock_insert_rows)} mock MERGE INSERT records for '{insert_name}'")
                            print(f"[FALLBACK] Mock UPDATE operations will be applied after INSERT: {update_config.get('update_cols', [])}")
                            
                            group_rows_map[update_name] = []
                            backup_id_map[update_name] = []
        
        
        has_updates = any('update_configs' in group and group['update_configs'] for group in router_conditions)
        
        all_rows = []
        custom_update_statements = []
        
        if use_custom_query == 'Y':
            for group in router_conditions:
                all_rows.extend(group_rows_map[group['group']])
                
                if 'update_configs' in group and group['update_configs']:
                    for update_config in group['update_configs']:
                        num_records = update_config.get('num_records', 0)
                        update_cols = update_config.get('update_cols', [])
                        update_values = update_config.get('update_values', [])
                        
                        for i in range(min(num_records, len(update_cols), len(all_rows))):
                            if i < len(update_cols) and i < len(update_values):
                                custom_update_statements.append({
                                    'row_index': i,
                                    'column': update_cols[i],
                                    'value': update_values[i]
                                })
        
        from collections import OrderedDict
        backup_ids_dict = OrderedDict()
        
        for group in router_conditions:
            rows = group_rows_map[group['group']]
            for row in rows:
                if use_custom_query == 'Y' or selected_source_columns:
                    available_cols = common_cols if 'common_cols' in locals() and common_cols else [col for col, _, _ in columns]

                else:
                    available_cols = [col for col, _, _ in columns]
                
                pk_values = []
                for pk in pk_cols:
                    if pk in available_cols:
                        pk_idx = available_cols.index(pk)
                        if pk_idx < len(row):
                            pk_values.append(row[pk_idx])
                        else:
                            pk_values.append(None)
                    else:
                        print(f"[WARNING] Primary key column '{pk}' not available in source data")
                        pk_values.append(None)
                
                pk_tuple = tuple(pk_values)
                if pk_tuple not in backup_ids_dict:
                    backup_ids_dict[pk_tuple] = True
            
            if 'update_configs' in group and group['update_configs']:
                for update_config in group['update_configs']:
                    update_name = f"{group['group']}_UPDATE_{update_config.get('name', 'DEFAULT')}"
                    if update_name in group_rows_map:
                        update_rows = group_rows_map[update_name]
                        
                        for row in update_rows:
                            pk_values = []
                            for pk in pk_cols:
                                if pk in available_cols:
                                    pk_idx = available_cols.index(pk)
                                    if pk_idx < len(row):
                                        pk_values.append(row[pk_idx])
                                    else:
                                        pk_values.append(None)
                                else:
                                    print(f"[WARNING] Primary key column '{pk}' not available in UPDATE record")
                                    pk_values.append(None)
                            
                            pk_tuple = tuple(pk_values)
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
            conditions = []
            for col, val in zip(pk_cols, pk_tuple):
                if val is None:
                    conditions.append(f"{col} IS NULL")
                elif isinstance(val, str):
                    escaped_val = val.replace("'", "''")
                    conditions.append(f"{col} = '{escaped_val}'")
                else:
                    conditions.append(f"{col} = {val}")
            return ' AND '.join(conditions)
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
        
        insert_record_count = sum(len(group_rows_map[group['group']]) for group in router_conditions)
        update_record_count = 0
        for group in router_conditions:
            if 'update_configs' in group and group['update_configs']:
                for update_config in group['update_configs']:
                    update_name = f"{group['group']}_UPDATE_{update_config.get('name', 'DEFAULT')}"
                    if update_name in group_rows_map:
                        update_record_count += len(group_rows_map[update_name])
        
        total_backup_records = len(backup_ids)
        
        
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
                    
                    #print(f"[DEBUG] Processing custom query INSERT record {row_idx + 1}")
                    
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
                                insert_values.append(format_date_for_sql_insert(val, 'DATE'))
                            elif col_type.upper().startswith('TIMESTAMP'):
                                insert_values.append(format_date_for_sql_insert(val, 'TIMESTAMP'))
                            elif isinstance(val, str):
                                if col_len and isinstance(col_len, int):
                                    val = val[:col_len]
                                escaped_val = val.replace("'", "''")
                                insert_values.append(f"'{escaped_val}'")
                            else:
                                insert_values.append(str(val))
                        else:
                            smart_val = get_smart_column_value(smart_column_data, col_name, col_type, col_len, row_idx)
                            fake_val, val_type = (smart_val, 'STRING') if isinstance(smart_val, str) else (smart_val, type(smart_val).__name__.upper())
                            
                            if col_type.upper() in ['VARCHAR2', 'CHAR', 'NVARCHAR2']:
                                val_type = 'STRING'
                            elif col_type.upper() == 'NUMBER':
                                val_type = 'NUMBER'
                            elif col_type.upper() == 'DATE':
                                val_type = 'DATE'
                            elif col_type.upper().startswith('TIMESTAMP'):
                                val_type = 'TIMESTAMP'
                            
                            if val_type == 'STRING':
                                escaped_fake = str(fake_val).replace("'", "''")
                                insert_values.append(f"'{escaped_fake}'")
                            elif val_type == 'DATE':
                                insert_values.append(f"TO_DATE('{fake_val}', 'DD-Mon-YYYY')")
                            elif val_type == 'TIMESTAMP':
                                insert_values.append(f"TO_DATE('{fake_val}', 'DD-Mon-YYYY')")
                            else:
                                insert_values.append(str(fake_val))
                    
                    raw_values = []
                    for i, (col_name, col_type, col_len) in enumerate(columns):
                        if i < len(insert_values):
                            sql_value = insert_values[i]
                            if sql_value == 'NULL':
                                raw_values.append(None)
                            elif sql_value.startswith("'") and sql_value.endswith("'"):
                                raw_values.append(sql_value[1:-1].replace("''", "'"))
                            elif sql_value.startswith("TO_DATE("):
                                import re
                                date_match = re.search(r"TO_DATE\('([^']+)'", sql_value)
                                raw_values.append(date_match.group(1) if date_match else None)
                            else:
                                raw_values.append(sql_value)
                        else:
                            raw_values.append(None)
                    
                    corrected_values, fk_corrections_made, fk_messages = validate_and_correct_fk_values(
                        dest_connection, foreign_keys, columns, raw_values, "INSERT"
                    )
                    
                    if fk_corrections_made:
                        insert_values = []
                        
                        for i, (col_name, col_type, col_len) in enumerate(columns):
                            if i < len(corrected_values):
                                val = corrected_values[i]
                                
                                if val is None:
                                    insert_values.append('NULL')
                                elif col_type.upper() == 'DATE':
                                    insert_values.append(format_date_for_sql_insert(val, 'DATE'))
                                elif col_type.upper().startswith('TIMESTAMP'):
                                    insert_values.append(format_date_for_sql_insert(val, 'TIMESTAMP'))
                                elif isinstance(val, str):
                                    if col_len and isinstance(col_len, int):
                                        val = val[:col_len]
                                    escaped_val = val.replace("'", "''")
                                    insert_values.append(f"'{escaped_val}'")
                                else:
                                    insert_values.append(str(val))
                            else:
                                insert_values.append('NULL')
                        
                        for msg in fk_messages:
                            yaml_data['sql'].append({
                                'description': f'FK Correction for INSERT record {row_idx + 1}: {msg}',
                                'sql': f"-- {msg}",
                                'dml': 'Yes',
                                'schema': dest_schema,
                                'db': dest_service_name,
                                'dbtype': 'oracle',
                                'connection_name': dest_connection_name
                            })
                    
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
                    
                    pk_values = []
                    for pk in pk_cols:
                        if pk in common_cols:
                            idx = common_cols.index(pk)
                            if idx < len(row):
                                pk_values.append(row[idx])
                            else:
                                pk_values.append(None)
                        else:
                            pk_values.append(None)
                    
                    if pk_values:
                        pk_tuple = tuple(pk_values)
                        all_processed_pk_values.add(pk_tuple)
                
                if custom_update_statements:
                    for update_spec in custom_update_statements:
                        row_idx = update_spec['row_index']
                        if row_idx < len(all_rows):
                            row = all_rows[row_idx]
                            update_col = update_spec['column']
                            update_val = update_spec['value']
                            
                            result = generate_update_statement(
                                row, update_col, update_val, columns, key_cols, common_cols,
                                dest_schema, dest_table, dest_connection_name, dest_service_name, f'record {row_idx + 1}',
                                dest_connection, foreign_keys
                            )
                            
                            if result and all(stmt is not None for stmt in result):
                                if len(result) == 4:
                                    fk_comment_statement, pre_count_statement, update_statement, post_count_statement = result
                                    yaml_data['sql'].append(fk_comment_statement)
                                else:
                                    pre_count_statement, update_statement, post_count_statement = result
                                    
                                yaml_data['sql'].append(pre_count_statement)
                                yaml_data['sql'].append(update_statement)
                                yaml_data['sql'].append(post_count_statement)
                                
                                pk_values = []
                                for pk in pk_cols:
                                    if pk in common_cols:
                                        idx = common_cols.index(pk)
                                        if idx < len(row):
                                            pk_values.append(row[idx])
                                        else:
                                            pk_values.append(None)
                                    else:
                                        pk_values.append(None)
                                
                                if pk_values:
                                    pk_tuple = tuple(pk_values)
                                    all_processed_pk_values.add(pk_tuple)
            
        else:
            for group in router_conditions:
                rows = group_rows_map[group['group']]
                
                all_delete_rows = []
                all_delete_rows.extend(rows)  
                
                update_records_count = 0
                if 'update_configs' in group and group['update_configs']:
                    for update_config in group['update_configs']:
                        update_name = f"{group['group']}_UPDATE_{update_config.get('name', 'DEFAULT')}"
                        if update_name in group_rows_map:
                            update_rows = group_rows_map[update_name]
                            update_records_count += len(update_rows)
                
                
                if not all_delete_rows:
                    print(f"[INFO] No data found for group {group['group']}.")
                else:
                    group_delete_conditions = []
                    for row_idx, row in enumerate(all_delete_rows):
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
                            'description': f'Delete {len(all_delete_rows)} records from group {group["group"]} in {dest_schema}.{dest_table}',
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
                                    insert_values.append(format_date_for_sql_insert(val, 'DATE'))
                                elif col_type.upper().startswith('TIMESTAMP'):
                                    insert_values.append(format_date_for_sql_insert(val, 'TIMESTAMP'))
                                elif isinstance(val, str):
                                    if col_len and isinstance(col_len, int):
                                        val = val[:col_len]
                                    escaped_val = val.replace("'", "''")
                                    insert_values.append(f"'{escaped_val}'")
                                else:
                                    insert_values.append(str(val))
                            else:
                                smart_val = get_smart_column_value(smart_column_data, col_name, col_type, col_len, row_idx)
                                fake_val, val_type = (smart_val, 'STRING') if isinstance(smart_val, str) else (smart_val, type(smart_val).__name__.upper())
                                
                                if col_type.upper() in ['VARCHAR2', 'CHAR', 'NVARCHAR2']:
                                    val_type = 'STRING'
                                elif col_type.upper() == 'NUMBER':
                                    val_type = 'NUMBER'
                                elif col_type.upper() == 'DATE':
                                    val_type = 'DATE'
                                elif col_type.upper().startswith('TIMESTAMP'):
                                    val_type = 'TIMESTAMP'
                                
                                if val_type == 'STRING':
                                    escaped_fake = str(fake_val).replace("'", "''")
                                    insert_values.append(f"'{escaped_fake}'")
                                elif val_type == 'DATE':
                                    insert_values.append(f"TO_DATE('{fake_val}', 'DD-Mon-YYYY')")
                                elif val_type == 'TIMESTAMP':
                                    insert_values.append(f"TO_DATE('{fake_val}', 'DD-Mon-YYYY')")
                                else:
                                    insert_values.append(str(fake_val))
                    
                    raw_values = []
                    for i, (col_name, col_type, col_len) in enumerate(columns):
                        if i < len(insert_values):
                            sql_value = insert_values[i]
                            if sql_value == 'NULL':
                                raw_values.append(None)
                            elif sql_value.startswith("'") and sql_value.endswith("'"):
                                raw_values.append(sql_value[1:-1].replace("''", "'"))
                            elif sql_value.startswith("TO_DATE("):
                                import re
                                date_match = re.search(r"TO_DATE\('([^']+)'", sql_value)
                                raw_values.append(date_match.group(1) if date_match else None)
                            else:
                                raw_values.append(sql_value)
                        else:
                            raw_values.append(None)
                    
                    corrected_values, fk_corrections_made, fk_messages = validate_and_correct_fk_values(
                        dest_connection, foreign_keys, columns, raw_values, "INSERT"
                    )
                    
                    if fk_corrections_made:
                        print(f"[FK_VALIDATION] Applying FK corrections for INSERT record {row_idx + 1} from group {group['group']}")
                        insert_values = []
                        
                        for i, (col_name, col_type, col_len) in enumerate(columns):
                            if i < len(corrected_values):
                                val = corrected_values[i]
                                
                                if val is None:
                                    insert_values.append('NULL')
                                elif col_type.upper() == 'DATE':
                                    insert_values.append(format_date_for_sql_insert(val, 'DATE'))
                                elif col_type.upper().startswith('TIMESTAMP'):
                                    insert_values.append(format_date_for_sql_insert(val, 'TIMESTAMP'))
                                elif isinstance(val, str):
                                    if col_len and isinstance(col_len, int):
                                        val = val[:col_len]
                                    escaped_val = val.replace("'", "''")
                                    insert_values.append(f"'{escaped_val}'")
                                else:
                                    insert_values.append(str(val))
                            else:
                                insert_values.append('NULL')
                        
                        for msg in fk_messages:
                            yaml_data['sql'].append({
                                'description': f'FK Correction for INSERT record {row_idx + 1} from group {group["group"]}: {msg}',
                                'sql': f"-- {msg}",
                                'dml': 'Yes',
                                'schema': dest_schema,
                                'db': dest_service_name,
                                'dbtype': 'oracle',
                                'connection_name': dest_connection_name
                            })
                    
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
                    
                    pk_values = []
                    for pk in pk_cols:
                        if pk in common_cols:
                            idx = common_cols.index(pk)
                            if idx < len(row):
                                pk_values.append(row[idx])
                            else:
                                pk_values.append(None)
                        else:
                            pk_values.append(None)
                    
                    if pk_values:
                        pk_tuple = tuple(pk_values)
                        all_processed_pk_values.add(pk_tuple)
                
                
                if 'update_configs' in group and group['update_configs']:
                    for update_config in group['update_configs']:
                        update_name = f"{group['group']}_UPDATE_{update_config.get('name', 'DEFAULT')}"
                        merge_insert_name = f"{update_name}_MERGE_INSERT"
                        
                        if merge_insert_name in group_rows_map and group_rows_map[merge_insert_name]:
                            merge_insert_rows = group_rows_map[merge_insert_name]
                            print(f"[INFO] Processing {len(merge_insert_rows)} MERGE INSERT records from '{merge_insert_name}' in group {group['group']}")
                            print(f"[INFO] These are source records not found in target - generating INSERT statements")
                            
                            for row_idx, row in enumerate(merge_insert_rows):
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
                                    else:
                                        val = None
                                    
                                    if val is None:
                                        smart_val = get_smart_column_value(smart_column_data, col_name, col_type, col_len, row_idx)
                                        fake_val, val_type = (smart_val, 'STRING') if isinstance(smart_val, str) else (smart_val, type(smart_val).__name__.upper())
                                        
                                        if col_type.upper() in ['VARCHAR2', 'CHAR', 'NVARCHAR2']:
                                            val_type = 'STRING'
                                        elif col_type.upper() == 'NUMBER':
                                            val_type = 'NUMBER'
                                        elif col_type.upper() == 'DATE':
                                            val_type = 'DATE'
                                        elif col_type.upper().startswith('TIMESTAMP'):
                                            val_type = 'TIMESTAMP'
                                        
                                        if val_type == 'STRING':
                                            if col_len and isinstance(col_len, int):
                                                fake_val = str(fake_val)[:col_len]
                                            escaped_val = str(fake_val).replace("'", "''")
                                            insert_values.append(f"'{escaped_val}'")
                                        elif val_type == 'DATE':
                                            insert_values.append(f"TO_DATE('{fake_val}', 'DD-Mon-YYYY')")
                                        elif val_type == 'TIMESTAMP':
                                            insert_values.append(f"TO_DATE('{fake_val}', 'DD-Mon-YYYY')")
                                        else:
                                            insert_values.append(str(fake_val))
                                    elif col_type.upper() == 'DATE':
                                        insert_values.append(format_date_for_sql_insert(val, 'DATE'))
                                    elif col_type.upper().startswith('TIMESTAMP'):
                                        insert_values.append(format_date_for_sql_insert(val, 'TIMESTAMP'))
                                    elif isinstance(val, str):
                                        if col_len and isinstance(col_len, int):
                                            val = val[:col_len]
                                        escaped_val = val.replace("'", "''")
                                        insert_values.append(f"'{escaped_val}'")
                                    else:
                                        insert_values.append(str(val))
                                
                                values_clause = ', '.join(insert_values)
                                columns_clause = ', '.join(insert_columns)
                                
                                fk_correction_messages = []
                                if foreign_keys and dest_connection:
                                    try:
                                        raw_values = []
                                        for i, val_str in enumerate(insert_values):
                                            if val_str == 'NULL':
                                                raw_values.append(None)
                                            elif val_str.startswith("'") and val_str.endswith("'"):
                                                raw_values.append(val_str[1:-1].replace("''", "'"))
                                            elif val_str.startswith("TO_DATE("):
                                                import re
                                                date_match = re.search(r"TO_DATE\('([^']+)'", val_str)
                                                if date_match:
                                                    raw_values.append(date_match.group(1))
                                                else:
                                                    raw_values.append(val_str)
                                            else:
                                                try:
                                                    if '.' in val_str:
                                                        raw_values.append(float(val_str))
                                                    else:
                                                        raw_values.append(int(val_str))
                                                except ValueError:
                                                    raw_values.append(val_str)
                                        
                                        corrected_values, correction_messages = validate_and_correct_fk_values(
                                            dest_connection, dest_schema, dest_table, raw_values, foreign_keys
                                        )
                                        
                                        if correction_messages:
                                            fk_correction_messages.extend(correction_messages)
                                            print(f"[FK] Applied {len(correction_messages)} FK corrections to MERGE INSERT record {row_idx + 1}")
                                            
                                            corrected_insert_values = []
                                            for i, (col_name, col_type, col_len) in enumerate(columns):
                                                if i < len(corrected_values):
                                                    val = corrected_values[i]
                                                    if val is None:
                                                        corrected_insert_values.append('NULL')
                                                    elif col_type.upper() == 'DATE':
                                                        corrected_insert_values.append(format_date_for_sql_insert(val, 'DATE'))
                                                    elif col_type.upper().startswith('TIMESTAMP'):
                                                        corrected_insert_values.append(format_date_for_sql_insert(val, 'TIMESTAMP'))
                                                    elif isinstance(val, str):
                                                        if col_len and isinstance(col_len, int):
                                                            val = val[:col_len]
                                                        escaped_val = val.replace("'", "''")
                                                        corrected_insert_values.append(f"'{escaped_val}'")
                                                    else:
                                                        corrected_insert_values.append(str(val))
                                                else:
                                                    corrected_insert_values.append('NULL')
                                            
                                            values_clause = ', '.join(corrected_insert_values)
                                            
                                    except Exception as e:
                                        print(f"[WARNING] FK validation failed for MERGE INSERT record {row_idx + 1}: {e}")
                                
                                if fk_correction_messages:
                                    fk_comment_lines = ["# FK Corrections applied:"]
                                    for msg in fk_correction_messages:
                                        fk_comment_lines.append(f"# - {msg}")
                                    yaml_data['sql'].append({
                                        'description': f'FK corrections for MERGE INSERT record {row_idx + 1}',
                                        'sql': '\n'.join(fk_comment_lines),
                                        'dml': 'No',
                                        'schema': dest_schema,
                                        'db': dest_service_name,
                                        'dbtype': 'oracle',
                                        'connection_name': dest_connection_name
                                    })
                                
                                yaml_data['sql'].append({
                                    'description': f'MERGE INSERT: Record {row_idx + 1} from source (not in target) for {merge_insert_name} into {dest_schema}.{dest_table}',
                                    'sql': f"INSERT INTO {dest_schema}.{dest_table} ({columns_clause}) VALUES ({values_clause})",
                                    'dml': 'Yes',
                                    'schema': dest_schema,
                                    'db': dest_service_name,
                                    'dbtype': 'oracle',
                                    'connection_name': dest_connection_name
                                })
                                
                                pk_values = []
                                for pk in pk_cols:
                                    if pk in common_cols:
                                        idx = common_cols.index(pk)
                                        if idx < len(row):
                                            pk_values.append(row[idx])
                                        else:
                                            pk_values.append(None)
                                    else:
                                        pk_values.append(None)
                                
                                if pk_values:
                                    pk_tuple = tuple(pk_values)
                                    all_processed_pk_values.add(pk_tuple)
                                
                                update_cols = update_config.get('update_cols', [])
                                update_values = update_config.get('update_values', [])
                                
                                if update_cols and update_values:
                                    
                                    if '_merge_processed_updates' not in group:
                                        group['_merge_processed_updates'] = set()
                                    if row_idx < len(update_cols) and row_idx < len(update_values):
                                        update_col = update_cols[row_idx]
                                        update_val = update_values[row_idx]
                                        
                                        if update_val is not None and str(update_val).strip():
                                            
                                            pk_tuple = tuple(pk_values) if pk_values else None
                                            merge_update_key = (pk_tuple, update_col) if pk_tuple else None
                                            
                                            if merge_update_key and merge_update_key in group['_merge_processed_updates']:
                                                print(f"[SKIP] MERGE UPDATE operation for record {row_idx + 1}, column '{update_col}' already processed - avoiding duplicate")
                                                continue
                                            
                                            if merge_update_key and merge_update_key in group['_merge_processed_updates']:
                                                print(f"[SKIP] MERGE UPDATE operation for record {row_idx + 1}, column '{update_col}' already processed - avoiding duplicate")
                                            else:
                                                columns_for_update = common_cols  
                                                
                                                result = generate_update_statement(
                                                    row, update_col, update_val, columns, key_cols, common_cols,
                                                    dest_schema, dest_table, dest_connection_name, dest_service_name, 
                                                    f'MERGE UPDATE: Record {row_idx + 1} from {merge_insert_name} (newly inserted) - Column: {update_col}',
                                                    dest_connection, foreign_keys
                                                )
                                                
                                                if result and all(stmt is not None for stmt in result):
                                                    if len(result) == 4:
                                                        fk_comment_statement, pre_count_statement, update_statement, post_count_statement = result
                                                        yaml_data['sql'].append(fk_comment_statement)
                                                    else:
                                                        pre_count_statement, update_statement, post_count_statement = result
                                                
                                                
                                                if result and all(stmt is not None for stmt in result):
                                                    if len(result) == 4:
                                                        fk_comment_statement, pre_count_statement, update_statement, post_count_statement = result
                                                        yaml_data['sql'].append(fk_comment_statement)
                                                    else:
                                                        pre_count_statement, update_statement, post_count_statement = result
                                                
                                                    if pre_count_statement and update_statement and post_count_statement:
                                                        yaml_data['sql'].append(pre_count_statement)
                                                        yaml_data['sql'].append(update_statement)
                                                        yaml_data['sql'].append(post_count_statement) 
                                                    if merge_update_key:
                                                        group['_merge_processed_updates'].add(merge_update_key)
                                                    
                                                    
                                                    
                                                else:
                                                    print(f"[ERROR] Failed to generate MERGE UPDATE statement for record {row_idx + 1}, column '{update_col}' = '{update_val}'")
                                        else:
                                            print(f"[INFO] Skipping UPDATE for MERGE INSERT record {row_idx + 1} - value is null or empty: '{update_val}'")
                                    else:
                                        
                                        print(f"[INFO] No UPDATE assignment for MERGE INSERT record {row_idx + 1} - row index {row_idx} beyond available columns/values (one-to-one mapping)")
                                else:
                                    print()
                                    print(f"[INFO] No user-specified update columns for MERGE INSERT record {row_idx + 1} - no UPDATE statements needed")
                
                if 'update_configs' in group and group['update_configs']:
                    for update_config in group['update_configs']:
                        update_name = f"{group['group']}_UPDATE_{update_config.get('name', 'DEFAULT')}"
                        if update_name in group_rows_map and group_rows_map[update_name]:
                            update_rows = group_rows_map[update_name]
                            
                            records_existing = [(row_idx, row) for row_idx, row in enumerate(update_rows)]
                            
                            if records_existing:
                                update_config['existing_records'] = records_existing
                                print(f"[INFO] {len(records_existing)} records from '{update_name}' exist in target and will be processed for UPDATE statements")
                
                if 'update_configs' in group and group['update_configs']:
                    group_processed_updates = set()
                    
                    if '_merge_processed_updates' in group:
                        group_processed_updates.update(group['_merge_processed_updates'])
                        print(f"[DEDUP] Inherited {len(group['_merge_processed_updates'])} MERGE INSERT updates for deduplication in group '{group['group']}'")
                    
                    for update_config in group['update_configs']:
                        config_name = update_config.get('name', 'DEFAULT')
                        update_name = f"{group['group']}_UPDATE_{config_name}"
                        
                        if update_name in group_rows_map and group_rows_map[update_name]:
                            update_rows = group_rows_map[update_name]
                            update_cols = update_config.get('update_cols', [])
                            update_values = update_config.get('update_values', [])
                            
                            
                            target_columns_list = [col for col, _, _ in columns]
                            is_target_records = len(update_rows[0]) == len(target_columns_list) if update_rows else False
                            
                            if is_target_records:
                                columns_for_update = target_columns_list
                            else:
                                columns_for_update = common_cols
                            
                            update_statements_generated = 0
                            max_col_val_pairs = min(len(update_cols), len(update_values))
                            
                            
                            merge_processed_pks = update_config.get('processed_pk_values', set())
                            if merge_processed_pks:
                                print(f"[DEDUP] Will cross-check against {len(merge_processed_pks)} primary keys processed during MERGE-SYNC")
                            
                            for row_idx in range(min(len(update_rows), max_col_val_pairs)):
                                row = update_rows[row_idx]
                                update_col = update_cols[row_idx]
                                update_val = update_values[row_idx]
                                
                                
                                pk_values = []
                                for pk in pk_cols:
                                    if pk in columns_for_update:
                                        idx = columns_for_update.index(pk)
                                        if idx < len(row):
                                            pk_values.append(row[idx])
                                        else:
                                            pk_values.append(None)
                                    else:
                                        pk_values.append(None)
                                
                                pk_tuple = tuple(pk_values) if pk_values else None
                                
                                update_key = (pk_tuple, update_col) if pk_tuple else None
                                
                                if update_key and update_key in group_processed_updates:
                                    print(f"[SKIP] UPDATE operation for record {row_idx+1}, column '{update_col}' already processed (includes MERGE updates) in group '{group['group']}' - avoiding duplicate")
                                    continue
                                
                                if pk_tuple and pk_tuple in merge_processed_pks:
                                    print(f"[SKIP] UPDATE operation for record {row_idx+1}, column '{update_col}' - primary key {pk_tuple} was already processed during MERGE-SYNC - avoiding cross-phase duplicate")
                                    continue
                                
                                record_description = f'UPDATE record {row_idx + 1} from {update_name} - Column: {update_col}'
                                
                                result = generate_update_statement(
                                    row, update_col, update_val, columns, key_cols, columns_for_update,
                                    dest_schema, dest_table, dest_connection_name, dest_service_name, 
                                    record_description, dest_connection, foreign_keys
                                )
                                
                                if result and all(stmt is not None for stmt in result):
                                    if len(result) == 4:
                                        fk_comment_statement, pre_count_statement, update_statement, post_count_statement = result
                                        yaml_data['sql'].append(fk_comment_statement)
                                    else:
                                        pre_count_statement, update_statement, post_count_statement = result
                                
                                    if pre_count_statement and update_statement and post_count_statement:
                                        yaml_data['sql'].append(pre_count_statement)
                                        yaml_data['sql'].append(update_statement)
                                        yaml_data['sql'].append(post_count_statement)
                                        update_statements_generated += 1
                                    
                                    if update_key:
                                        group_processed_updates.add(update_key)
                                    
                                    
                                    if pk_tuple:
                                        all_processed_pk_values.add(pk_tuple)
                                    
                                else:
                                    print(f"[ERROR] Failed to generate UPDATE statement for record {row_idx + 1}, column '{update_col}' in '{update_name}'")
                            
                            if len(update_rows) != len(update_cols) or len(update_cols) != len(update_values):
                                expected_statements = len(update_rows) * max_col_val_pairs
                                print(f"[INFO] Regular UPDATE processing: {len(update_rows)} rows × {max_col_val_pairs} column-value pairs = {expected_statements} expected operations")
                                print(f"[INFO] Actual operations generated: {update_statements_generated} (some may have been skipped as duplicates)")
                            else:
                                print()
                            
                            merge_updates_count = len(group['_merge_processed_updates']) if '_merge_processed_updates' in group else 0
                            regular_updates_count = len(group_processed_updates) - merge_updates_count
                            total_updates_count = len(group_processed_updates)
                            
                        else:
                            print(f"[INFO] No records available for UPDATE operations in '{update_name}'")
                else:
                    if 'num_update_records' in group and group.get('num_update_records', 0) > 0:
                        print(f"[INFO] Processing legacy UPDATE structure for group {group['group']}")
                        for i in range(group['num_update_records']):
                            if i < len(rows) and i < len(group.get('update_cols', [])):
                                row = rows[i]
                                update_col = group['update_cols'][i]
                                update_val = group.get('update_values', [None])[i] if i < len(group.get('update_values', [])) else None
                                
                                record_exists = check_individual_record_exists(dest_connection, dest_schema, dest_table, key_cols, common_cols, row)
                                if record_exists:
                                    result = generate_update_statement(
                                        row, update_col, update_val, columns, key_cols, common_cols,
                                        dest_schema, dest_table, dest_connection_name, dest_service_name, 
                                        f'legacy record {i + 1} from group {group["group"]}', dest_connection, foreign_keys
                                    )
                                    
                                    if result and all(stmt is not None for stmt in result):
                                        if len(result) == 4:
                                            fk_comment_statement, pre_count_statement, update_statement, post_count_statement = result
                                            yaml_data['sql'].append(fk_comment_statement)
                                        else:
                                            pre_count_statement, update_statement, post_count_statement = result
                                    
                                        if pre_count_statement and update_statement and post_count_statement:
                                            yaml_data['sql'].append(pre_count_statement)
                                            yaml_data['sql'].append(update_statement)
                                            yaml_data['sql'].append(post_count_statement)
                                        
                                        pk_values = []
                                        for pk in pk_cols:
                                            if pk in common_cols:
                                                idx = common_cols.index(pk)
                                                if idx < len(row):
                                                    pk_values.append(row[idx])
                                                else:
                                                    pk_values.append(None)
                                            else:
                                                pk_values.append(None)
                                        
                                        if pk_values:
                                            pk_tuple = tuple(pk_values)
                                            all_processed_pk_values.add(pk_tuple)
                                        
                                else:
                                    print(f"[INFO] Legacy UPDATE record {i + 1} not found in target table - INSERT statement was generated instead")
        
        if all_processed_pk_values:
            
            def pk_where_clause_final(pk_cols, pk_tuple):
                conditions = []
                for col, val in zip(pk_cols, pk_tuple):
                    if val is None:
                        conditions.append(f"{col} IS NULL")
                    else:
                        col_type = None
                        for col_name, column_type, _ in columns:
                            if col_name == col:
                                col_type = column_type
                                break
                        
                        if col_type and (col_type.upper() == 'DATE' or col_type.upper().startswith('TIMESTAMP')):
                            conditions.append(format_date_for_where_clause(val, col))
                        elif isinstance(val, str):
                            escaped_val = val.replace("'", "''")
                            conditions.append(f"{col} = '{escaped_val}'")
                        else:
                            conditions.append(f"{col} = {val}")
                return ' AND '.join(conditions)
            
            delete_conditions = []
            for pk_tuple in all_processed_pk_values:
                condition = pk_where_clause_final(pk_cols, pk_tuple)
                if condition:
                    delete_conditions.append(f"({condition})")
            
            if delete_conditions:
                final_where_clause = ' OR '.join(delete_conditions)
                
                yaml_data['sql'].append({
                    'description': f'Count all processed records (INSERT + UPDATE) to be deleted from {dest_schema}.{dest_table}',
                    'sql': f"SELECT COUNT(1) FROM {dest_schema}.{dest_table} WHERE {final_where_clause}",
                    'dml': 'Yes',
                    'schema': dest_schema,
                    'db': dest_service_name,
                    'dbtype': 'oracle',
                    'connection_name': dest_connection_name
                })
                
                yaml_data['sql'].append({
                    'description': f'FINAL DELETE: Remove all {len(all_processed_pk_values)} processed records (INSERT + UPDATE) from {dest_schema}.{dest_table}',
                    'sql': f"DELETE FROM {dest_schema}.{dest_table} WHERE {final_where_clause}",
                    'dml': 'Yes',
                    'schema': dest_schema,
                    'db': dest_service_name,
                    'dbtype': 'oracle',
                    'connection_name': dest_connection_name
                })
                
            else:
                print(f"[WARNING] No valid primary key conditions found for final DELETE statement")
        else:
            print(f"No processed records found - skipping final DELETE statement")
        
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
        
        print(f"\nAll DML operations have been written to YAML file: {output_filename}")
        print(f"  - Total target table columns: {len([col for col, _, _ in columns])}")
        print(f"  - Columns from source/query: {len(common_cols)} ({common_cols})")
        if missing_in_source:
            print(f"  - Columns populated with smart data: {len(missing_in_source)} ({missing_in_source})")
            if smart_column_data:
                fk_cols = [col for col, data in smart_column_data.items() if data['type'] == 'FK']
                unique_cols = [col for col, data in smart_column_data.items() if data['type'] == 'UNIQUE']
                if fk_cols:
                    print(f"    * FK columns (using parent table data): {fk_cols}")
                if unique_cols:
                    print(f"    * UNIQUE columns (maintaining uniqueness): {unique_cols}")
        else:
            print(f"  - All target columns available from source - no smart data needed")
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

def generate_update_statement(row, update_col, update_val, columns, key_cols, common_cols, dest_schema, dest_table, dest_connection_name, dest_service_name, record_description="", dest_connection=None, foreign_keys=None):
    update_col_idx = [col for col, _, _ in columns].index(update_col)
    update_col_type = columns[update_col_idx][1]
    update_col_length = columns[update_col_idx][2]
    
    fk_validated_value = update_val
    fk_correction_message = None
    
    if dest_connection and foreign_keys and update_col.upper() in foreign_keys:
        fk_info = foreign_keys[update_col.upper()]
        is_valid, corrected_value, message = validate_foreign_key_value(
            dest_connection, fk_info, update_val, update_col
        )
        
        if not is_valid and corrected_value is not None:
            fk_validated_value = corrected_value
            fk_correction_message = f"UPDATE FK correction: {message}"
            print(f"[FK_CORRECTION] {message}")
        elif not is_valid:
            print(f"[FK_ERROR] FK validation failed for UPDATE column {update_col}: {message}")
    
    final_update_val = fk_validated_value
    
    if not final_update_val:
        fake_val, val_type = generate_fake_data(update_col_type, update_col_length)
        if val_type == 'STRING':
            escaped_fake = fake_val.replace("'", "''")
            set_value = f"'{escaped_fake}'"
        elif val_type == 'DATE':
            set_value = f"TO_DATE('{fake_val}', 'DD-Mon-YYYY')"
        elif val_type == 'TIMESTAMP':
            set_value = f"TO_DATE('{fake_val}', 'DD-Mon-YYYY')"
        else:
            set_value = str(fake_val)
    else:
        if update_col_type.upper() in ['VARCHAR2', 'CHAR', 'NVARCHAR2']:
            escaped_val = final_update_val.replace("'", "''")
            set_value = f"'{escaped_val}'"
        elif update_col_type.upper() == 'DATE':
            oracle_date = format_date_for_oracle(final_update_val, include_time=False)
            set_value = f"TO_DATE('{oracle_date}', 'DD-Mon-YYYY')"
        elif update_col_type.upper().startswith('TIMESTAMP'):
            oracle_date = format_date_for_oracle(final_update_val, include_time=False)
            set_value = f"TO_DATE('{oracle_date}', 'DD-Mon-YYYY')"
        else:
            set_value = str(final_update_val)
    
    where_conditions = []
    for key_col in key_cols:
        if key_col in common_cols:
            idx = common_cols.index(key_col)
            if idx < len(row):
                val = row[idx]
                if val is None:
                    where_conditions.append(f"{key_col} IS NULL")
                else:
                    key_col_type = None
                    for col_name, col_type, _ in columns:
                        if col_name == key_col:
                            key_col_type = col_type
                            break
                    
                    if key_col_type and key_col_type.upper() in ['DATE'] or (key_col_type and key_col_type.upper().startswith('TIMESTAMP')):
                        where_conditions.append(format_date_for_where_clause(val, key_col))
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
        
        pre_count_where_conditions = where_conditions.copy()
        if current_value is not None:
            if update_col_type.upper() in ['DATE'] or update_col_type.upper().startswith('TIMESTAMP'):
                pre_count_where_conditions.append(format_date_for_where_clause(current_value, update_col))
            elif isinstance(current_value, str):
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
        
        statements = [pre_count_statement, update_statement, post_count_statement]
        
        if fk_correction_message:
            fk_comment_statement = {
                'description': f'FK Correction for UPDATE: {fk_correction_message}',
                'sql': f"-- {fk_correction_message}",
                'dml': 'Yes',
                'schema': dest_schema,
                'db': dest_service_name,
                'dbtype': 'oracle',
                'connection_name': dest_connection_name
            }
            statements.insert(0, fk_comment_statement)  
        
        return tuple(statements) if fk_correction_message else (pre_count_statement, update_statement, post_count_statement)
    else:
        return None, None, None

if __name__ == "__main__":
    main()


