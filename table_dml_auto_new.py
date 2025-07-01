
import os
import oracledb
import random
import string
from dotenv import load_dotenv
import sys
from faker import Faker

fake = Faker()

#Purpose: 
#This script generates mock DELETE, INSERT, and UPDATE SQL statements for a specified Oracle table,
#using meaningful fake data for VARCHAR2/CHAR columns (via the Faker library). It supports Oracle 
#check constraints, identity columns, and sequence columns, and writes the generated SQL to an 
#output file. Useful for testing, development, and populating Oracle tables with sample data.

#Developed by: Debasish Ghosh
#Date: June 2025

#oracledb.init_oracle_client(lib_dir=r"C:\path\to\instantclient")
# Load Oracle connection details from environment variables or a .env file

#dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
#load_dotenv(dotenv_path)
#load_dotenv()
env_path = ".env"
if len(sys.argv) > 1:
    env_path = sys.argv[1]
load_dotenv(env_path)

# Oracle connection details are now loaded from environment variables or .env file

ORACLE_USER = os.getenv('ORACLE_USER')
ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD')
ORACLE_DSN = os.getenv('ORACLE_DSN')

#ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD', 'changeme')
#ORACLE_DSN = os.getenv('ORACLE_DSN', 'localhost:1521/FREE') 1522 for MAC



connection = oracledb.connect(
    user=ORACLE_USER,
    password=ORACLE_PASSWORD,
    dsn=ORACLE_DSN
)

# Below section to get columns, Primary key (PK), unique key (UK), or treat first column as PK
# Returns: (columns, key_type, key_cols)
def get_table_metadata(connection, schema, table):
    cursor = connection.cursor()
    # Get all columns and their data types, lengths, defaults, and virtual status for the table
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

# Generate a random value for a given data type
# If is_key is True, use the index as the value for uniqueness
def random_value(data_type, length, is_key=False, idx=1, allowed_values=None, col_name=None):
    import random
    if allowed_values:
        return random.choice(list(allowed_values))
    if is_key:
        return str(idx)
    if data_type in ('VARCHAR2', 'CHAR'):
        # Use column name to generate context-aware data
        if col_name:
            col_lower = col_name.lower()
            if 'name' in col_lower:
                return fake.name()[:length]
            elif 'email' in col_lower:
                return fake.email()[:length]
            elif 'address' in col_lower:
                return fake.address().replace('\n', ' ')[:length]
            elif 'city' in col_lower:
                return fake.city()[:length]
            elif 'phone' in col_lower or 'mobile' in col_lower:
                return fake.phone_number()[:length]
            elif 'desc' in col_lower or 'text' in col_lower:
                return fake.sentence(nb_words=6)[:length]
        return fake.word()[:length]
    elif data_type == 'NUMBER':
        return str(random.randint(1, 9999))
    elif data_type == 'DATE':
        return 'SYSDATE'
    else:
        return 'NULL'

# Generate mock DELETE and INSERT SQL for N records
def generate_insert_sql(schema, table, columns, key_type, key_cols, num_records, always_identity_cols=None, seq_cols=None, check_constraints=None):
    lines = []
    full_table = f"{schema}.{table}"
    if seq_cols is None:
        seq_cols = {}
    if always_identity_cols is None:
        always_identity_cols = set()
    if check_constraints is None:
        check_constraints = {}
    # Exclude DELETE statement if any identity or sequence column exists
    if key_cols and not (always_identity_cols or seq_cols):
        key_col = key_cols[0]  # Only support single-column key for delete
        for i in range(1, num_records + 1):
            lines.append(f"DELETE FROM {full_table} WHERE {key_col} = {i};")
    # Exclude only GENERATED ALWAYS identity columns, always include sequence columns
    insert_columns = [(col, dtype, length) for col, dtype, length in columns if col not in always_identity_cols]
    # Always generate individual INSERTs
    for i in range(1, num_records + 1):
        values = []
        for col, dtype, length in insert_columns:
            is_key = col in key_cols
            allowed = check_constraints.get(col)
            if seq_cols and col in seq_cols:
                values.append(f"{seq_cols[col]}.NEXTVAL")
            else:
                val = random_value(dtype, length, is_key, i, allowed, col)
                # Format value based on data type
                if dtype == 'DATE' and not is_key:
                    values.append(val)
                elif dtype == 'NUMBER' and not is_key:
                    values.append(val)
                elif dtype == 'DATE' and is_key:
                    values.append(val)
                elif dtype == 'NUMBER' and is_key:
                    values.append(val)
                else:
                    values.append(f"'{val}'")
        col_names = ', '.join(col for col, _, _ in insert_columns)
        val_str = ', '.join(values)
        lines.append(f"INSERT INTO {full_table} ({col_names}) VALUES ({val_str});")
    lines.append("COMMIT;")
    return '\n'.join(lines)

# Generate mock UPDATE SQL for M records, updating a single column
def generate_update_sql(schema, table, columns, key_type, key_cols, num_records, col_to_update, check_constraints=None):
    import random
    # Find the data type and length for the column to update
    dtype, length = next((dtype, length) for col, dtype, length in columns if col == col_to_update)
    key_col = key_cols[0]
    allowed = None
    if check_constraints:
        allowed = check_constraints.get(col_to_update)
    lines = []
    for i in range(1, num_records + 1):
        new_val = random_value(dtype, length, is_key=False, idx=i, allowed_values=allowed, col_name=col_to_update)
        # Format value based on data type
        if allowed:
            set_val = f"'{new_val}'"
        elif dtype in ('DATE', 'NUMBER'):
            set_val = new_val
        else:
            set_val = f"'{new_val}'"
        lines.append(f"UPDATE {schema}.{table} SET {col_to_update} = {set_val} WHERE {key_col} = {i};")
    lines.append("COMMIT;")
    return '\n'.join(lines)

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
        m = re.search(rf"{col} IN \(([^)]+)\)", cond, re.IGNORECASE)
        if m:
            vals = m.group(1)
            # Remove quotes and spaces, split by comma
            allowed = [v.strip().strip("'") for v in vals.split(',')]
            check_map.setdefault(col, set()).update(allowed)
    return check_map

# Main workflow: prompt for schema/table/insert count, generate insert/delete, then update SQL
def main():
    schema = input("Enter schema name: ").strip()
    table = input("Enter table name: ").strip()
    num_records = int(input("Enter number of records to insert: "))
    user = schema  # Use the schema as the user for connection
    try:
        # Connect as the schema user
        connection = oracledb.connect(user=user, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)
        print("Successfully connected to Oracle Database")
        columns, key_type, key_cols, always_identity_cols, seq_cols = get_table_metadata(connection, schema, table)
        check_constraints = get_table_check_constraints(connection, schema, table)
        if not columns:
            print(f"No columns found for table {table} in schema {schema}.")
            return
        print(f"Key type: {key_type}, Key columns: {key_cols}")
        if always_identity_cols:
            print(f"GENERATED ALWAYS identity columns (excluded from insert): {always_identity_cols}")
        if seq_cols:
            print(f"Columns using .NEXTVAL in insert: {seq_cols}")
        if check_constraints:
            print(f"Check constraints detected: {check_constraints}")
        # Generate DELETE and INSERT SQL for N records
        sql_lines = []
        insert_sql = generate_insert_sql(schema, table, columns, key_type, key_cols, num_records, always_identity_cols, seq_cols, check_constraints)
        sql_lines.append(insert_sql)
        # Prompt for update column and number of updates
        updatable_cols = [col for col, _, _ in columns if col not in key_cols and col not in always_identity_cols]
        if not updatable_cols:
            print("No updatable columns (all columns are PK/UK or GENERATED ALWAYS identity)")
            connection.close()
            return
        print(f"Updatable columns: {updatable_cols}")
        col_to_update = input("Which column do you want to update? ").strip().upper()
        while col_to_update not in updatable_cols:
            print(f"Column {col_to_update} is not updatable (it may be a PK/UK, GENERATED ALWAYS identity, or does not exist)")
            col_to_update = input("Please enter a valid updatable column: ").strip().upper()
        # Ask for number of records to update, must be 1 <= M <= N
        while True:
            num_updates = int(input(f"Enter number of update statements to generate (1 to {num_records}): "))
            if 1 <= num_updates <= num_records:
                break
            print(f"Number of records to update must be between 1 and {num_records}.")
        # Generate UPDATE SQL for M records
        update_sql = generate_update_sql(schema, table, columns, key_type, key_cols, num_updates, col_to_update, check_constraints)
        sql_lines.append(update_sql)
        # Write all SQL to a single output file
        output_file = f"mock_{table.lower()}.sql"
        with open(output_file, 'w') as f:
            f.write('\n'.join(sql_lines))
        print(f"SQL (delete, insert, update) statements written to {output_file}")
        connection.close()
    except Exception as error:
        print(error)

if __name__ == "__main__":
    main()
