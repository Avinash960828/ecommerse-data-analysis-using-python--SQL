import pandas as pd
import mysql.connector
import os

# -----------------------------
# MySQL connection
# -----------------------------
conn = mysql.connector.connect(
    host='localhost',        # MySQL host
    user='root',             # MySQL username
    password='960828@avk',   # MySQL password
    database='sales_db1'         # Make sure this database exists
)
cursor = conn.cursor()

# -----------------------------
# Folder containing CSV files
# -----------------------------
folder_path = r"C:\Users\ak695\OneDrive\Desktop\dataanalystproject\project01\zipfiledataset"

# -----------------------------
# List of CSV files and table names
# -----------------------------
csv_files = [
    ('customers.csv', 'customers'),
    ('products.csv', 'products'),
    ('orders.csv', 'orders'),
    ('order_items.csv', 'order_items'),
    ('sellers.csv', 'sellers'),
    ('payments.csv', 'payments'),
    ('geolocation.csv', 'geolocation')
]

# -----------------------------
# Drop all tables first (child tables first)
# -----------------------------
tables_to_drop = ['order_items', 'payments', 'orders', 'products', 'customers', 'sellers', 'geolocation']
for table in tables_to_drop:
    cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
    print(f"Dropped table {table} if it existed.")

# -----------------------------
# Helper function to map pandas dtype to MySQL
# -----------------------------
def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

# -----------------------------
# Process each CSV file
# -----------------------------
for csv_file, table_name in csv_files:
    try:
        file_path = os.path.join(folder_path, csv_file)
        df = pd.read_csv(file_path, encoding='utf-8')

        # Drop duplicate columns automatically
        df = df.loc[:, ~df.columns.duplicated()]

        # Replace NaN with None for SQL
        df = df.where(pd.notnull(df), None)

        # Clean column names
        df.columns = [col.strip().replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

        # Debug: print columns and first 3 rows
        print(f"\nProcessing {csv_file}")
        print("Columns:", df.columns.tolist())
        print("First 3 rows:\n", df.head(3))

        # Create table dynamically
        columns_sql = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
        create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns_sql})'
        cursor.execute(create_table_query)
        print(f"Table `{table_name}` created.")

        # Insert data using executemany (faster)
        values_list = [tuple(None if pd.isna(x) else x for x in row) for row in df.itertuples(index=False, name=None)]
        sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(df.columns))})"
        cursor.executemany(sql, values_list)
        conn.commit()
        print(f"Inserted {len(df)} rows into `{table_name}`")

    except Exception as e:
        print(f"Error processing {csv_file}: {e}")

# -----------------------------
# Close connection
# -----------------------------
cursor.close()
conn.close()
print("\nAll CSV files processed and data inserted successfully!")
