from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import pandas as prani
import tempfile
import os
import time
from snowflake.connector.errors import OperationalError

# Constants
TARGET_TABLE = 'retail_sales'  # Snowflake table to load data into
SNOWFLAKE_STAGE = 'PIPELINE_STAGE'
SNOWFLAKE_DATABASE = 'DQLABS_QA'  # Your Snowflake database
SNOWFLAKE_SCHEMA = 'STAGING'     # Your Snowflake schema

# Hardcoded Credentials for External Testing
MSSQL_CONFIG = {
    'server': '198.37.101.135',
    'database': 'dqlabs',
    'username': 'dqlabs',
    'password': 'Intellectyx@123',
    'port': 1433,
    'driver': 'ODBC Driver 18 for SQL Server'
}

SNOWFLAKE_CONFIG = {
    'account': 'tr68481.us-east-2.aws',
    'user': 'USER_QA',
    'password': 'Dql@bs2022',
    'warehouse': 'DQLABS_QA',
    'database': 'DQLABS_QA',
    'schema': 'STAGING',
    'role': 'USER_QA'  # Using same role as user
}

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

def get_snowflake_connection():
    """Get Snowflake connection with retry logic using hardcoded credentials."""
    import snowflake.connector
    
    for attempt in range(MAX_RETRIES):
        try:
            conn = snowflake.connector.connect(
                account=SNOWFLAKE_CONFIG['account'],
                user=SNOWFLAKE_CONFIG['user'],
                password=SNOWFLAKE_CONFIG['password'],
                warehouse=SNOWFLAKE_CONFIG['warehouse'],
                database=SNOWFLAKE_CONFIG['database'],
                schema=SNOWFLAKE_CONFIG['schema'],
                role=SNOWFLAKE_CONFIG['role']
            )
            if conn is None:
                raise AirflowFailException("Failed to get Snowflake connection - connection is None")
            return conn
        except (OperationalError, Exception) as e:
            if attempt == MAX_RETRIES - 1:
                raise AirflowFailException(f"Failed to connect to Snowflake after {MAX_RETRIES} attempts: {str(e)}")
            print(f"Connection attempt {attempt + 1} failed. Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
    raise AirflowFailException(f"Failed to connect to Snowflake after {MAX_RETRIES} attempts")

def extract_data_from_mssql(**context):
    """Extract retail sales data from MSSQL and save to CSV using hardcoded credentials."""
    import pyodbc
    
    # Build connection string using hardcoded credentials
    conn_str = f"DRIVER={{{MSSQL_CONFIG['driver']}}};SERVER={MSSQL_CONFIG['server']};DATABASE={MSSQL_CONFIG['database']};UID={MSSQL_CONFIG['username']};PWD={MSSQL_CONFIG['password']};PORT={MSSQL_CONFIG['port']}"
    
    # Your updated query to fetch data from MSSQL
    query = """
    SELECT Record_ID, [Date], Store_ID, Product_Name, Category, Quantity, Unit_Price, Total_Amount, Customer_ID, Sales_Rep, Region
    FROM dqlabs.Hariii.raw_retail_sales;
    """
    # Extract data from MSSQL
    try:
        # Connect using pyodbc with hardcoded credentials
        conn = pyodbc.connect(conn_str)
        df = pd.read_sql(query, conn)
        conn.close()
        print("\n=== Data Preview ===")
        print(f"Total number of records: {len(df)}")
        print("\nFirst 5 rows:")
        print(df.head().to_string())
        
        if len(df) == 0:
            print("Warning: No data found in the source table!")
            # Create empty CSV with headers
            df = pd.DataFrame(columns=['Record_ID', 'Date', 'Store_ID', 'Product_Name', 'Category', 'Quantity', 'Unit_Price', 'Total_Amount', 'Customer_ID', 'Sales_Rep', 'Region'])
        
        # Save to CSV
        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        df.to_csv(tmp_file.name, index=False)
        context['ti'].xcom_push(key='csv_path', value=tmp_file.name)
        # Log the file path for debugging
        print(f"\nCSV file saved to: {tmp_file.name}")
        print(f"CSV file size: {os.path.getsize(tmp_file.name)} bytes")
        
        return f"Successfully extracted {len(df)} records"
        
    except Exception as e:
        raise AirflowFailException(f"Error extracting data from MSSQL: {str(e)}")

def upload_to_snowflake_stage(**context):
    """Upload CSV file to Snowflake stage using hardcoded credentials."""
    csv_path = context['ti'].xcom_pull(key='csv_path')
    try:
        # Get the Snowflake connection using hardcoded credentials
        conn = get_snowflake_connection()
        cur = conn.cursor()
        
        # Create the stage if it doesn't exist
        create_stage_sql = f"""
        CREATE STAGE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
        """
        print(f"Creating stage: {create_stage_sql}")
        cur.execute(create_stage_sql)
        
        # Upload the CSV file to the stage
        put_sql = f"PUT file://{csv_path} @{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}"
        print(f"Uploading file: {put_sql}")
        cur.execute(put_sql)
        
        # List files in stage to verify upload
        list_sql = f"LIST @{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}"
        print(f"Listing files in stage: {list_sql}")
        cur.execute(list_sql)
        files = cur.fetchall()
        print(f"Files in stage: {files}")
        
        # Commit the transaction
        conn.commit()
        print(f"File uploaded to stage successfully: {csv_path}")
        
        return "File uploaded to Snowflake stage successfully"
        
    except Exception as e:
        if 'conn' in locals():
            conn.rollback()
        raise AirflowFailException(f"Failed to upload file to Snowflake stage: {str(e)}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def ensure_target_table_exists(**context):
    """Ensure the target table exists with correct schema."""
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        
        # Define the table schema
        schema = {
            'Record_ID': 'VARCHAR(50)',
            'Date': 'DATE',
            'Store_ID': 'VARCHAR(20)',
            'Product_Name': 'VARCHAR(200)',
            'Category': 'VARCHAR(100)',
            'Quantity': 'INTEGER',
            'Unit_Price': 'DECIMAL(10,2)',
            'Total_Amount': 'DECIMAL(12,2)',
            'Customer_ID': 'VARCHAR(50)',
            'Sales_Rep': 'VARCHAR(100)',
            'Region': 'VARCHAR(50)'
        }
        
        # Drop the table if it exists to ensure correct schema
        drop_stmt = f"DROP TABLE IF EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TARGET_TABLE};"
        cur.execute(drop_stmt)
        
        # Create the table with the correct schema
        create_stmt = f"""
        CREATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TARGET_TABLE} (
            {', '.join(f"{column_name} {data_type}" for column_name, data_type in schema.items())}
        );
        """
        print(f"Creating table: {create_stmt}")
        cur.execute(create_stmt)
        conn.commit()
        print(f"Table {TARGET_TABLE} created successfully")
        
        return f"Table {TARGET_TABLE} ensured successfully"
        
    except Exception as e:
        if 'conn' in locals():
            conn.rollback()
        raise AirflowFailException(f"Failed to ensure target table exists: {str(e)}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def copy_into_target_table(**context):
    """Copy data from stage to target table."""
    csv_path = context['ti'].xcom_pull(key='csv_path')
    file_name = os.path.basename(csv_path)
    conn = None
    cur = None
    
    try:
        # Get Snowflake connection
        conn = get_snowflake_connection()
        cur = conn.cursor()
        
        # Get the actual compressed filename from the stage
        list_sql = f"LIST @{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}"
        cur.execute(list_sql)
        files = cur.fetchall()
        
        # Find the file that matches our base name (it will be compressed with .gz extension)
        compressed_file_name = None
        for file_info in files:
            if file_info[0].endswith(f"{file_name}.gz"):
                compressed_file_name = file_info[0].split('/')[-1]  # Get just the filename
                break
        
        if not compressed_file_name:
            raise AirflowFailException(f"Could not find compressed file for {file_name} in stage")
        
        print(f"Found compressed file: {compressed_file_name}")
        
        # Perform the COPY operation with the compressed filename
        copy_stmt = f"""
        COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TARGET_TABLE}
        FROM @{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}
        FILES = ('{compressed_file_name}')
        FILE_FORMAT = (
            TYPE = 'CSV',
            FIELD_OPTIONALLY_ENCLOSED_BY = '"',
            SKIP_HEADER = 1,
            FIELD_DELIMITER = ',',
            NULL_IF = ('NULL', 'null', '')
        )
        ON_ERROR = 'CONTINUE';
        """
        print(f"\nExecuting COPY command: {copy_stmt}")
        cur.execute(copy_stmt)
        
        # Get the result to see how many rows were loaded
        result = cur.fetchone()
        if result:
            print(f"COPY result: {result}")
        
        # Commit the transaction
        conn.commit()
        print(f"Data loaded into {TARGET_TABLE} successfully.")
        
        # Verify the data was loaded
        verify_stmt = f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TARGET_TABLE}"
        cur.execute(verify_stmt)
        count = cur.fetchone()[0]
        print(f"Total rows in {TARGET_TABLE}: {count}")
        
        # Clean up the temporary CSV file only after successful copy
        try:
            if os.path.exists(csv_path):
                os.unlink(csv_path)
                print(f"Cleaned up temporary file: {csv_path}")
        except Exception as e:
            print(f"Warning: Could not clean up temporary file {csv_path}: {str(e)}")
        
        return f"Successfully loaded {count} rows into {TARGET_TABLE}"
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error in copy_into_target_table: {str(e)}")
        raise AirflowFailException(f"Failed to copy data into target table: {str(e)}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# DAG definition
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['your-email@example.com'],  # Add your email for notifications
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
}

with DAG(
    dag_id='mssql_to_snowflake_retail_sales',
    default_args=default_args,
    schedule_interval=None,  # This can be set to a cron schedule if needed
    start_date=datetime(2023, 1, 1),
    catchup=False,
    description='ETL from MSSQL to Snowflake for Retail Sales',
    tags=['retail', 'sales', 'etl', 'mssql', 'snowflake']
) as dag:
    
    extract_data = PythonOperator(
        task_id='extract_data_from_mssql',
        python_callable=extract_data_from_mssql,
        doc="Extract retail sales data from MSSQL and save to CSV"
    )
    
    ensure_table = PythonOperator(
        task_id='ensure_target_table_exists',
        python_callable=ensure_target_table_exists,
        doc="Ensure the target Snowflake table exists with correct schema"
    )
    
    upload_stage = PythonOperator(
        task_id='upload_to_snowflake_stage',
        python_callable=upload_to_snowflake_stage,
        doc="Upload CSV file to Snowflake stage"
    )
    
    copy_data = PythonOperator(
        task_id='copy_into_target_table',
        python_callable=copy_into_target_table,
        doc="Copy data from stage to target table"
    )
    
    # Task dependencies
    extract_data >> ensure_table >> upload_stage >> copy_data
