from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import snowflake.connector
from hdbcli import dbapi
from datetime import datetime, timedelta

# Snowflake credentials
SNOWFLAKE_USER = 'USER_QA'
SNOWFLAKE_PASSWORD = 'Dql@bs2022'
SNOWFLAKE_ACCOUNT = 'tr68481.us-east-2.aws'
SNOWFLAKE_WAREHOUSE = 'DQLABS_QA'
SNOWFLAKE_DATABASE = 'DQLABS_QA'

# Function to connect to SAP HANA
def connect_to_sap_hana():
    """
    Establishes a connection to the SAP HANA database using hdbcli.
    """
    conn = dbapi.connect(
        address='3e5df258-c7cd-4167-8fd4-24fb228ac9fc.hana.trial-us10.hanacloud.ondemand.com',  # Replace with your SAP HANA host
        port=443,                    # Replace with your SAP HANA port
        user='dbadmin',          # Replace with your SAP HANA username
        password='ObobNHSRzB1!'       # Replace with your SAP HANA password
    )
    return conn

# Function to extract and run query on SAP HANA
def extract_sales_data_from_sap():
    """
    Extract data from SAP HANA using the provided SQL query and store it in a CSV file.
    """
    sap_conn = connect_to_sap_hana()

    query = """
        SELECT PRODUCTID, 
               sum(GROSSAMOUNT) AS total_amount, 
               sum(NETAMOUNT) AS total_net_amount, 
               sum(TAXAMOUNT) AS total_tax
        FROM DBADMIN.SALESORDERITEMS
        GROUP BY PRODUCTID
    """

    # Execute the query
    cursor = sap_conn.cursor()
    cursor.execute(query)

    # Fetch the result and convert to DataFrame
    result = cursor.fetchall()
    df = pd.DataFrame(result, columns=['PRODUCTID', 'total_amount', 'total_net_amount', 'total_tax'])

    # Save the result to a CSV file
    df.to_csv('/tmp/sap_sales_data.csv', index=False)

    # Close connection
    cursor.close()
    sap_conn.close()

    print("Data extracted from SAP HANA and saved to /tmp/sap_sales_data.csv")

# Function to upload CSV to Snowflake
def upload_to_snowflake():
    """
    Upload the CSV file to Snowflake stage and copy it into a table.
    """
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE
    )

    # Create a Snowflake cursor
    cursor = conn.cursor()

    try:
        # Create or replace stage
        cursor.execute("USE SCHEMA  STAGING;")
        cursor.execute("CREATE OR REPLACE STAGE STAGING.my_stage;")
        print("Stage created/replaced.")

        # Upload CSV file to the stage
        cursor.execute(f"PUT file:///tmp/sap_sales_data.csv @my_stage;")
        print("CSV file uploaded to stage.")

        # Create or replace file format with header skip
        cursor.execute("""
            CREATE OR REPLACE FILE FORMAT my_csv_format
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1;
        """)
        print("File format created/replaced with header skip.")

        # Copy data from stage to Snowflake table
        cursor.execute("""
            COPY INTO DQLABS_QA4.STAGING.PRODUCT_SALES
            FROM @my_stage/sap_sales_data.csv
            FILE_FORMAT = (TYPE = 'CSV' FORMAT_NAME = my_csv_format);
        """)
        print("Data copied into Snowflake table.")
    
    finally:
        # Close cursor and connection
        cursor.close()
        conn.close()

    print("Snowflake operations completed successfully.")


# Define default args
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Initialize the DAG
with DAG(
    dag_id='sap_sales_data_to_snowflake_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust your schedule as needed
    catchup=False,
    tags=["etl", "sales", "production"]
) as dag:

    # Task 1: Extract sales data from SAP HANA
    extract_sales_data_task = PythonOperator(
        task_id='extract_sales_data_from_sap',
        python_callable=extract_sales_data_from_sap
    )

    # Task 2: Upload CSV to Snowflake
    upload_to_snowflake_task = PythonOperator(
        task_id='upload_to_snowflake',
        python_callable=upload_to_snowflake
    )

    # Task dependencies
    extract_sales_data_task >> upload_to_snowflake_task
