from airflow import DA
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import snowflake.connector
from hdbcli import dbapi
import os

# Function to connect to SAP HANA
def connect_to_sap_hana():
    """
    Establishes a connection to the SAP HANA database using hdbcli.
    """
    try:
        # Retrieve connection details from Airflow connections (use BaseHook for managing creds)
        hana_conn = BaseHook.get_connection('hana_connection')  # Replace with your Airflow SAP HANA connection ID
        conn = dbapi.connect(
            address=hana_conn.host,
            port=hana_conn.port,
            user=hana_conn.login,
            password=hana_conn.password,
            encrypt=True
        )
        return conn
    except Exception as e:
        print(f"Failed to connect to SAP HANA: {e}")
        raise

# Function to extract and run query on SAP HANA
def extract_sales_data_from_sap():
    """
    Extract data from SAP HANA using the provided SQL query and store it in a CSV file.
    """
    try:
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

        # Save the result to a CSV file (Consider using cloud storage if running on multiple machines)
        csv_file_path = '/tmp/sap_sales_data.csv'
        df.to_csv(csv_file_path, index=False)

        # Close connection
        cursor.close()
        sap_conn.close()

        print(f"Data extracted from SAP HANA and saved to {csv_file_path}")
        return csv_file_path
    except Exception as e:
        print(f"Error in SAP HANA extraction: {e}")
        raise

# Function to upload CSV to Snowflake
def upload_to_snowflake(csv_file_path):
    """
    Upload the CSV file to Snowflake stage and copy it into a table.
    """
    try:
        # Connect to Snowflake (use Airflow connections)
        snowflake_conn = BaseHook.get_connection('snowflake_connection')  # Replace with your Airflow Snowflake connection ID
        conn = snowflake.connector.connect(
            user=snowflake_conn.login,
            password=snowflake_conn.password,
            account=snowflake_conn.host,
            warehouse=snowflake_conn.extra_dejson['warehouse'],
            database=snowflake_conn.schema,
            schema=snowflake_conn.schema,
            role=snowflake_conn.extra_dejson['role']
        )

        # Create a Snowflake cursor
        cursor = conn.cursor()

        # Upload CSV file to the Snowflake stage (use cloud storage or ensure path is correct)
        cursor.execute("USE SCHEMA STAGING;")
        cursor.execute("CREATE OR REPLACE STAGE STAGING.my_stage;")
        print("Stage created/replaced.")

        cursor.execute(f"PUT file://{csv_file_path} @my_stage;")
        print(f"CSV file {csv_file_path} uploaded to stage.")

        # File format configuration
        cursor.execute("""
            CREATE OR REPLACE FILE FORMAT my_csv_format
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1;
        """)
        print("File format created/replaced with header skip.")

        # Copy data from stage to Snowflake table
        cursor.execute("""
            COPY INTO DQLABS_QA.STAGING.PRODUCT_SALES
            FROM @my_stage/sap_sales_data.csv
            FILE_FORMAT = (TYPE = 'CSV' FORMAT_NAME = my_csv_format);
        """)
        print("Data copied into Snowflake table.")

    except Exception as e:
        print(f"Error during Snowflake operations: {e}")
        raise
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
    dag_id='TestDAG',
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
        python_callable=upload_to_snowflake,
        op_args=['{{ task_instance.xcom_pull(task_ids="extract_sales_data_from_sap") }}'],  # Pass file path from XCom
    )

    # Task dependencies
    extract_sales_data_task >> upload_to_snowflake_task
