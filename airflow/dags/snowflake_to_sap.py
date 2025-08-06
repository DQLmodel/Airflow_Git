from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import snowflake.connector
from hdbcli import dbapi

# Snowflake credentials
SNOWFLAKE_USER = 'Dqlabs'
SNOWFLAKE_PASSWORD = 'DQlabs!@#$5'
SNOWFLAKE_ACCOUNT = 'bq24865.us-east-2.aws'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'DQLABS'

# SAP HANA credentials
SAP_HANA_HOST = '4eb2a5d4-c4f5-4a4a-93f7-e1e242f365d6.hana.trial-us10.hanacloud.ondemand.com'
SAP_HANA_PORT = 443
SAP_HANA_USER = 'dbadmin'
SAP_HANA_PASSWORD = 'ObobNHSRzB1!'

# Function to connect to Snowflake
def connect_to_snowflake():
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE
    )
    return conn

# Function to extract data from Snowflake
def extract_data_from_snowflake():
    """
    Extract data from Snowflake and store it in a CSV file.
    """
    conn = connect_to_snowflake()
    query = """
        SELECT * FROM DQLABS.CARMAX.COLGATE_TESTING;
    """

    # Execute the query
    cursor = conn.cursor()
    cursor.execute(query)

    # Fetch the result and convert to DataFrame
    df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

    # Save the result to a CSV file
    df.to_csv('/tmp/snowflake_colgate_data.csv', index=False)

    # Close the connection
    cursor.close()
    conn.close()

    print("Data extracted from Snowflake and saved to /tmp/snowflake_colgate_data.csv")

# Function to connect to SAP HANA
def connect_to_sap_hana():
    conn = dbapi.connect(
        address=SAP_HANA_HOST,
        port=SAP_HANA_PORT,
        user=SAP_HANA_USER,
        password=SAP_HANA_PASSWORD
    )
    return conn

# Function to load data into SAP HANA
def load_data_into_sap_hana():
    """
    Load the CSV data into SAP HANA.
    """
    sap_conn = connect_to_sap_hana()

    # Read the CSV file into a DataFrame
    df = pd.read_csv('/tmp/snowflake_colgate_data.csv')

    # Convert DataFrame to a list of tuples
    data = [tuple(row) for row in df.to_numpy()]

    # Prepare the INSERT SQL query
    placeholders = ', '.join(['?' for _ in df.columns])
    columns = ', '.join(df.columns)
    query = f"""
        INSERT INTO DBADMIN.COLGATE_TESTING_NEW({columns})
        VALUES ({placeholders})
    """

    # Execute the query for each row
    cursor = sap_conn.cursor()
    cursor.executemany(query, data)
    sap_conn.commit()

    # Close the connection
    cursor.close()
    sap_conn.close()

    print("Data loaded into SAP HANA from /tmp/snowflake_colgate_data.csv")

# Define default args
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Initialize the DAG
with DAG(
    dag_id='snowflake_to_sap_hana_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust your schedule as needed
    catchup=False,
) as dag:

    # Task 1: Extract data from Snowflake
    extract_data_from_snowflake_task = PythonOperator(
        task_id='extract_data_from_snowflake',
        python_callable=extract_data_from_snowflake
    )

    # Task 2: Load data into SAP HANA
    load_data_into_sap_hana_task = PythonOperator(
        task_id='load_data_into_sap_hana',
        python_callable=load_data_into_sap_hana
    )

    # Task dependencies
    extract_data_from_snowflake_task >> load_data_into_sap_hana_task
