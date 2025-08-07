from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import urllib

# Define MSSQL connection parameters
MSSQL_CONFIG = {
    'server': '198.37.101.135',
    'database': 'dqlabs',
    'username': 'dqlabs',
    'password': 'Intellectyx@123',
    'port': '1433'
}

# Define Azure Synapse connection parameters
server = 'synapsedbtest.sql.azuresynapse.net'
database = 'dedicatedpool1'
username = 'sqladminuser'
password = 'DQL@bs!@#$%'
table = 'customer_rcp'

# Define the target columns for the Azure Synapse table
target_columns = [
    'WASTE_TYPE', 'DISPOSAL_NAME', 'CARRYALLOW', 'CUST_SRVC_SITE', 'SITE_CD', 
    'SITE_NAME', 'MKT_STAT_AREA_NAME', 'MKT_AREA_CD', 'MKT_AREA_NAME', 'ORG_GRP_NAME', 
    'DIM_ORG_SITE_KEY', 'CREATED_BY', 'CREATED_TIMESTAMP', 'DRIVER_ID', 'MAINTENANCE_STATUS', 
    'RETURN_SERVICE_DATE', 'ROUTE_EFFICIENCY_AT_ROUTE_START', 'ROUTE_ID', 
    'ROUTE_ID_AT_VEHICLE_DOWN', 'ROUTE_ORDER_STATS_AT_ROUTE_START', 'ROUTE_START_LOCK', 
    'SITE_ID', 'STARTING_RAIL_STATUS', 'SUB_LOB_ID', 'TRANSACTION_ID', 'TRASMIT_STATUS', 
    'UPDATED_BY', 'UPDATED_BY_EVENT', 'UPDATED_BY_USER', 'UPDATED_TIMESTAMP', 
    'VEHICLE_ID', 'VEHICLE_ID_AT_VEHICLE_DOWN', 'VEHICLE_STATUS', 'AUDIT_ID', 
    'AUDIT_LOAD_DTM', 'DIM_SITE_CD', 'DIM_MKT_AREA_CD'
]


# Function to extract data from MSSQL
def extract_data(**kwargs):
    try:
        # Create connection to MSSQL
        mssql_conn_str = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={MSSQL_CONFIG["server"]},{MSSQL_CONFIG["port"]};'
            f'DATABASE={MSSQL_CONFIG["database"]};'
            f'UID={MSSQL_CONFIG["username"]};'
            f'PWD={MSSQL_CONFIG["password"]};'
        )
        mssql_conn = pyodbc.connect(mssql_conn_str)
        
        # Execute SQL query to fetch data
        query = 'SELECT * FROM dqlabs.STAGING.CUSTOMER_RCP'
        data = pd.read_sql(query, mssql_conn)
        data = data.astype(str)
        print(data)
        print(f"Data types: {data.dtypes}")
        print('Data loaded successfully')
        
        # Close the MSSQL connection
        mssql_conn.close()
        
        # Push the DataFrame to XCom
        kwargs['ti'].xcom_push(key='mssql_data', value=data)
    
    except Exception as e:
        print(f"Error in data extraction: {e}")

# Function to push data to Azure Synapse
def push_data(**kwargs):
    try:
        # Retrieve DataFrame from XCom
        ti = kwargs['ti']
        data = ti.xcom_pull(key='mssql_data', task_ids='extract_data')

        if data is None:
            raise ValueError("No data found in XCom.")
        print(f"Data to be pushed: {data.head()}")
        print(f"Data types: {data.dtypes}")

        data = data[target_columns]  # Select only the target columns from the DataFrame
        print(f"Filtered data columns: {data.columns}")

        # Create connection string for Azure Synapse
        params = urllib.parse.quote_plus(
            f'Driver={{ODBC Driver 17 for SQL Server}};'
            f'Server=tcp:{server},1433;'
            f'Database={database};'
            f'Uid={username};'
            f'Pwd={password};'
            f'Encrypt=yes;'
            f'TrustServerCertificate=no;'
            f'Connection Timeout=30;'
        )
        conn_str = f'mssql+pyodbc:///?odbc_connect={params}'

        # Create the engine
        engine = create_engine(conn_str)
        schema_name = 'REPORTING'
        table_name = 'CUSTOMER_RCP'

        # Push data to Azure Synapse
        batch_size = 100  # Define your batch size
        with engine.begin() as connection:
            for i in range(0, len(data), batch_size):
                batch_data = data.iloc[i:i + batch_size]
                batch_data.to_sql(table_name, con=connection, schema=schema_name, if_exists='append', index=False)
                print(f"Pushed batch {i // batch_size + 1}: Rows {i} to {i + len(batch_data) - 1}")

        print(f"All data pushed successfully to {schema_name}.{table_name} in Azure Synapse.")
    
    
    except Exception as e:
        print(f"Error in data push: {e}")
        raise 

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 4),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'customer_mssql_to_synapse_dag',
    default_args=default_args,
    description='A simple DAG to transfer data from MSSQL to Azure Synapse',
    schedule_interval='@daily',
)

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

push_task = PythonOperator(
    task_id='push_data',
    python_callable=push_data,
    dag=dag,
)

# Set task dependencies
extract_task >> push_task
