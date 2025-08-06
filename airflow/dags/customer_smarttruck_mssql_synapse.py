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
table = 'CUSTOMER_SMARTTRUCK'

# Define the target columns for the Azure Synapse table
target_columns = [
    'ORG_GRP_CD', 'ORG_GRP_NM', 'SITE_CD', 'EVENT', 'CUSTOMER_NAME', 
    'WASTE_TYPE', 'DISPOSAL_NAME', 'CUST_SRVC_SITE', 'MKT_AREA_NAME', 
    'VEHICLE_ID', 'UPDATED_TIMESTAMP', 'CREATED_TIMESTAMP'
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
        query = '''SELECT TOP 1000
                    s.ORG_GRP_CD,
                    s.ORG_GRP_NM,
                    s.SITE_CD,
                    s.EVENT,
                    s.CUSTOMER_NAME,
                    c.WASTE_TYPE,
                    c.DISPOSAL_NAME,
                    c.CUST_SRVC_SITE,
                    c.MKT_AREA_NAME,
                    c.VEHICLE_ID,
                    c.UPDATED_TIMESTAMP,
                    c.CREATED_TIMESTAMP
                FROM dqlabs.STAGING.SMARTTRUCK_TICKET AS s
                JOIN dqlabs.STAGING.CUSTOMER_RCP AS c
                ON s.SITE_CD = c.SITE_CD
                WHERE s.SITE_CD IS NOT NULL; '''
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
        table_name = 'CUSTOMER_SMARTTRUCK'

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
    'customer_smarttruck_mssql_to_synapse_dag',
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
