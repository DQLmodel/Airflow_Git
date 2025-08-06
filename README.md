# Airflow MSSQL to Snowflake ETL Project

This project provides a template for orchestrating data movement from MS SQL Server to Snowflake using Apache Airflow.

## Project Structure

- `dags/` — Place your Airflow DAGs here (sample provided)
- `requirements.txt` — Python dependencies for Airflow and providers
- `README.md` — This file

## Setup Instructions

### 1. Create a Python Virtual Environment (Recommended)
```
python3 -m venv venv
source venv/bin/activate
```

### 2. Install Dependencies
```
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Initialize Airflow Database
```
airflow db init
```

### 4. Create an Admin User
```
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```

### 5. Start Airflow in Standalone Mode
```
airflow standalone
```

- The Airflow UI will be available at http://localhost:8080
- Login with the credentials you set above

### 6. Configure Connections
- Go to the Airflow UI → Admin → Connections
- Add connections for:
  - **MSSQL**: Use `mssql` connection type (provide host, schema, login, password, port)
  - **Snowflake**: Use `snowflake` connection type (provide account, user, password, database, schema, warehouse, role)

### 7. Place Your DAGs
- Put your DAG Python files in the `dags/` folder. A sample DAG is provided for MSSQL to Snowflake transfer.

---

## Notes
- You can run and debug Airflow DAGs directly from PyCharm.
- Make sure your MSSQL and Snowflake credentials are correct and accessible from your environment.# Airflow_Git
# Airflow_Git
