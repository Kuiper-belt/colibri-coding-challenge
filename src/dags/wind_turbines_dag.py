from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Import the initialization module
from src.utils.initialize_database import initialize_database

# Import the layer modules
import src.jobs.wind_turbines.ingestion_layer as ingestion
import src.jobs.wind_turbines.cleansed_layer as cleansed
import src.jobs.wind_turbines.quarantine_layer as quarantine
import src.jobs.wind_turbines.curated_layer as curated

# Import the configuration loader
from src.config.config_loader import load_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
env = "dev"  # Change this if needed
logger.info(f"Loading configuration for environment: {env}")
config = load_config(env)

# Get values from config
default_args = {
    'owner': config.get("etl_config", {}).get("dag", {}).get("owner", "airflow"),
    'depends_on_past': config.get("etl_config", {}).get("dag", {}).get("depends_on_past", False),
    'retries': config.get("etl_config", {}).get("dag", {}).get("retries", 1),
    'retry_delay': timedelta(minutes=5),
}

start_date_str = config.get("etl_config", {}).get("date_filter", {}).get("start_date", "2022-03-01")
start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

filter_column = config.get("etl_config", {}).get("date_filter", {}).get("filter_column", "timestamp")

# Define the date_filter_config function
def date_filter_config():
    return {
        'filter_column': filter_column,  # Use configured filter_column
        'start_date': start_date_str,  # Use configured start_date
        'end_date': datetime.now().strftime('%Y-%m-%d')
    }

# Define the DAG
with DAG(
    dag_id='wind_turbines_etl',
    default_args=default_args,
    description='ETL pipeline for wind turbines data processing',
    schedule_interval='30 23 * * *',  # Adjust this as needed
    start_date=start_date,  # Use the dynamically loaded start_date
    catchup=False,
) as dag:

    # Task Initialize Database
    initialize_db = PythonOperator(
        task_id='initialize_database',
        python_callable=lambda: initialize_database(env),
    )

    # Task for the ingestion layer
    ingestion_wind_turbine = PythonOperator(
        task_id='ingestion_layer_etl',
        python_callable=lambda: ingestion.execute(date_filter_config()),
    )

    # Task for the Cleansed layer
    cleansed_wind_turbine = PythonOperator(
        task_id='cleansed_layer_etl',
        python_callable=lambda: cleansed.execute(date_filter_config()),
    )

    # Task for the Quarantine layer
    quarantine_wind_turbine = PythonOperator(
        task_id='quarantine_layer_etl',
        python_callable=lambda: quarantine.execute(date_filter_config()),
    )

    # Task for the Curated layer
    curated_wind_turbine = PythonOperator(
        task_id='curated_layer_etl',
        python_callable=lambda: curated.execute(date_filter_config()),
    )

    # Set task dependencies
    initialize_db >> ingestion_wind_turbine  # Ensure DB is ready before ingestion
    ingestion_wind_turbine >> [quarantine_wind_turbine, cleansed_wind_turbine] >> curated_wind_turbine
