from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import the layer modules
import src.jobs.wind_turbines.bronze_layer as bronze
import src.jobs.wind_turbines.silver_layer as silver
import src.jobs.wind_turbines.quarantine_layer as quarantine
import src.jobs.wind_turbines.gold_layer as gold

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the date_filter_config function
def date_filter_config():
    return {
        'filter_column': 'timestamp',
        'start_date': '2022-03-01',  # Example start date
        'end_date': datetime.now().strftime('%Y-%m-%d')  # Current date
    }

# Define the DAG
with DAG(
    dag_id='wind_turbines_etl',
    default_args=default_args,
    description='ETL pipeline for wind turbines data processing',
    schedule_interval='@daily',  # Adjust this as needed
    start_date=datetime(2023, 1, 1), # Determines when the DAG is eligible to run
    catchup=False,
) as dag:
    # Task for the Bronze layer
    bronze_wind_turbine = PythonOperator(
        task_id='bronze_layer_etl',
        python_callable=lambda: bronze.execute(date_filter_config()),
    )

    # Task for the Silver layer
    silver_wind_turbine = PythonOperator(
        task_id='silver_layer_etl',
        python_callable=lambda: silver.execute(date_filter_config()),
    )

    # Task for the Quarantine layer
    quarantine_wind_turbine = PythonOperator(
        task_id='quarantine_layer_etl',
        python_callable=lambda: quarantine.execute(date_filter_config()),
    )

    # Task for the Gold layer
    gold_wind_turbine = PythonOperator(
        task_id='gold_layer_etl',
        python_callable=lambda: gold.execute(date_filter_config()),
    )

    # Set task dependencies
    bronze_wind_turbine >> [quarantine_wind_turbine, silver_wind_turbine] >> gold_wind_turbine
