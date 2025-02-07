# colibri-coding-challenge

## Challenge Overview

Consider the following scenario:

You are a data engineer for a renewable energy company that operates a farm of wind turbines. The turbines generate 
power based on wind speed and direction, and their output is measured in megawatts (MW). Your task is to build a data 
processing pipeline that ingests raw data from the turbines and performs the following operations:

- Cleans the data: The raw data contains missing values and outliers, which must be removed or imputed.
- Calculates summary statistics: For each turbine, calculate the minimum, maximum, and average power output over a given time period (e.g., 24 hours).
- Identifies anomalies: Identify any turbines that have significantly deviated from their expected power output over the same time period. Anomalies can be defined as turbines whose output is outside of 2 standard deviations from the mean.
- Stores the processed data: Store the cleaned data and summary statistics in a database for further analysis.

Data is provided to you as CSVs which are appended daily. Due to the way the turbine measurements are set up, each csv 
contains data for a group of 5 turbines. Data for a particular turbine will always be in the same file (e.g. turbine 1 
will always be in data_group_1.csv). Each day the csv will be updated with data from the last 24 hours, however the 
system is known to sometimes miss entries due to sensor malfunctions.

The files provided in the attachment represent a valid set for a month of data recorded from the 15 turbines. Feel 
free to add/remove data from the set provided in order to test/satisfy the requirements above.

Your pipeline should be scalable and testable; emphasis is based on the clarity and quality of the code and the 
implementation of the functionality outlined above, and not on the overall design of the application.

Your solution should be implemented in Python and Pyspark, using any frameworks or libraries that you deem appropriate. 
Please provide a brief description of your solution design and any assumptions made in your implementation.

-----------------------------------------------------------

## Solution Design and Preliminary Assumptions

### Initial Assumptions and Observations

Through an initial inspection of the data and supporting research, I have identified key operational thresholds for 
wind turbines that ensure safe and efficient performance. Wind turbines require a cut-in speed of 6-9 mph (2.5-4 m/s) 
to begin generating power. At wind speeds above 55 mph (25 m/s), known as the cut-out speed, turbines automatically 
shut down to prevent damage. Given this, I do not expect modern wind turbines to generate power at wind speeds 
exceeding 25 m/s, as they are designed to cut out for safety reasons.

In addition to these operational limits, wind direction should always be recorded as an integer value between 0 and 360 
degrees, as this represents a full rotational range. Furthermore, any power output values below 0 should be removed 
from the dataset, as negative power generation is highly unlikely to occur under normal operating conditions.

These initial assumptions form the foundation of my approach, with further details outlined in the solution design 
within this file.

### Data Processing Pipeline

The solution consists of a **pipeline of ETL jobs** that processes data through **five sequential layers**, with 
the last four corresponding to jobs in the `jobs/` directory and tables in the **PostgreSQL** 
`wind_turbines_db` database.

#### **1. Raw Layer**

Contains the raw, unprocessed data as initially received and stored in `data/`.

#### **2. Database Initialization Layer**

The database initialization layer ensures that the required PostgreSQL database and tables exist before ingestion 
begins. This step is executed using the initialize_database.py module, which:

- Loads configuration details, including database and schema definitions.

- Ensures the wind_turbines_db database is created if it does not already exist.

- Creates tables based on the predefined schema.

- Closes the database connection after setup.

This process ensures that the ingestion layer has a valid database structure to write data into.

#### **3. Ingestion Layer**

The **ingestion layer** processes raw wind turbine data by ensuring schema consistency, applying basic transformations, and 
tracking metadata before storing the data in **PostgreSQL**. Its primary function is to enforce data integrity while 
preparing it for further refinement in downstream processing layers.

To achieve schema enforcement, the module uses the `cast_ingestion_schema` function, which casts columns to their 
expected data types and validates required fields. If any necessary columns are missing, an error is raised to 
prevent inconsistencies. Additionally, this function maps certain data types, such as PostgreSQL’s `DOUBLE PRECISION`, 
to PySpark-compatible types like `DOUBLE`, ensuring smooth integration with the processing framework.

Beyond schema validation, the `ingestion_layer_transform` function enhances the dataset by adding a metadata column, 
`metadata_datetime_created`, which tracks when a record was processed. This timestamp is crucial for auditability 
and traceability, allowing analysts to monitor data ingestion timelines.

The `execute` function orchestrates the ETL pipeline by reading raw CSV data, applying transformations, and 
writing the structured data to **PostgreSQL**. It also supports optional **date filtering**, allowing selective data 
processing based on specific time ranges. Although the primary focus is on schema validation, some data 
transformations, such as type casting and timestamp addition, are performed. The ETL function (etl()), 
which is invoked within execute, may also apply additional transformations, such as filtering, depending 
on its internal implementation.

#### **4. Cleansed Layer**

The **cleansed layer** processes wind turbine data from the **ingestion layer**, refining it by performing **data cleaning, 
validation, and deduplication.** It ensures that the data is **accurate, consistent, and ready for downstream analysis** 
by removing missing values, filtering out outliers, and enforcing schema integrity.

The `cleansed_layer.py` module orchestrates the **ETL pipeline** by extracting data from the **ingestion layer**, transforming it 
using predefined rules, and writing it to the **cleansed layer table** in PostgreSQL. The transformation includes **casting 
columns to the correct data types, applying validation rules, filtering out invalid records, and removing duplicates.** 
Additionally, it removes **missing values** by ensuring that all required columns are non-null, and it eliminates **outliers** 
by applying predefined conditions such as valid wind speed ranges (0-25), wind direction limits (0-360), and ensuring 
non-negative power output values.

The cleansed_layer_operations.py module provides utility functions for enforcing schema integrity and validation. The 
cast_cleansed_schema function ensures that columns are correctly typed, while get_conditions builds filtering expressions 
using predefined and dynamic validation rules to remove erroneous data points. Additionally, log_validation_statistics 
tracks and logs the number of records removed during each validation step, providing insight into data 
quality improvements.

This process **cleans, standardises, and validates** the raw ingested data from the **ingestion layer**, making it suitable 
for further processing and analytics.

#### **5. Quarantine Layer**

The **quarantine layer** is responsible for **identifying and isolating invalid records** in the wind turbine data pipeline. 
It ensures that only **valid data progresses** to downstream processing by filtering out records that **fail predefined 
validation conditions and holding them in a separate dataset for review.**

The `quarantine_layer.py` module orchestrates the **ETL pipeline**, extracting data from the **ingestion layer**, applying 
**data validation rules,** and storing **non-compliant records in the quarantine layer table.** The transformation process 
includes **casting columns to the expected schema, filtering invalid records, and removing duplicates** to 
maintain data integrity.

The quarantine layer **holds records with missing values or outliers,** ensuring that potentially problematic data 
is **not discarded outright but instead flagged for review.** The **DEFAULT_CLEANSED_CONDITIONS** define valid data ranges 
for key turbine metrics, and any record that **does not meet these conditions is quarantined.**

**Outlier Rules Applied**

- **turbine_id and timestamp are excluded** from outlier detection, as they are identifiers and not measurements.
- **Wind Speed:** Values **< 0 or > 25** m/s are quarantined.
  - Negative values are **invalid.**
  - The **upper threshold** of 25 m/s assumes wind turbines **shut down** at high speeds for safety.
- **Wind Direction:** Values **< 0 or > 360** degrees are quarantined since this is an **angular measurement** and must fall within a valid range.
- **Power Output:** Values **< 0** are quarantined, as power output **cannot be negative.**
  - No additional **outlier detection** is applied to **power output,** since this metric is analyzed separately in Stage 5 for anomalies.

This **quarantine layer enhances data quality monitoring** by isolating **invalid records, missing values, and outliers,** 
ensuring only **clean, validated data** reaches the **cleansed and curated layers** for further analysis.

#### **6. Curated Layer**

The **curated layer** processes wind turbine data from the **cleansed layer**, refining it for **anomaly detection, and 
analytics.** This stage **aggregates data**, calculates **summary statistics**, and **flags anomalies**, 
ensuring a high-quality dataset for reporting and decision-making.

The `curated_layer.py` module orchestrates the **ETL pipeline**, extracting data from the **cleansed layer**, transforming it, 
and storing the results in the **curated layer table** in PostgreSQL. The transformation process includes **aggregating 
daily statistics (mean, min, max) for each turbine, detecting anomalies in power output based on standard deviation 
thresholds, and restructuring data for efficient analysis.** Anomalies are identified when a turbine's **power output 
deviates by more than two standard deviations from its own daily mean**, ensuring turbine-specific anomaly detection 
without reliance on a fleet-wide benchmark.

The `curated_layer_operations.py` module provides **utility functions** for schema enforcement. The `cast_curated_schema` function 
ensures that all columns conform to the expected schema, handling necessary data type conversions for consistency.

### Database Overview

This project uses a PostgreSQL database for data storage. While both MySQL and PostgreSQL are viable options, 
PostgreSQL provides several advantages that make it the preferred choice. 

Key features such as Common Table Expressions (CTEs), materialized views, and parallel query execution significantly 
enhance performance, particularly in read-heavy and analytical workloads. Additionally, PostgreSQL enforces secure 
connections by default, offering built-in TLS encryption for improved security. 

While MySQL supports replication, PostgreSQL provides more advanced features such as logical replication and 
partitioning, making it better suited for distributed and high-availability environments. Its ability to efficiently 
handle large datasets and execute complex queries further reinforces its suitability for this project.

### Database Tables

#### Ingestion Table

The images below showcase details of the `ingestion_table` in the PostgreSQL database `wind_turbines_db`.

- The first image displays a query that retrieves metadata for each column, including the column name, data type, 
maximum character length (if applicable), numeric precision, and numeric scale.

![Image Alt](https://github.com/Kuiper-belt/colibri-coding-challenge/blob/dbb66361d48c04987e3436bb0b2a41bb9bf98224/imgs/ingestion_table_columns.png)

- The second image presents a query that retrieves the first ten records from the `ingestion_table`, displaying its contents.

![Image Alt](https://github.com/Kuiper-belt/colibri-coding-challenge/blob/dbb66361d48c04987e3436bb0b2a41bb9bf98224/imgs/ingestion_table_limit_10.png)

#### Cleansed Table

The following images provide details of the `cleansed_table` in the `wind_turbines_db` database.

- The first image contains a query that retrieves metadata about each column, including names, data types, 
character lengths, and numeric properties.

![Image Alt](https://github.com/Kuiper-belt/colibri-coding-challenge/blob/dbb66361d48c04987e3436bb0b2a41bb9bf98224/imgs/cleansed_table_columns.png)

- The second image presents a query that extracts the first ten rows from the `cleansed_table`.

![Image Alt](https://github.com/Kuiper-belt/colibri-coding-challenge/blob/dbb66361d48c04987e3436bb0b2a41bb9bf98224/imgs/cleansed_table_limit_10.png)

#### Quarantine Table

The images below highlight the `quarantine_table` in the `wind_turbines_db` database.

- The first image shows a query retrieving metadata about the table’s columns, including data types and constraints.

![Image Alt](https://github.com/Kuiper-belt/colibri-coding-challenge/blob/dbb66361d48c04987e3436bb0b2a41bb9bf98224/imgs/quarantine_table_columns.png)

- The second image presents a query fetching the first ten rows of the `quarantine_table`.

![Image Alt](https://github.com/Kuiper-belt/colibri-coding-challenge/blob/dbb66361d48c04987e3436bb0b2a41bb9bf98224/imgs/quarantine_table_limit_10.png)

#### Curated Table

The images below provide details about the `curated_table` in the `wind_turbines_db` database.

- The first image displays a query extracting column metadata, including names, data types, and size constraints.

![Image Alt](https://github.com/Kuiper-belt/colibri-coding-challenge/blob/dbb66361d48c04987e3436bb0b2a41bb9bf98224/imgs/curated_table_columns.png)

- The second image retrieves the first ten records from the `curated_table`.

![Image Alt](https://github.com/Kuiper-belt/colibri-coding-challenge/blob/dbb66361d48c04987e3436bb0b2a41bb9bf98224/imgs/curated_table_limit_10.png)

### Configuring PostgreSQL Database Connection

To ensure proper connectivity to the PostgreSQL database, you need to modify the configuration files based on the 
type of environment you are working with (**dev**, **test**, or **prod**).

#### **1. Locate the Configuration Files**
Navigate to the following directory in your project:

```plaintext
  colibri-coding-challenge/src/config/
```

This directory contains the configuration files for different environments:

    dev_config.json (Development)
    test_config.json (Testing)
    prod_config.json (Production)

#### **2. Update Database Connection Details**

Open the appropriate configuration file (**dev_config.json**, **test_config.json**, or **prod_config.json**) and 
update the database connection parameters:

#### **Example Configuration Update**

```json
  {
  "database": {
    "pgsql_database": "your_database_name",
    "user": "your_postgres_username",
    "password": "your_postgres_password",
    "host": "localhost",
    "port": 5432
    }
  }
```

#### **3. Save and Apply the Changes**

After updating the file:

- Ensure the database name is correctly assigned to the **"pgsql_database"** argument.
- Provide the correct PostgreSQL user credentials under **"user"** and **"password"**.

#### **4. Verify the Connection**

Run the following command to test the connection to the PostgreSQL database:

```bash
  psql -U your_postgres_username -d your_database_name -h localhost -p 5432
```

If the connection is successful, you should see the PostgreSQL interactive terminal.

### Python and PySpark ETL Modules

The solution is fully implemented using Python and PySpark, with a focus on scalability and testability 
through a modular design.

To support scalability, reusable components have been developed and organised within the `utils/` directory. These 
components are designed to be applicable across multiple jobs and pipelines:

### Module Descriptions

### `ingestion_layer_operations.py`
This module provides utility functions for handling operations in the ingestion layer of the ETL pipeline. The 
ingestion layer is responsible for ingesting and preparing raw data for further processing.

#### Key Features:
- **Schema Enforcement**: Ensures data consistency by enforcing schema integrity.
- **Logging**: Provides detailed logging for debugging.
- **Key Function**: `cast_ingestion_schema` - Casts DataFrame columns to a specified schema, ensuring compatibility with downstream layers.

#### Dependencies:
- PySpark (for DataFrame operations)

### `curated_layer_operations.py`
This module offers utility functions for processing data in the curated layer of the ETL pipeline. The curated layer 
aggregates and refines data from the cleansed layer, ensuring high data quality for analytics.

#### Key Features:
- **Schema Transformation**: Casts DataFrame columns to specified types to maintain consistency.
- **Data Aggregation**: Ensures that data is structured for advanced analytics.

#### Dependencies:
- PySpark (for distributed data processing)
- Logging (for monitoring transformations)

### `cleansed_layer_operations.py`
This module contains functions for performing data transformations and validations in the cleansed layer. The cleansed 
layer ensures that data is refined and clean before being processed further.

#### Key Features:
- **Data Type Casting**: Casts DataFrame columns to specified data types.
- **Filtering Expressions**: Builds expressions for filtering data using predefined conditions.

#### Dependencies:
- PySpark (for data processing)

### `initialize_database.py`
This script initializes a PostgreSQL database and its tables based on configurations defined in the project. It 
leverages the `PostgreSQLManager` utility class to manage database creation and table setup.

#### Key Features:
- **Database Initialization**: Creates and sets up the required database schema.
- **Dynamic Environment Support**: Loads configurations dynamically for different environments (dev, prod, test).
- **Logging**: Provides logs for the database initialization process.

#### Dependencies:
- PostgreSQLManager (for managing database interactions)
- Logging (for debugging and monitoring)
- Configuration Loader (for loading database settings)

### `postgresql_db.py`
This module provides utility functions for interacting with PostgreSQL databases using Python and PySpark. It 
includes dynamic configuration for different environments (e.g., dev, prod, test).

#### Key Features:
- **Dynamic Configuration**: Loads database configurations for different environments.
- **JDBC Connection Management**: Parses JDBC URLs and handles connections.
- **Database Operations**: Provides methods for creating tables, executing queries, and managing transactions.

#### Dependencies:
- PostgreSQL JDBC Driver (for PySpark SQL connectivity)
- Python Database API (for direct database interactions)

### `spark_etl.py`
This module defines a framework for orchestrating ETL (Extract, Transform, Load) processes using Apache Spark. It 
supports dynamic configurations and manages data transformations efficiently.

#### Key Features:
- **ETL Orchestration**: Defines the process for extracting, transforming, and loading data.
- **Flexible Data Handling**: Supports reading and writing data in multiple formats.
- **Configuration Adaptability**: Adapts to different environments (dev, prod, test).

#### Dependencies:
- PySpark (for distributed ETL operations)
- JSON (for configuration management)

### `spark_session_manager.py`
This module provides a centralized class, `SparkSessionManager`, to manage Spark session lifecycles, ensuring efficient 
and consistent Spark usage.

#### Key Features:
- **Singleton Pattern**: Ensures only one instance of SparkSession is created.
- **Environment-Specific Configurations**: Loads configurations dynamically for different environments.
- **Custom Spark Settings**: Applies configurations such as application name, resource allocation, and external JARs.

#### Dependencies:

- PySpark (for Spark session management)

This modular design allows for scalability and reusability across different ETL jobs and pipelines while maintaining 
high code quality and testability.

The `initialize_database.py` **module plays a dual role in the ETL workflow**. It can be executed **manually** before 
running the pipeline to set up the required PostgreSQL database and tables, allowing for independent testing of each layer using 
the `main.py` module and the CLI ('Refer to the section labelled **Evaluating ETL Layers and Modules**.'). However, 
it is also **integrated into the Airflow DAG** (`wind_turbines_dag.py`), ensuring that the database and tables are 
created before any other processing steps can be executed.

The workflow is orchestrated and scheduled using the `wind_turbines_dag.py` Airflow DAG. This DAG **automates** the 
execution of the ETL pipeline, running the **cleansed and quarantine layers in parallel**, as they effectively split 
the data into two streams for validation and anomaly detection.

![Image Alt](https://github.com/Kuiper-belt/colibri-coding-challenge/blob/b093f323e440d9de537f42192c0eab096fc8cb87/imgs/wind_turbines_etl_dag.png)

The process is set to run every day at 23:30. It assumes that the system has been active since the date of the first 
available records and that CSV files will take no longer than 30 minutes to append. Additionally, after a 
built-in single retry, any missed or failed ingestions will be manually re-run (instead of being back-filled) 
so that developers have the opportunity to inspect any errors beforehand.

The solution uses incremental ingestion by applying the `date_filter_config` to filter the timestamp column 
for records received on the current day, and it writes the data in `append` mode to the target table. This date 
filter can be managed directly within the DAG or overridden during a manual trigger. The logic for this process is 
implemented in the `spark_etl.filter_ingestion_period` function.

### Project Structure

**The repository is structured as follows:**

- src/: Contains all the source code and logic for the ETL pipeline, including configurations, DAGs, jobs, utilities, and data files.
  - configs/: Configuration files for different environments (e.g., dev, test, prod) and a config_loader.py script for dynamic configuration loading.
  - dags/: Airflow DAGs, including the primary ETL pipeline (wind_turbines_dag.py).
  - data/: Contains sample CSV data files grouped by turbine sets.
  - jobs/: ETL layers split into ingestion_layer.py, cleansed_layer.py, curated_layer.py, and quarantine_layer.py.
  - utils/: Helper modules for database operations (db_utils.py), Spark session management, ETL processes, and layer-specific operations like cleansed_layer_operations.py and ingestion_layer_operations.py.
  - tests/: Unit and integration tests to ensure code reliability.
- libs/: External dependencies such as the PostgreSQL JDBC driver.
- .gitignore: Specifies files and directories ignored by version control.
- LICENSE: The project's license file.
- README.md: Documentation for understanding and running the project.
- requirements.txt: List of Python dependencies required for the project.

**Detailed overview of the project structure:**

```commandline
colibri-coding-challenge/
│
├── imgs/                        # Images used in the README file
│   ├── ingestion_table_column.png
│   ├── ingestion_table_limit_10.png
│   ├── curated_table_column.png
│   ├── curated_table_limit_10.png
│   ├── quarantine_table_column.png
│   ├── quarantine_limit_10.png
│   ├── cleansed_table_column.png
│   └── cleansed_table_limit_10.png
│
├── libs/                        # External libraries (e.g., JDBC drivers)
│   └── postgresql-42.7.4.jar
│
├── src/                         # Core Python package
│   ├── configs/                 # Configuration files
│   │   ├── __init__.py          # Makes configs a Python package
│   │   ├── config_loader.py     # Configuration loader
│   │   ├── dev_config.json
│   │   ├── prod_config.json
│   │   └── test_config.json
│   │
│   ├── dags/                    # Airflow DAGs
│   │   ├── __init__.py          # Makes dags a Python package
│   │   └── wind_turbines_dag.py # Airflow DAG for wind turbines ETL pipeline
│   │
│   ├── data/                    # Data files for ETL pipeline
│   │   ├── data_group_1.csv
│   │   ├── data_group_2.csv
│   │   └── data_group_3.csv
│   │
│   ├── jobs/                    # ETL pipeline jobs
│   │   ├── __init__.py          # Makes jobs a Python package
│   │   └── wind_turbines/
│   │       ├── __init__.py      # Makes wind_turbines a Python package
│   │       ├── ingestion_layer.py  # Ingestion layer transformation
│   │       ├── curated_layer.py    # Curated layer transformation
│   │       ├── quarantine_layer.py  # Quarantine layer transformation
│   │       └── cleansed_layer.py  # Cleansed layer transformation
│   │
│   ├── tests/                   # Unit and integration tests
│   │   ├── __init__.py                  # Makes tests a Python package
│   │   ├── test_ingestion_layer.py          # Unit tests for ingestion_layer.py
│   │   ├── test_config_loader.py         # Unit tests for config loader
│   │   ├── test_curated_layer.py            # Unit tests for curated_layer.py
│   │   ├── test_initialize_database.py   # Unit tests for database initialization
│   │   ├── test_postgresql_db.py         # Unit tests for PostgreSQLManager
│   │   ├── test_quarantine_layer.py      # Unit tests for quarantine_layer.py
│   │   ├── test_cleansed_layer.py          # Unit tests for cleansed_layer.py
│   │   ├── test_spark_etl.py             # Unit tests for spark_etl.py
│   │   └── test_spark_session_manager.py # Unit tests for spark_session_manager.py
│   │
│   ├── utils/                   # Utility modules
│   │   ├── __init__.py                  # Makes utils a Python package
│   │   ├── ingestion_layer_operations.py   # Ingestion layer utility functions
│   │   ├── curated_layer_operations.py     # Curated layer utility functions
│   │   ├── initialize_database.py       # Database initialization utilities
│   │   ├── postgresql_db.py             # PostgreSQL database interaction
│   │   ├── cleansed_layer_operations.py   # Cleansed layer utility functions
│   │   ├── spark_etl.py                 # Core ETL logic
│   │   └── spark_session_manager.py     # Spark session management
│   │
│   ├── __init__.py              # Makes src a Python package
│   └── main.py                  # CLI for manual testing of ETL layers
│
├── .gitignore                   # Git ignore rules
├── LICENSE                      # Project license
├── README.md                    # Project documentation
└── requirements.txt             # Python dependencies
```

-----------------------------------------------------------

## Executing and validating the solution in a local environment

1. Clone this repository onto your development or testing machine.

2. Follow the instructions in the **System Configuration** section to install PostgreSQL and Apache Spark. Then, use 
the provided guidelines to create a virtual environment for running the data pipeline with Python, PySpark, and Airflow.

3. Manually execute the `initialize_database.py` module to create the PostgreSQL database and tables.

4. This project supports unit testing and individual execution of ETL pipeline components. You can test each module 
separately using the CLI (main.py) or by running unit tests. Refer to the section labelled **Evaluating ETL Layers and Modules**.

5. Set the dags_folder in `~/airflow/airflow.cfg` to the dag directory path.

6. Run the wind_turbines_dag.py DAG using Apache Airflow, you can either trigger it from the Airflow UI or run it via the terminal.

**Note:** If you experience difficulties running Airflow, you can manually execute the jobs by referring to the 
**Evaluating ETL Layers and Modules** section in this file. Use the `main.py` module to run each job individually. 
Ensure that the jobs are executed sequentially in the order defined in the DAG, while parallel tasks can be run in any order.

-----------------------------------------------------------

## Evaluating ETL Layers and Modules

### 1. Executing Unit Tests

To run all unit tests, navigate to the project's root directory and execute:

```bash
  python -m unittest discover -s src/tests
```

If you wish to execute a specific test, select the relevant unit test module from the list below:

```plaintext
  test_config_loader.py
  test_initialize_database.py
  test_spark_etl.py
  test_curated_layer.py
  test_postgresql_db.py
  test_spark_session_manager.py
  test_cleansed_layer.py
  test_ingestion_layer.py
  test_quarantine_layer.py
```

For instance, to run the tests for the `initialize_database.py` module, use the following command:

```bash
  python -m unittest src.tests.test_initialize_database
```

### 2. Running Individual ETL Layers via CLI

Each ETL layer can be tested individually using `main.py`.

General Syntax

```bash
  python -m src.main <module_name> --env <environment> [additional_args]
```

- <module_name>: The ETL module to execute (initialize_database, ingestion_layer, cleansed_layer, curated_layer, quarantine_layer).
- --env <environment>: Specifies the environment (dev, test, prod).
- [additional_args]: Optional arguments for specific modules.

#### 2.1. Initialize Database

Creates the PostgreSQL database and tables.
```bash
  python -m src.main initialize_database --env dev
```

Expected Output:

- Ensures the database exists.
- Creates necessary tables.

#### 2.2. Run Ingestion Layer (with Date Filtering)

Extracts raw data, applies transformations, and writes to the ingestion table.

```bash
  python -m src.main ingestion_layer --env dev --start_date 2022-03-01 --end_date 2025-01-28
```

Expected Output:

- Reads raw data from CSV.
- Filters data based on start_date and end_date.
- Saves transformed data to the Ingestion layer.

#### 2.3. Run Cleansed Layer

Processes ingestion data and applies validation before writing to the cleansed table.

```bash
  python -m src.main cleansed_layer --env dev
```

Expected Output:

- Reads from Ingestion.
- Validates and cleans data.
- Saves cleaned data to the Cleansed layer.

#### 2.4. Run Curated Layer

Aggregates cleansed data and stores business-ready data in the curated table.

```bash
  python -m src.main curated_layer --env dev
```

Expected Output:

- Reads from Cleansed.
- Computes aggregations (e.g., summaries, trends).
- Saves results to the Curated layer.

#### 2.5. Run Quarantine Layer

Handles rejected or invalid records from ingestion and cleansed.

```bash
  python -m src.main quarantine_layer --env dev
```

Expected Output:

- Identifies and logs bad records.
- Moves them to the quarantine table.

### 3. Debugging & Logs

If any errors occur, logs can be found in:

```plaintext
   logs/
   └── etl.log  # Contains execution details
```

To increase verbosity for debugging, run:

```bash
  python -m src.main ingestion_layer --env dev --start_date 2022-03-01 --end_date 2025-01-28 --log_level debug
```

### 4. Running the Full ETL Pipeline via Airflow

If using Airflow, the entire ETL pipeline can be triggered with:

```bash
  airflow dags trigger wind_turbines_dag
```

-----------------------------------------------------------

## SQL Commands for querying the Postgres database and tables

### 1. Check for NULL Records in the `cleansed_table`
```sql
SELECT * 
FROM cleansed_table
WHERE 
    timestamp IS NULL 
    OR turbine_id IS NULL
    OR wind_speed IS NULL
    OR wind_direction IS NULL
    OR power_output IS NULL
    OR metadata_datetime_created IS NULL;
```
- **Function:** Retrieves all records from `cleansed_table` that contain at least one `NULL` value.

### 2. Retrieve Both the Minimum and Maximum Wind Speed
```sql
SELECT 
    MIN(wind_speed) AS min_wind_speed, 
    MAX(wind_speed) AS max_wind_speed
FROM cleansed_table;
```
- **Function:** Fetches the minimum and maximum values of wind speed from the `cleansed_table`.

### 3. Retrieve Both the Minimum and Maximum Wind Direction
```sql
SELECT 
    MIN(wind_direction) AS min_wind_direction, 
    MAX(wind_direction) AS max_wind_direction
FROM cleansed_table;
```
- **Function:** Fetches the minimum and maximum values of wind direction from the `cleansed_table`.

### 4. Retrieve Both the Minimum and Maximum Power Output
```sql
SELECT 
    MIN(power_output) AS min_power_output, 
    MAX(power_output) AS max_power_output
FROM cleansed_table;
```
- **Function:** Retrieves the lowest and highest recorded power output from the `cleansed_table`.

### 5. Count the Number of Anomalous Power Output Days per Turbine
```sql
SELECT 
    turbine_id, 
    COUNT(date) AS anomalous_days
FROM curated_table
WHERE anomalous_power_output = TRUE
GROUP BY turbine_id
ORDER BY anomalous_days DESC;
```
- **Function:** Counts the number of days each turbine had anomalous power output in the `curated_table`, sorting 
them in descending order.

-----------------------------------------------------------

## Future Improvements

To further enhance this project, integration tests could be developed to simulate the processing of wind 
turbine data across various operating systems and cloud environments. I also welcome any further discussion 
or suggestions regarding additional potential improvements to this project.

-----------------------------------------------------------

## System Configuration

This guide provides step-by-step instructions for setting up **Apache Airflow**, **PySpark**, 
and **PostgreSQL** in a **Python virtual environment**.

### Install PostgreSQL on Ubuntu

Follow these steps to install and configure PostgreSQL:

#### 1. Install PostgreSQL

- Update the package list:

```bash
  sudo apt update
```

- Install PostgreSQL along with its client and contrib packages:

```bash
  sudo apt install -y postgresql postgresql-contrib
```

#### 2. Start and Enable PostgreSQL Service

- Start the PostgreSQL service:

```bash
  sudo systemctl start postgresql
```

- Enable PostgreSQL to start on boot:

```bash
  sudo systemctl enable postgresql
```

- Verify the status:

```bash
  sudo systemctl status postgresql
```

#### 3. Configure PostgreSQL

- Switch to the PostgreSQL user:

```bash
  sudo -i -u postgres
```

- Access the PostgreSQL shell:

```bash
  psql
```

- Set a password for the `postgres` user:

```sql
  ALTER USER postgres PASSWORD 'your_password';
```

- Edit the authentication configuration:

```bash
  sudo nano /etc/postgresql/12/main/pg_hba.conf
```

- Change `peer` to `md5` for local authentication:

```plaintext
  local   all             postgres                                md5
```

- Save and exit, then restart PostgreSQL:

```bash
  sudo systemctl restart postgresql
```

- Exit the PostgreSQL shell:

```sql
  \q
```

- Return to your regular user:

```bash
  exit
```

- Verify PostgreSQL installation:

```bash
  psql --version
```

## Install Java (Required for PySpark)

PySpark requires Java. Install OpenJDK if it's not already installed:

```bash
  sudo apt install -y openjdk-11-jdk
```

Verify installation:

```bash
  java -version
```

## Install Python 3.10 (Ubuntu/WSL)

Ensure Python 3.10 is installed:

```bash
  sudo apt update
```

```bash
  sudo apt install -y python3.10
```

To set Python 3.10 as the default version:

```bash
  sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1
```

```bash
  sudo update-alternatives --config python3
```

Verify installation:

```bash
  python3 --version
```

## Setting Up a Python Virtual Environment

### 1. Install Virtualenv

Check if Python is installed:

```bash
  python --version
```

Install virtualenv if not already installed:

```bash
  pip install virtualenv
```

### 2. Create a Virtual Environment

Navigate to your project directory and create a virtual environment:

```bash
  cd <your_project_directory>
```

```bash
  python -m venv airflow-venv
```

### 3. Activate the Virtual Environment

On Linux/macOS:

```bash
  source airflow-venv/bin/activate
```

You should now see (airflow-venv) at the beginning of your terminal prompt.

### 4. Install Required Libraries

```bash
  pip install apache-airflow pyspark psycopg2-binary
```

Save dependencies:

```bash
  pip freeze > requirements.txt
```

To deactivate the virtual environment:

```bash
  deactivate
```

To reactivate later:

```bash
  source airflow-venv/bin/activate
```

## Apache Airflow Setup

### 1. Create and Activate Virtual Environment

Ensure you are using Python 3.10:

```bash
  source airflow-venv/bin/activate
```

### 2. Upgrade Core Python Tools

```bash
  pip install --upgrade pip setuptools wheel
```

### 3. Set Airflow and Python Version Variables

```bash
  AIRFLOW_VERSION=2.10.4
  PYTHON_VERSION=$(python --version | cut -d " " -f 2 | cut -d "." -f 1,2)
  CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

### 4. Install Airflow with Constraints

```bash
  pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

### 5. Configure Airflow

#### a. Set Airflow Home Directory

```bash
  export AIRFLOW_HOME=~/airflow
```

#### b. Configure PostgreSQL Database for Airflow

Edit the airflow.cfg file:

```bash
  nano ~/airflow/airflow.cfg
```

Update the [database] section:

```bash
  [database]
  sql_alchemy_conn = postgresql+psycopg2://postgres:<your_postgres_password>@localhost/airflow
```

### 6. Initialise the Airflow Database

```bash
  airflow db init
```

### 7. Create an Airflow User

```bash
  airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### 8. Start Airflow Services

Start the Scheduler:

```bash
  airflow scheduler
```

Start the Webserver:

```bash
  airflow webserver --port 8080
```

You can access the Airflow UI at: http://localhost:8080

### 9. Running DAGs in Airflow

Once the scheduler and webserver are running, you can manage and execute DAGs from the Airflow UI.

-----------------------------------------------------------