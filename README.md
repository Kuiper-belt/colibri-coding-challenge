# colibri-coding-challenge

## Challenge Overview

Consider the following scenario:

You are a data engineer for a renewable energy company that operates a farm of wind turbines. The turbines generate power based on wind speed and direction, and their output is measured in megawatts (MW). Your task is to build a data processing pipeline that ingests raw data from the turbines and performs the following operations:

- Cleans the data: The raw data contains missing values and outliers, which must be removed or imputed.
- Calculates summary statistics: For each turbine, calculate the minimum, maximum, and average power output over a given time period (e.g., 24 hours).
- Identifies anomalies: Identify any turbines that have significantly deviated from their expected power output over the same time period. Anomalies can be defined as turbines whose output is outside of 2 standard deviations from the mean.
- Stores the processed data: Store the cleaned data and summary statistics in a database for further analysis.

Data is provided to you as CSVs which are appended daily. Due to the way the turbine measurements are set up, each csv contains data for a group of 5 turbines. Data for a particular turbine will always be in the same file (e.g. turbine 1 will always be in data_group_1.csv). Each day the csv will be updated with data from the last 24 hours, however the system is known to sometimes miss entries due to sensor malfunctions.

The files provided in the attachment represent a valid set for a month of data recorded from the 15 turbines. Feel free to add/remove data from the set provided in order to test/satisfy the requirements above.

Your pipeline should be scalable and testable; emphasis is based on the clarity and quality of the code and the implementation of the functionality outlined above, and not on the overall design of the application.

Your solution should be implemented in Python and Pyspark, using any frameworks or libraries that you deem appropriate. Please provide a brief description of your solution design and any assumptions made in your implementation.

---
## Solution Design

**More to add here**

For this project, data is stored in a PostgreSQL database. While both MySQL and PostgreSQL are viable options for this 
application, PostgreSQL offers several advantages that make it a superior choice. Features such as Common Table 
Expressions (CTEs), materialized views, and parallel query execution enhance its performance in read-heavy and 
analytical workloads. PostgreSQL also enables secure connections by default, providing TLS encryption out-of-the-box. 
Additionally, while MySQL supports replication, PostgreSQL's logical replication and advanced partitioning capabilities 
are better suited for distributed and high-availability environments. Its ability to handle larger datasets and execute 
complex queries more efficiently further solidifies PostgreSQL as the preferred choice.

**The repository is structured as follows:**

- src/: Contains all the source code and logic for the ETL pipeline, including configurations, DAGs, jobs, utilities, and data files.
  - configs/: Configuration files for different environments (e.g., dev, test, prod) and a config_loader.py script for dynamic configuration loading.
  - dags/: Airflow DAGs, including the primary ETL pipeline (wind_turbines_dag.py).
  - data/: Contains sample CSV data files grouped by turbine sets.
  - jobs/: ETL layers split into bronze_layer.py, silver_layer.py, gold_layer.py, and quarantine_layer.py.
  - utils/: Helper modules for database operations (db_utils.py), Spark session management, ETL processes, and layer-specific operations like silver_layer_operations.py and bronze_layer_operations.py.
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
│   │       ├── bronze_layer.py  # Bronze layer transformation
│   │       ├── gold_layer.py    # Gold layer transformation
│   │       ├── quarantine_layer.py  # Quarantine layer transformation
│   │       └── silver_layer.py  # Silver layer transformation
│   │
│   ├── tests/                   # Unit and integration tests
│   │   ├── __init__.py          # Makes tests a Python package
│   │   └── (test modules)       # Placeholder for test scripts
│   │
│   ├── utils/                   # Utility modules
│   │   ├── __init__.py          # Makes utils a Python package
│   │   ├── bronze_layer_operations.py # Bronze layer utility functions
│   │   ├── db_utils.py          # Database utilities
│   │   ├── gold_layer_operations.py # Gold layer utility functions
│   │   ├── silver_layer_operations.py # Silver layer utility functions
│   │   ├── spark_etl.py         # Core ETL logic
│   │   └── spark_session_manager.py # Spark session management
│   │
│   ├── __init__.py              # Makes src a Python package
│   └── main.py                  # CLI for manual testing of ETL layers
│
├── .gitignore                   # Git ignore rules
├── LICENSE                      # Project license
├── README.md                    # Project documentation
└── requirements.txt             # Python dependencies
```

## System Configuration

### Install PostgreSQL on Ubuntu, follow these steps:

- Update the package list:
    ```bash
    sudo apt update
    ``` 
- Install PostgreSQL along with its client and contrib packages:
    ```bash
    sudo apt install -y postgresql postgresql-contrib
    ````
- Start the PostgreSQL service:
    ```bash
    sudo systemctl start postgresql or sudo service postgresql start ?????? look into which one
    ```
- You can also check its status:
    ```bash
    sudo service postgresql status
    ```
- Enable PostgreSQL to start on boot:
    ```bash
    sudo systemctl enable postgresql
    ```
- Switch to the PostgreSQL user (postgres):
    ```bash
    sudo -i -u postgres
    ```
- Access the PostgreSQL prompt:
    ```bash
    psql
    ```
- You should now see the PostgreSQL prompt:
    ```makefile
    postgres=#
    ```
- Inside the psql shell, set a password for the postgres user:
    ```sql
    ALTER USER postgres PASSWORD 'your_password';
    ```
- Edit the PostgreSQL configuration file to enable password authentication:
    ```bash
    sudo nano /etc/postgresql/12/main/pg_hba.conf
    ```
- Change peer to md5 for the line referencing local:  
    ```plaintext
    local   all             postgres                                md5
    ```
- Save and exit, then restart the PostgreSQL service:
    ```bash
    sudo service postgresql restart
    ```
- Exit the PostgreSQL prompt:
    ```sql
    \q
    ```
- Return to your regular user:
    ```bash
    exit
    ```
- Verify the PostgreSQL installation:
    ```bash
  psql --version
    ```
- This will show the installed PostgreSQL version. For example:
    ```scss
    psql (PostgreSQL) 14.x
    ```

### Install Java on Ubuntu (WSL)




Add instructions here!!!

PySpark requires Java to run. You can install OpenJDK if it's not already installed:

sudo apt install -y openjdk-11-jdk

You can verify the Java installation with:

java -version





### Install Python 3.10 on Ubuntu (WSL)

If you are on Ubuntu in WSL and need Python 3.10, follow these steps:

- Update the Package List:
    ```bash
    sudo apt update
    ```
- Check if Python 3.10 is Available:
    ```bash
    sudo apt install -y python3.10
    ```
- Set Python 3.10 as Default (Optional): If multiple Python versions are installed and you want to set Python 3.10 
as the default, you can update the alternatives system:
    ```bash
    sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1
    sudo update-alternatives --config python3
    ```
- Verify the Installation:
    ```bash
    python3 --version
    ```

### Follow these steps to set up a Python virtual environment for your project.

?????? python -m venv airflow-venv??????

### 1. Install Python and Virtualenv

1. **Check if Python is Installed**:
   ```bash
   python --version
   ```

2. **Install Virtualenv** (if not already installed):
   ```bash
   pip install virtualenv
   ```

3. **Create the Virtual Environment**:
   Navigate to your project directory and create the virtual environment:
   ```bash
   cd <your_project_directory>
   python -m venv venv
   ```

---

### 2. Create the Project Directory

1. Create a new directory for your project (if not already created):
   ```bash
   mkdir <project_name>
   cd <project_name>
   ```

---

### 3. Create a Virtual Environment

1. Use `virtualenv` to create the virtual environment:
   ```bash
   python -m venv venv
   ```

   This will create a directory named `venv` in your project folder.

---

### 4. Activate the Virtual Environment

#### Linux/macOS:
```bash
source venv/bin/activate
```

#### Windows:
```bash
venv\Scripts\activate
```

You should now see `(venv)` at the beginning of your terminal prompt.

---

### 5. Install Required Libraries

1. Install the libraries needed for your project:
   ```bash
   pip install apache-airflow pyspark
   ```

2. Save the dependencies to a `requirements.txt` file:
   ```bash
   pip freeze > requirements.txt
   ```

---

### 6. Deactivate the Virtual Environment

When you are done working in the virtual environment, deactivate it:
```bash
deactivate
```

---

### 7. Reactivate the Virtual Environment

To reactivate the virtual environment later, navigate to your project directory and run the activation command again:

#### Linux/macOS:
```bash
source venv/bin/activate
```

#### Windows:
```bash
venv\Scripts\activate
```

----------------------------------------------------------------------------------------------

### Steps to Run Airflow Using venv
- Refer to the instructions above to create a Virtual Environment
   ```bash
    source airflow-venv/bin/activate
   ```


2. Install Airflow

Install the required Airflow packages in your virtual environment. Use the constraints file specific to the Airflow version you're installing to ensure compatibility.

pip install apache-airflow==2.7.2 \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.2/constraints-3.8.txt"

Replace 2.7.2 and 3.8 with your desired Airflow version and Python version.
3. Initialize the Airflow Database

Initialize the Airflow database, which is required for tracking DAG runs and metadata.

airflow db init

4. Configure Airflow

Airflow uses the ~/airflow/airflow.cfg file for configuration. Adjust settings as needed.

For Windows users, ensure you set the AIRFLOW_HOME environment variable to a folder where Airflow will store its configuration and logs. For example:

set AIRFLOW_HOME=E:\path_to\airflow_home

For Linux/Mac:

export AIRFLOW_HOME=~/airflow

5. Start the Airflow Scheduler and Webserver

Start the scheduler and webserver to execute and monitor your DAGs.

    Start the Scheduler:

airflow scheduler

Start the Webserver:

    airflow webserver

    Access the Airflow UI at http://localhost:8080.

6. Running DAGs

Once the scheduler and webserver are running, you can manage and execute your DAGs from the Airflow UI. Place your DAG files in the dags/ folder (default is ~/airflow/dags/).




















Installing Airflow in a Virtual Environment

Here’s how you can set up Airflow in a venv:
1. Create a Virtual Environment

python3 -m venv airflow-env
source airflow-env/bin/activate

2. Upgrade pip, setuptools, and wheel

pip install --upgrade pip setuptools wheel

3. Install Airflow

Airflow requires specifying an installation constraint file:

AIRFLOW_VERSION=2.7.3
PYTHON_VERSION=$(python --version | cut -d " " -f 2 | cut -d "." -f 1,2)
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

4. Initialize Airflow

airflow db init

5. Set Up Airflow User

Create an admin user for the Airflow web interface:

airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

6. Run Airflow

Start the scheduler and webserver:

    Scheduler:

airflow scheduler

Webserver:

airflow webserver --port 8080


















Steps to Install Apache Airflow 2.10.4
1. Create and Activate a Virtual Environment

This isolates Airflow and its dependencies from the system Python.

python3 -m venv airflow-env
source airflow-env/bin/activate

2. Upgrade Core Python Tools

Ensure your virtual environment has the latest versions of pip, setuptools, and wheel.

pip install --upgrade pip setuptools wheel

3. Set Airflow and Python Version Variables

These variables help dynamically select the appropriate constraint file.

AIRFLOW_VERSION=2.10.4
PYTHON_VERSION=$(python --version | cut -d " " -f 2 | cut -d "." -f 1,2)
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

4. Install Airflow with Constraints

Install Airflow along with the constraints file for the specified version.

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

5. Initialize Airflow Database

After installation, initialize the database that Airflow uses to track its metadata.

airflow db init

6. Create an Admin User

Create a user for accessing the Airflow web interface.

airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

7. Start Airflow Components

Start the Airflow scheduler and webserver:

    Scheduler (runs tasks):

airflow scheduler

Webserver (UI for managing workflows):

    airflow webserver --port 8080

Access the web interface at: http://localhost:8080







Step 2: Update airflow.cfg

    Locate the airflow.cfg file. By default, it’s in your AIRFLOW_HOME directory:

~/airflow/airflow.cfg

Open the file in an editor:

nano ~/airflow/airflow.cfg

Look for the [database] section and update the sql_alchemy_conn parameter. Use the following format:

[database]
sql_alchemy_conn = postgresql+psycopg2://postgres:<your_postgres_password>@localhost/airflow

Step 3: Install PostgreSQL Dependencies for Airflow

Install the required PostgreSQL client library for Python:

pip install psycopg2-binary






Step 2: Celery Configuration

The CeleryExecutor requires a message broker (such as Redis or RabbitMQ) and a result backend. Let’s set these up.
Install Redis

    Install Redis (as a broker):

sudo apt update
sudo apt install redis

Start and enable Redis:

    sudo service redis-server start
    sudo systemctl enable redis

Configure Redis in airflow.cfg

    In your airflow.cfg file, configure the Celery broker URL:

    [celery]
    broker_url = redis://localhost:6379/0
    result_backend = db+postgresql://postgres:AES256bam_bam@localhost:5432/wind_turbines_db

    Save and close the file.

Step 3: Install Required Python Packages

Install the Python dependencies for PostgreSQL, Redis, and Celery:

pip install psycopg2-binary redis celery

Step 4: Initialize the Airflow Database

Reinitialize the database to reflect the new configuration:

airflow db init





Step 1: Verify Redis Status

Check if Redis is running correctly:

sudo service redis-server status

    If it says "Active: active (running)", Redis is functioning, and no further action is needed.
    If it’s not running, try starting it:

    sudo service redis-server start

Step 2: Manually Configure Redis to Start at Boot

Since systemctl might not fully work in WSL, you can manually configure Redis to start when WSL launches:

    Open or create the WSL profile script:

nano ~/.bashrc

Add the following line at the end to start Redis automatically:

sudo service redis-server start

Save and close the file (Ctrl+O, Enter, Ctrl+X).

Reload the .bashrc file:

    source ~/.bashrc

Step 3: Verify Redis Functionality

Ensure Redis is running by testing with the Redis CLI:

redis-cli ping

If Redis responds with PONG, it’s working correctly.




Redis is successfully running! 🎉

The redis-server.service is active and ready to accept connections, so you're good to proceed with configuring Airflow to use Redis as the message broker.
Verify Redis Connectivity

You can double-check Redis functionality with the following:

redis-cli ping

If Redis responds with PONG, it's working perfectly.
Next Steps: Configure Airflow to Use Redis

    Open the airflow.cfg file:

nano ~/airflow/airflow.cfg

In the [celery] section, ensure the following lines are set:

[celery]
broker_url = redis://localhost:6379/0
result_backend = db+postgresql://postgres:AES256bam_bam@localhost:5432/wind_turbines_db

Save and close the file.





Yes, using a config.json approach can support working in a cloud environment, provided you design your configuration file and application to be flexible and environment-agnostic. Here's how it aligns with cloud-based workflows:
Benefits of Using config.json in a Cloud Environment

    Environment-Specific Configuration:
        You can maintain separate configuration files for different environments (e.g., dev_config.json, test_config.json, prod_config.json).
        In a cloud deployment, you can specify which configuration to use based on environment variables or runtime parameters.

    Centralized Management:
        All critical parameters (e.g., database credentials, Spark settings, file paths) are stored in one place, making it easier to manage and update configurations.

    Flexibility:
        A config.json file can be loaded dynamically, allowing the same application code to work across different environments (local, cloud, staging, production) by simply switching the configuration file.

    Cloud Integration:
        The configuration file can store:
            Cloud storage paths (e.g., s3://bucket-name/, gs://bucket-name/).
            Cloud database connection strings (e.g., Amazon RDS, Google Cloud SQL, Azure SQL).
            Spark cluster configurations (e.g., AWS EMR, Google Dataproc).

    Security and Separation of Concerns:
        Sensitive information like database credentials can be externalized from the application code, which is a best practice for secure deployments.
        For cloud environments, you can also integrate with secrets managers (e.g., AWS Secrets Manager, Azure Key Vault, GCP Secret Manager) and use the config.json file to store references to these secrets instead of hardcoding sensitive data.

dev_config.json

    Purpose: For development purposes, used locally by developers during the coding and debugging phase.

    Characteristics:
        Uses local or minimal resources.
        Paths point to local directories.
        Credentials and data are often for non-sensitive, local development.
        Spark executor memory and partitions are set low to optimize resource usage during testing.
        Database may be a local instance or an isolated development server.

test_config.json

    Purpose: For testing purposes, used by automated test suites or QA teams to verify the correctness of the application.

    Characteristics:
        Mimics the production environment more closely than the development configuration.
        Paths and credentials point to a testing or staging environment.
        Larger resources allocated compared to development to simulate real-world conditions.
        Data might be a subset of production data or anonymized copies.

prod_config.json

    Purpose: For production purposes, used when the application is deployed in a live environment.

    Characteristics:
        Uses the actual production database and resources.
        Paths point to production directories or cloud storage (e.g., S3, Azure Blob Storage).
        Credentials and data are for production.
        Maximum resources allocated to ensure optimal performance.
        Includes configurations for cloud services, logging, and monitoring.

