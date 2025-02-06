import argparse
import logging
from src.config.config_loader import load_config
from src.jobs.wind_turbines.ingestion_layer import execute as execute_ingestion
from src.jobs.wind_turbines.cleansed_layer import execute as execute_cleansed
from src.jobs.wind_turbines.curated_layer import execute as execute_curated
from src.jobs.wind_turbines.quarantine_layer import execute as execute_quarantine
from src.utils.initialize_database import initialize_database

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """
    Entry point for running specific ETL pipeline steps or database initialization via CLI.
    """
    parser = argparse.ArgumentParser(
        description="Run specific ETL pipeline steps or database initialization."
    )
    parser.add_argument(
        "task",
        choices=[
            "initialize_database",
            "ingestion_layer",
            "cleansed_layer",
            "curated_layer",
            "quarantine_layer",
        ],
        help="The specific task to execute (e.g., initialize_database, ingestion_layer)."
    )
    parser.add_argument(
        "--env",
        default="dev",
        help="The environment to load configurations from (default: dev)."
    )
    parser.add_argument(
        "--start_date",
        type=str,
        help="Start date for filtering data (format: YYYY-MM-DD)."
    )
    parser.add_argument(
        "--end_date",
        type=str,
        help="End date for filtering data (format: YYYY-MM-DD)."
    )

    args = parser.parse_args()

    # Load environment-specific configuration
    logger.info(f"Loading configuration for environment: {args.env}")
    config = load_config(args.env)

    # Handle the requested task
    if args.task == "initialize_database":
        logger.info("Executing database initialization.")
        initialize_database(env=args.env)

    elif args.task == "ingestion_layer":
        logger.info("Executing Ingestion Layer transformation.")
        date_filter_config = {
            "filter_column": "timestamp",
            "start_date": args.start_date,
            "end_date": args.end_date,
        }
        execute_ingestion(date_filter_config=date_filter_config)

    elif args.task == "cleansed_layer":
        logger.info("Executing Cleansed Layer transformation.")
        date_filter_config = {
            "filter_column": "timestamp",
            "start_date": args.start_date,
            "end_date": args.end_date,
        }
        execute_cleansed(date_filter_config=date_filter_config)

    elif args.task == "curated_layer":
        logger.info("Executing Curated Layer transformation.")
        date_filter_config = {
            "filter_column": "timestamp",
            "start_date": args.start_date,
            "end_date": args.end_date,
        }
        execute_curated(date_filter_config=date_filter_config)

    elif args.task == "quarantine_layer":
        logger.info("Executing Quarantine Layer transformation.")
        date_filter_config = {
            "filter_column": "timestamp",
            "start_date": args.start_date,
            "end_date": args.end_date,
        }
        execute_quarantine(date_filter_config=date_filter_config)

    else:
        logger.error(f"Unknown task: {args.task}")
        parser.print_help()

if __name__ == "__main__":
    main()
