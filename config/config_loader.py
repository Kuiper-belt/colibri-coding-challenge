import os
import json
import logging

# Configure logging for the loader
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(env=None) -> dict:
    """
    Loads configuration from a JSON file based on the specified or dynamically detected environment.

    Args:
        env (str): The environment to load configuration for (e.g., 'dev', 'prod').
                   Defaults to the value of the APP_ENV environment variable, or 'dev' if not set.

    Returns:
        dict: Configuration dictionary.

    Raises:
        RuntimeError: If the configuration file is missing or invalid.
    """
    # Dynamically detect the environment if not explicitly provided
    env = env or os.getenv("APP_ENV", "dev")  # Default to 'dev' if no environment is specified

    # Construct the path to the environment-specific configuration file
    config_path = os.path.join(os.path.dirname(__file__), f"{env}_config.json")

    try:
        # Read and parse the JSON configuration file
        with open(config_path, "r") as file:
            config = json.load(file)
            logger.info(f"Configuration loaded successfully for environment: {env}")
            return config
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise RuntimeError(f"Configuration file not found: {config_path}")
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON format in configuration file: {config_path}")
        raise RuntimeError(f"Invalid JSON format in configuration file: {config_path}")
