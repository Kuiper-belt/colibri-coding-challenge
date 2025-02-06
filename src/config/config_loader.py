import os
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(env=None) -> dict:
    """
    Loads configuration from a JSON file based on the specified or dynamically detected environment,
    merges 'data_path' with short filenames in the ETL config, and returns the final dictionary.

    Args:
        env (str): The environment to load configuration for (e.g., 'dev', 'prod').
                   Defaults to the value of the APP_ENV environment variable, or 'dev' if not set.

    Returns:
        dict: Configuration dictionary.

    Raises:
        RuntimeError: If the configuration file is missing or invalid.
    """
    # Dynamically detect the environment if not explicitly provided
    env = env or os.getenv("APP_ENV", "dev")  # Default to 'dev' if none is specified

    # Construct the path to the environment-specific configuration file
    # e.g., dev_config.json, prod_config.json, etc.
    config_path = os.path.join(os.path.dirname(__file__), f"{env}_config.json")

    try:
        # Read and parse the JSON configuration file
        with open(config_path, "r") as file:
            config = json.load(file)
            logger.info(f"Configuration loaded successfully for environment: {env}")
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise RuntimeError(f"Configuration file not found: {config_path}")
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON format in configuration file: {config_path}")
        raise RuntimeError(f"Invalid JSON format in configuration file: {config_path}")

    # After loading, merge short filenames with the base data_path
    _merge_paths_with_base(config)
    return config

def _merge_paths_with_base(config: dict) -> None:
    """
    Ensures all paths in the configuration are absolute.
    If a path is already absolute, it remains unchanged.
    """
    etl_cfg = config.get("etl_config", {})

    def ensure_absolute_path(path: str) -> str:
        """Ensures the given path is absolute. If it's relative, converts it to an absolute path."""
        return os.path.abspath(path) if path and not os.path.isabs(path) else path

    # Ensure raw_data read path is absolute
    if "raw_data" in etl_cfg and "read_options" in etl_cfg["raw_data"]:
        path = etl_cfg["raw_data"]["read_options"].get("path")
        if path:
            etl_cfg["raw_data"]["read_options"]["path"] = ensure_absolute_path(path)
            logger.debug(f"Updated raw_data path: {path} -> {etl_cfg['raw_data']['read_options']['path']}")

    # Ensure ingestion_layer_config write path is absolute
    if "ingestion_layer_config" in etl_cfg and "write_options" in etl_cfg["ingestion_layer_config"]:
        path = etl_cfg["ingestion_layer_config"]["write_options"].get("path")
        if path:
            etl_cfg["ingestion_layer_config"]["write_options"]["path"] = ensure_absolute_path(path)
            logger.debug(f"Updated ingestion_layer_config path: {path} -> {etl_cfg['ingestion_layer_config']['write_options']['path']}")
