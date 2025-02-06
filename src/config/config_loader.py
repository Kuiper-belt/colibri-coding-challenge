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
    Merge the base 'data_path' with file paths in the configuration.
    If a path is already absolute, it is left unchanged.
    """
    if "data_path" not in config:
        logger.debug("No 'data_path' found in config; skipping path merge.")
        return

    data_path = config["data_path"]
    etl_cfg = config.get("etl_config", {})

    def merge_path(short_path: str) -> str:
        """Helper to merge short path with data_path unless it is already absolute."""
        return short_path if os.path.isabs(short_path) else os.path.join(data_path, short_path)

    # Merge path in raw_data
    if "raw_data" in etl_cfg and "read_options" in etl_cfg["raw_data"]:
        short_path = etl_cfg["raw_data"]["read_options"].get("path")
        if short_path:
            etl_cfg["raw_data"]["read_options"]["path"] = merge_path(short_path)
            logger.debug(f"Merged raw_data path: {short_path} -> {etl_cfg['raw_data']['read_options']['path']}")

    # Merge path in ingestion_layer_config
    if "ingestion_layer_config" in etl_cfg and "write_options" in etl_cfg["ingestion_layer_config"]:
        short_path = etl_cfg["ingestion_layer_config"]["write_options"].get("path")
        if short_path:
            etl_cfg["ingestion_layer_config"]["write_options"]["path"] = merge_path(short_path)
            logger.debug(f"Merged ingestion_layer_config path: {short_path} -> {etl_cfg['ingestion_layer_config']['write_options']['path']}")