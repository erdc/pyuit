import os
import logging

import yaml
import dodcerts

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_FILE = os.environ.get("UIT_CONFIG_FILE", os.path.join(os.path.expanduser("~"), ".uit"))


def parse_config(config_file):
    try:
        with open(config_file, "r") as f:
            return yaml.safe_load(f)
    except IOError:
        pass  # This config file is rarely used, so ignore errors if it doesn't exist
    except yaml.YAMLError as e:
        logger.error(f"Error while parsing config file '{config_file}': {e}")


# Parse Default Config
DEFAULT_CONFIG = parse_config(DEFAULT_CONFIG_FILE) or {}
DEFAULT_CA_FILE = os.environ.get("UIT_CA_FILE", DEFAULT_CONFIG.get("ca_file", dodcerts.where()))
