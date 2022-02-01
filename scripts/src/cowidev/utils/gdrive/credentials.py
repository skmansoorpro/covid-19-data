import os
import json
from pyaml_env import parse_config

from cowidev.utils import paths


OWID_CONFIG_FILE = os.path.join(str(paths.SCRIPTS), "config.yaml")


def _load_google_credentials():
    config = parse_config(OWID_CONFIG_FILE, raise_if_na=False)
    try:
        # path = config["global"]["credentials"]
        with open(config["global"]["credentials"], "r") as f:
            credentials = json.load(f)
        return credentials["google_credentials"]
    except:
        raise FileNotFoundError("Credentials file not found! Check global.credentials and config file.")


CLIENT_SECRETS_PATH = _load_google_credentials()
CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".config", "owid")
SETTINGS_PATH = os.path.join(CONFIG_PATH, "gdrive_settings.yaml")
CREDENTIALS_PATH = os.path.join(CONFIG_PATH, "gdrive_credentials.json")
