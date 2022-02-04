import os
import json
from pyaml_env import parse_config

from cowidev.utils import paths


def _load_google_credentials():
    config = parse_config(paths.CONFIG_FILE, raise_if_na=False)
    try:
        # path = config["global"]["credentials"]
        with open(config["global"]["credentials"], "r") as f:
            credentials = json.load(f)
        return credentials["google_credentials"]
    except:
        raise FileNotFoundError("Credentials file not found! Check global.credentials and config file.")


CLIENT_SECRETS_PATH = _load_google_credentials()
SETTINGS_PATH = os.path.join(paths.CONFIG_DIR, "gdrive_settings.yaml")
CREDENTIALS_PATH = os.path.join(paths.CONFIG_DIR, "gdrive_credentials.json")
