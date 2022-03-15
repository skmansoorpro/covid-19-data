import os

from ._config_loader import countries_mapping

__CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))

# EMRO
__SPRO_CONFIG = os.path.join(__CURRENT_DIR, "spro_config.yaml")
SPRO_COUNTRIES = countries_mapping(__SPRO_CONFIG)
