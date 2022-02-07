from dataclasses import dataclass
from pyaml_env import parse_config

from cowidev.utils.paths import SECRETS_FILE


secrets_raw = parse_config(SECRETS_FILE, raise_if_na=False)


@dataclass()
class GoogleSecrets:
    mail: str
    client_secrets: str


@dataclass()
class VaccinationsSecrets:
    post: str
    sheet_id: str


@dataclass()
class TestingSecrets:
    post: str
    sheet_id: str
    sheet_id_attempted: str


@dataclass()
class TwitterSecrets:
    consumer_key: str
    consumer_secret: str
    access_secret: str
    access_token: str


@dataclass()
class Secrets:
    google: GoogleSecrets
    vaccinations: VaccinationsSecrets
    testing: TestingSecrets
    twitter: TwitterSecrets

    def __post_init__(self):
        self.google = GoogleSecrets(**self.google)
        self.vaccinations = VaccinationsSecrets(**self.vaccinations)
        self.testing = TestingSecrets(**self.testing)
        self.twitter = TwitterSecrets(**self.twitter)


# config_raw["global_"] = config_raw.pop("global")
SECRETS = Secrets(**secrets_raw)
