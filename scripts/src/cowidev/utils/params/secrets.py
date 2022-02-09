from dataclasses import dataclass, field
from pyaml_env import parse_config

from cowidev.utils.paths import SECRETS_FILE


secrets_raw = parse_config(SECRETS_FILE, raise_if_na=False)


@dataclass()
class GoogleSecrets:
    client_secrets: str
    mail: str = None


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
    consumer_key: str = None
    consumer_secret: str = None
    access_secret: str = None
    access_token: str = None


@dataclass()
class Secrets:
    google: GoogleSecrets
    vaccinations: VaccinationsSecrets = field(default_factory=dict)
    testing: TestingSecrets = field(default_factory=dict)
    twitter: TwitterSecrets = field(default_factory=dict)

    def __post_init__(self):
        self.google = GoogleSecrets(**self.google)
        self.vaccinations = VaccinationsSecrets(**self.vaccinations)
        self.testing = TestingSecrets(**self.testing)
        self.twitter = TwitterSecrets(**self.twitter)


# config_raw["global_"] = config_raw.pop("global")
SECRETS = Secrets(**secrets_raw)
