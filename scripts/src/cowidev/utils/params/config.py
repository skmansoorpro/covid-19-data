from dataclasses import dataclass
from pyaml_env import parse_config

from cowidev.utils.paths import CONFIG_FILE


config_raw = parse_config(CONFIG_FILE, raise_if_na=False)


@dataclass()
class VaccinationsProcessConfig:
    skip_complete: list
    skip_monotonic_check: list
    skip_anomaly_check: list


@dataclass()
class BaseGetConfig:
    parallel: bool
    njobs: int
    countries: list
    skip_countries: list


@dataclass()
class TestingGetConfig(BaseGetConfig):
    pass


@dataclass()
class VaccinationsGetConfig(BaseGetConfig):
    pass


@dataclass()
class Base4Config:
    get: BaseGetConfig
    process: dict
    generate: dict
    export: dict


@dataclass()
class TestingConfig(Base4Config):
    get: TestingGetConfig

    def __post_init__(self):
        if self.get["countries"] is None:
            self.get["countries"] = ["all"]
        if self.get["skip_countries"] is None:
            self.get["skip_countries"] = []
        self.get = TestingGetConfig(**self.get)


@dataclass()
class VaccinationsConfig(Base4Config):
    get: VaccinationsGetConfig
    process: VaccinationsProcessConfig

    def __post_init__(self):
        # Get
        if self.get["countries"] is None:
            self.get["countries"] = ["all"]
        if self.get["skip_countries"] is None:
            self.get["skip_countries"] = []
        self.get = VaccinationsGetConfig(**self.get)
        # Process
        if self.process["skip_complete"] is None:
            self.process["skip_complete"] = []
        if self.process["skip_monotonic_check"] is None:
            self.process["skip_monotonic_check"] = {}
        if self.process["skip_anomaly_check"] is None:
            self.process["skip_anomaly_check"] = {}
        self.process = VaccinationsProcessConfig(**self.process)


@dataclass()
class PipelineConfig:
    testing: TestingConfig
    vaccinations: VaccinationsConfig

    def __post_init__(self):
        self.testing = TestingConfig(**self.testing)
        self.vaccinations = VaccinationsConfig(**self.vaccinations)


@dataclass()
class Config:
    pipeline: PipelineConfig

    def __post_init__(self):
        self.pipeline = PipelineConfig(**self.pipeline)


# config_raw["global_"] = config_raw.pop("global")
CONFIG = Config(**config_raw)
