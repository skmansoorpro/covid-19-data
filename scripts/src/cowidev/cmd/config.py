from dataclasses import dataclass
from pyaml_env import parse_config

from cowidev.utils.paths import CONFIG_FILE_NEW as CONFIG_FILE


config_raw = parse_config(CONFIG_FILE, raise_if_na=False)


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
class Base4Config:
    get: TestingGetConfig
    process: dict
    generate: dict
    export: dict


@dataclass()
class TestingConfig(Base4Config):
    # get: TestingGetConfig

    def __post_init__(self):
        self.get = TestingGetConfig(**self.get)


@dataclass()
class VaccinationsConfig(Base4Config):
    pass


@dataclass()
class PipelineConfig:
    testing: TestingConfig
    vaccinations: VaccinationsConfig

    def __post_init__(self):
        self.testing = TestingConfig(**self.testing)
        self.vaccinations = VaccinationsConfig(**self.vaccinations)


@dataclass()
class Config:
    global_: list
    pipeline: PipelineConfig

    def __post_init__(self):
        self.pipeline = PipelineConfig(**self.pipeline)


config_raw["global_"] = config_raw.pop("global")
CONFIG = Config(**config_raw)
