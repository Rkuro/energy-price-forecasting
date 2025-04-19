# config.py
from pydantic import BaseModel, Field
import pytimeparse
import humanfriendly
import yaml
import os
import logging
from pathlib import Path
from dotenv import load_dotenv


log = logging.getLogger(__name__)

# Assuming we are running as a module
default_config_path = os.path.join(os.path.join(Path(os.getcwd()), "config", "config.yaml"))

dot_env = load_dotenv()

class GeneralConfig(BaseModel):
    max_disk: str = Field(default="5g", description="Max disk usage, e.g. '10g', '500m'")
    max_ram: str = Field(default="1g", description="Max RAM usage, e.g. '2g', '512m'")
    iso: str = Field(default=["ISO_NE"])

    # Parse the raw strings into bytes (humanfriendly.parse_size returns bytes)
    @property
    def max_disk_bytes(self) -> int:
        return humanfriendly.parse_size(self.max_disk)

    @property
    def max_ram_bytes(self) -> int:
        return humanfriendly.parse_size(self.max_ram)


class DataIngestionConfig(BaseModel):
    enable_weather_data: bool = True
    eia_api_key: str = Field(default=os.environ.get("EIA_API_KEY"))


class TrainingConfig(BaseModel):
    # e.g. '6h', '30m'
    training_interval: str = Field(default="6h")

    @property
    def training_interval_seconds(self) -> int:
        """Convert a string like '6h' or '30m' to total seconds."""
        parsed = pytimeparse.parse(self.training_interval)
        if parsed is None:
            raise ValueError(f"Invalid time interval string '{self.training_interval}'")
        return parsed


class Config(BaseModel):
    general: GeneralConfig = GeneralConfig()
    data_ingestion: DataIngestionConfig = DataIngestionConfig()
    training: TrainingConfig = TrainingConfig()

def load_config(config_path: str = default_config_path) -> Config:
    """
    Load the YAML file, parse into a dict, and validate using Pydantic.
    If fields are missing, Pydantic uses the defaults above.
    """
    log.info(f"config path {config_path}")
    with open(config_path, "r") as f:
        raw_data = yaml.safe_load(f)

    config = Config(**raw_data)

    # This is where we validate at the top level
    validate_config(config)

    return config

def validate_config(config: Config) -> None:
    SUPPORTED_ISOS = [
        "ISO_NE"
    ]

    if config.general.iso not in SUPPORTED_ISOS:
        raise ValueError(f"ISOs must be one of {', '.join(SUPPORTED_ISOS)}")

    if not os.environ.get("EIA_API_KEY"):
        raise ValueError(f"EIA_API_KEY must be set in .env file")


def derive_inference_vector_size(config: Config) -> int:
    pass