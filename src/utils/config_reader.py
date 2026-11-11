import os
from pathlib import Path
import yaml


def _config_dir() -> Path:
    env = os.getenv("CONFIG_DIR")
    if env:
        return Path(env)
    return Path(__file__).resolve().parents[2] / "config"


def load_config(filename: str) -> dict:
    with open(_config_dir() / filename, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)