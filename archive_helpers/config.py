"""Read configuration file."""

from __future__ import annotations

import os
import warnings
from configparser import ConfigParser
from dataclasses import dataclass


DEFAULT_RATIO_THRESHOLD = 100
DEFAULT_SIZE_THRESHOLD = 4 * 1024**4
DEFAULT_OBJECT_THRESHOLD = 10**5
CONFIG_PATH = "/etc/archive-helpers/archive-helpers.conf"


@dataclass
class Config:
    max_ratio: int
    max_size: int
    max_objects: int


def get_config(
    config_path: str | bytes | os.PathLike = CONFIG_PATH,
) -> Config:
    config_parser = ConfigParser()

    if config_parser.read(config_path):
        section = config_parser["THRESHOLDS"]
        return Config(
            max_ratio=section.getint("RATIO_THRESHOLD"),
            max_size=section.getint("SIZE_THRESHOLD"),
            max_objects=section.getint("OBJECT_THRESHOLD"),
        )

    warnings.warn(
        f"Configuration file '{config_path}' not found, using defaults.",
        UserWarning,
    )
    return Config(
        max_ratio=DEFAULT_RATIO_THRESHOLD,
        max_size=DEFAULT_SIZE_THRESHOLD,
        max_objects=DEFAULT_OBJECT_THRESHOLD,
    )


CONFIG = get_config()
