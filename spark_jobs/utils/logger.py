"""Centralized stage scoped logging for FreightLake."""

from __future__ import annotations

import logging
import pathlib

LOG_DIR = pathlib.Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)


def get_logger(name: str, stage: str = "general") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    fh = logging.FileHandler(LOG_DIR / f"{stage}.log")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger
