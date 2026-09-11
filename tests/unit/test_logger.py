"""tests/unit/test_logger.py"""

import logging
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.logger import get_logger, BASE_DIR


def test_get_logger_creates_handlers(tmp_path):
    # use unique name to avoid singleton cache
    name = "test_logger_unique_1"
    logger = get_logger(name, console_level=logging.WARNING)
    assert isinstance(logger, logging.Logger)
    # should have console + file handler
    assert len(logger.handlers) >= 2
    # second call returns same instance without duplicating handlers
    same = get_logger(name)
    assert same is logger
    assert len(logger.handlers) == len(same.handlers)
    # cleanup handlers to avoid leaking file handles in pytest
    for h in list(logger.handlers):
        logger.removeHandler(h)
        h.close()
    # remove from logging manager
    logging.Logger.manager.loggerDict.pop(name, None)


def test_get_logger_sub_folder(tmp_path):
    name = "test_logger_subfolder"
    # use side_effect to still create the directory under tmp_path so file handler can open
    original_makedirs = __import__("os").makedirs

    def fake_makedirs(path, exist_ok=False):
        # redirect logs/my_sub under tmp_path
        if "my_sub" in path:
            p = Path(tmp_path) / "logs" / "my_sub"
            p.mkdir(parents=True, exist_ok=True)
            return
        return original_makedirs(path, exist_ok=exist_ok)

    with patch("src.utils.logger.os.makedirs", side_effect=fake_makedirs) as mk:
        # patch BASE_DIR to tmp_path so file handler writes there, not repo logs
        with patch("src.utils.logger.BASE_DIR", str(tmp_path)):
            logger = get_logger(name, sub_folder="my_sub")
            assert mk.called
            called_path = mk.call_args[0][0]
            assert "my_sub" in called_path
            for h in list(logger.handlers):
                logger.removeHandler(h)
                try:
                    h.close()
                except Exception:
                    pass
            logging.Logger.manager.loggerDict.pop(name, None)


def test_get_logger_long_running_uses_timed_handler():
    name = "test_logger_long"
    logger = get_logger(name, long_running=True)
    # TimedRotatingFileHandler vs RotatingFileHandler
    from logging.handlers import TimedRotatingFileHandler

    assert any(isinstance(h, TimedRotatingFileHandler) for h in logger.handlers)
    for h in list(logger.handlers):
        logger.removeHandler(h)
        h.close()
    logging.Logger.manager.loggerDict.pop(name, None)


def test_get_logger_console_level_defaults():
    name = "test_logger_console_default"
    logger = get_logger(name, level=logging.INFO)
    # console handler level should equal level when console_level is None
    console_h = next(h for h in logger.handlers if isinstance(h, logging.StreamHandler) and not hasattr(h, "when"))
    assert console_h.level == logging.INFO
    for h in list(logger.handlers):
        logger.removeHandler(h)
        h.close()
    logging.Logger.manager.loggerDict.pop(name, None)
