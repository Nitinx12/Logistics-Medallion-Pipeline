import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

# Base directory for the project - repo root (src/utils -> src -> repo)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def get_logger(
    name: str = "app", 
    level: int = logging.INFO, 
    console_level: int | None = None,
    sub_folder: str = "",
    long_running: bool = False
) -> logging.Logger:
    """
    Creates an advanced logger with console and file handlers.
    
    Args:
        name: The name of the logger (usually the module name).
        level: The logging level for the file handler.
        console_level: The logging level for the console (defaults to `level`).
        sub_folder: Creates a sub-directory inside `logs/` for organized logging.
        long_running: If True, rolls files over at midnight instead of by file size.
    """
    # 1. Folder-Level Logging
    log_dir = os.path.join(BASE_DIR, "logs", sub_folder)
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # 2. Advanced Debugging Format (adds File, Line Number, and Function)
    LOG_FORMAT = (
        "%(asctime)s | %(levelname)-8s | %(name)s | "
        "[%(filename)s:%(lineno)d in %(funcName)s] | %(message)s"
    )
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # 3. Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level if console_level is not None else level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 4. File Handler
    if long_running:
        # Better for APIs/Daemons: Rolls over automatically at midnight
        log_filename = f"{name}.log"
        file_handler = TimedRotatingFileHandler(
            os.path.join(log_dir, log_filename),
            when="midnight",
            interval=1,
            backupCount=14, # Keep 14 days of logs
            encoding="utf-8",
        )
    else:
        # Better for Cronjobs/Scripts: Rotates based on file size (5MB)
        today = datetime.now().astimezone().strftime('%Y-%m-%d')
        log_filename = f"{name}_{today}.log"
        file_handler = RotatingFileHandler(
            os.path.join(log_dir, log_filename),
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=5,
            encoding="utf-8",
        )
        
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.propagate = False
    return logger