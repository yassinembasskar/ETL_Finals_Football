# utils/logging_setup.py
import logging
import os

def setup_logger(name: str, log_file: str, level=logging.INFO) -> logging.Logger:
    """
    Create a logger that writes messages to a file.

    Args:
        name (str): Name of the logger (use stage name like 'connection' or 'scraping')
        log_file (str): Path to the log file
        level: Logging level (default INFO)

    Returns:
        logging.Logger: Configured logger
    """
    # Ensure logs folder exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Create logger
    logger = logging.getLogger(name)

    # Avoid adding multiple handlers if logger already exists
    if not logger.handlers:
        # File handler
        handler = logging.FileHandler(log_file)
        # Log message format
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        handler.setFormatter(formatter)
        # Add handler to logger
        logger.addHandler(handler)
        logger.setLevel(level)

    return logger
