import logging
import os
from datetime import datetime

def setup_logger(name: str, log_level: str = None):
    """Setup logger with consistent formatting"""
    
    if log_level is None:
        log_level = os.getenv('LOG_LEVEL', 'INFO')
    
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level))
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, log_level))
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    ch.setFormatter(formatter)
    
    logger.addHandler(ch)
    
    return logger