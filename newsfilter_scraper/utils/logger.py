# Logging utilities and helpers

import logging
import logging.config
import sys
from pathlib import Path
from typing import Optional

from config.logging_config import LoggingConfig


def setup_logger(config_file: Optional[str] = None, environment: str = None) -> logging.Logger:
    """
    Setup logging configuration for the scraper
    
    Args:
        config_file: Optional path to logging config file
        environment: Environment type (development, production, cron)
    
    Returns:
        logging.Logger: Configured root logger
    """
    
    # Create logging configuration
    logging_config = LoggingConfig()
    
    # Apply the configuration
    try:
        logging.config.dictConfig(logging_config.get_config())
        
        # Setup custom handlers that can't be configured via dictConfig
        logging_config.setup_custom_handlers()
        
        # Get the root logger
        logger = logging.getLogger()
        
        # Log system information
        log_system_info()
        
        logger.info("Logging system initialized successfully")
        return logger
        
    except Exception as e:
        # Fallback to basic configuration if setup fails
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s] %(levelname)s [%(name)s:%(lineno)d] %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        
        logger = logging.getLogger()
        logger.error(f"Failed to setup advanced logging, using basic config: {e}")
        return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name
    
    Args:
        name: Logger name (typically __name__)
    
    Returns:
        logging.Logger: Logger instance
    """
    return logging.getLogger(name)


def log_system_info():
    """Log system and environment information at startup"""
    import platform
    import sys
    import os
    
    logger = logging.getLogger(__name__)
    
    logger.info("=== SYSTEM INFORMATION ===")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Platform: {platform.platform()}")
    logger.info(f"Architecture: {platform.architecture()}")
    logger.info(f"Processor: {platform.processor()}")
    logger.info(f"Working directory: {os.getcwd()}")
    logger.info(f"Script: {' '.join(sys.argv)}")
    logger.info("=== SCRAPER STARTUP ===")


def log_exception(logger: logging.Logger, exc: Exception, context: str = ""):
    """
    Log an exception with context
    
    Args:
        logger: Logger instance
        exc: Exception to log
        context: Additional context information
    """
    import traceback
    
    context_msg = f" in {context}" if context else ""
    logger.error(f"Exception{context_msg}: {type(exc).__name__}: {str(exc)}")
    logger.debug(f"Full traceback:\n{traceback.format_exc()}")


def create_stats_logger() -> logging.Logger:
    """
    Create a dedicated statistics logger
    
    Returns:
        logging.Logger: Statistics logger
    """
    stats_logger = logging.getLogger('scraper.stats')
    
    # If no handlers are configured, add a default file handler
    if not stats_logger.handlers:
        handler = logging.FileHandler('logs/scraper_stats.log')
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        handler.setFormatter(formatter)
        stats_logger.addHandler(handler)
        stats_logger.setLevel(logging.INFO)
        stats_logger.propagate = False
    
    return stats_logger


class ContextLogger:
    """Context manager that adds context to log messages"""
    
    def __init__(self, logger: logging.Logger, context: str):
        self.logger = logger
        self.context = context
        self.original_format = None
    
    def __enter__(self):
        # Store original formatter
        for handler in self.logger.handlers:
            if hasattr(handler, 'formatter') and handler.formatter:
                self.original_format = handler.formatter._fmt
                # Add context to format
                new_format = self.original_format.replace('%(message)s', f'[{self.context}] %(message)s')
                handler.setFormatter(logging.Formatter(new_format))
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore original formatter
        if self.original_format:
            for handler in self.logger.handlers:
                if hasattr(handler, 'formatter'):
                    handler.setFormatter(logging.Formatter(self.original_format))


class TimedLogger:
    """Context manager that logs execution time"""
    
    def __init__(self, logger: logging.Logger, operation: str, level: int = logging.INFO):
        self.logger = logger
        self.operation = operation
        self.level = level
        self.start_time = None
    
    def __enter__(self):
        import time
        self.start_time = time.time()
        self.logger.log(self.level, f"Starting {self.operation}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        import time
        duration = time.time() - self.start_time
        
        if exc_type is not None:
            self.logger.log(self.level, f"{self.operation} failed after {duration:.2f}s: {exc_val}")
        else:
            self.logger.log(self.level, f"{self.operation} completed in {duration:.2f}s")


def configure_third_party_loggers():
    """Configure logging levels for third-party libraries"""
    
    # Reduce noise from common libraries
    library_configs = {
        'urllib3.connectionpool': logging.WARNING,
        'requests.packages.urllib3': logging.WARNING,
        'sqlalchemy.engine': logging.WARNING,
        'sqlalchemy.pool': logging.WARNING,
        'PIL': logging.WARNING,
        'boto3': logging.WARNING,
        'botocore': logging.WARNING,
    }
    
    for library, level in library_configs.items():
        logging.getLogger(library).setLevel(level)


# Initialize third-party logger configuration when module is imported
configure_third_party_loggers()