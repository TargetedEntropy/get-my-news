# Logging configuration setup

import os
import logging
import logging.handlers
from pathlib import Path
from typing import Dict, Any


class LoggingConfig:
    """Centralized logging configuration for the newsfilter scraper"""
    
    def __init__(self):
        self.log_dir = Path("logs")
        self.log_dir.mkdir(exist_ok=True)
        
        # Get configuration from environment variables
        self.log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        self.log_file = os.getenv("LOG_FILE", "logs/scraper.log")
        self.max_log_size = int(os.getenv("MAX_LOG_SIZE_MB", "10")) * 1024 * 1024  # Convert MB to bytes
        self.backup_count = int(os.getenv("LOG_BACKUP_COUNT", "5"))
        self.console_output = os.getenv("CONSOLE_LOGGING", "true").lower() == "true"
        
        # Create the main logger configuration
        self.logger_config = self._create_logger_config()
    
    def _create_logger_config(self) -> Dict[str, Any]:
        """Create the logging configuration dictionary"""
        
        # Define formatters
        formatters = {
            'detailed': {
                'format': '[%(asctime)s] %(levelname)s [%(name)s:%(lineno)d] %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            },
            'simple': {
                'format': '%(levelname)s: %(message)s'
            },
            'stats': {
                'format': '%(message)s'
            }
        }
        
        # Define handlers
        handlers = {
            'file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': self.log_file,
                'maxBytes': self.max_log_size,
                'backupCount': self.backup_count,
                'formatter': 'detailed',
                'level': self.log_level,
                'encoding': 'utf-8'
            }
        }
        
        # Add console handler if enabled
        if self.console_output:
            handlers['console'] = {
                'class': 'logging.StreamHandler',
                'formatter': 'detailed',
                'level': self.log_level,
                'stream': 'ext://sys.stdout'
            }
        
        # Define loggers
        loggers = {
            # Root logger configuration
            '': {
                'level': self.log_level,
                'handlers': list(handlers.keys())
            },
            
            # Specific logger for statistics (always goes to file)
            'stats': {
                'level': 'INFO',
                'handlers': ['file'],
                'propagate': False
            },
            
            # Reduce verbosity for external libraries
            'urllib3': {
                'level': 'WARNING',
                'handlers': list(handlers.keys()),
                'propagate': False
            },
            'requests': {
                'level': 'WARNING', 
                'handlers': list(handlers.keys()),
                'propagate': False
            },
            'sqlalchemy.engine': {
                'level': 'WARNING',
                'handlers': list(handlers.keys()),
                'propagate': False
            }
        }
        
        return {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': formatters,
            'handlers': handlers,
            'loggers': loggers
        }
    
    def get_config(self) -> Dict[str, Any]:
        """Get the complete logging configuration"""
        return self.logger_config