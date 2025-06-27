# Configuration management - loads .env and provides app settings

import os
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
except ImportError:
    print("python-dotenv is required. Install with: pip install python-dotenv")
    sys.exit(1)


@dataclass
class DatabaseConfig:
    """Database configuration settings"""

    url: str
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    pool_recycle: int = 3600


@dataclass
class APIConfig:
    """API configuration settings"""

    key: str
    base_url: str
    timeout: int = 30
    retry_attempts: int = 3
    retry_backoff: float = 1.0


@dataclass
class RateLimitConfig:
    """Rate limiting configuration"""

    max_daily_requests: int = 100
    tracking_file: str = "data/rate_limit.json"
    reset_hour: int = 0  # Hour of day when limit resets (0-23)


@dataclass
class LoggingConfig:
    """Logging configuration settings"""

    level: str = "INFO"
    file: str = "logs/scraper.log"
    max_size_mb: int = 10
    backup_count: int = 5
    console_output: bool = True
    structured_format: bool = False


class Settings:
    """Main configuration class that loads and validates all settings"""

    def __init__(self, env_file: Optional[str] = None):
        """
        Initialize settings by loading from environment

        Args:
            env_file: Optional path to .env file (defaults to .env in project root)
        """
        self.project_root = Path(__file__).parent.parent
        self.env_file = env_file or self.project_root / ".env"

        # Load environment variables
        self._load_environment()

        # Initialize configuration sections
        self.database = self._load_database_config()
        self.api = self._load_api_config()
        self.rate_limit = self._load_rate_limit_config()
        self.logging = self._load_logging_config()

        # Initialize additional settings
        self.process_lock_file = self._get_env_path(
            "PROCESS_LOCK_FILE", "/tmp/newsfilter_scraper.lock"
        )
        self.data_directory = self._get_env_path("DATA_DIRECTORY", "data")
        self.logs_directory = self._get_env_path("LOGS_DIRECTORY", "logs")

        # Create required directories
        self._ensure_directories()

        # Validate configuration
        self._validate_config()

        # Expose commonly used settings at top level for backward compatibility
        self._setup_legacy_attributes()

    def _load_environment(self):
        """Load environment variables from .env file if it exists"""
        if self.env_file.exists():
            load_dotenv(self.env_file)
            print(f"Loaded environment from: {self.env_file}")
        else:
            print(f"No .env file found at: {self.env_file}")
            print("Using environment variables and defaults")

    def _load_database_config(self) -> DatabaseConfig:
        """Load database configuration"""
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise ValueError("DATABASE_URL environment variable is required")

        return DatabaseConfig(
            url=database_url,
            pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
            max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "10")),
            pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", "30")),
            pool_recycle=int(os.getenv("DB_POOL_RECYCLE", "3600")),
        )

    def _load_api_config(self) -> APIConfig:
        """Load API configuration"""
        api_key = os.getenv("NEWSFILTER_API_KEY")
        if not api_key:
            raise ValueError("NEWSFILTER_API_KEY environment variable is required")

        return APIConfig(
            key=api_key,
            base_url=os.getenv("NEWSFILTER_API_URL", "https://api.newsfilter.io"),
            timeout=int(os.getenv("API_TIMEOUT", "30")),
            retry_attempts=int(os.getenv("API_RETRY_ATTEMPTS", "3")),
            retry_backoff=float(os.getenv("API_RETRY_BACKOFF", "1.0")),
        )

    def _load_rate_limit_config(self) -> RateLimitConfig:
        """Load rate limiting configuration"""
        return RateLimitConfig(
            max_daily_requests=int(os.getenv("MAX_DAILY_REQUESTS", "100")),
            tracking_file=os.getenv("RATE_LIMIT_FILE", "data/rate_limit.json"),
            reset_hour=int(os.getenv("RATE_LIMIT_RESET_HOUR", "0")),
        )

    def _load_logging_config(self) -> LoggingConfig:
        """Load logging configuration"""
        return LoggingConfig(
            level=os.getenv("LOG_LEVEL", "INFO").upper(),
            file=os.getenv("LOG_FILE", "logs/scraper.log"),
            max_size_mb=int(os.getenv("MAX_LOG_SIZE_MB", "10")),
            backup_count=int(os.getenv("LOG_BACKUP_COUNT", "5")),
            console_output=os.getenv("CONSOLE_LOGGING", "true").lower() == "true",
            structured_format=os.getenv("STRUCTURED_LOGGING", "false").lower()
            == "true",
        )

    def _get_env_path(self, env_var: str, default: str) -> Path:
        """Get path from environment variable with default"""
        path_str = os.getenv(env_var, default)
        path = Path(path_str)

        # Make relative paths relative to project root
        if not path.is_absolute():
            path = self.project_root / path

        return path

    def _ensure_directories(self):
        """Create required directories if they don't exist"""
        directories = [
            self.data_directory,
            self.logs_directory,
            Path(self.rate_limit.tracking_file).parent,
            Path(self.logging.file).parent,
        ]

        for directory in directories:
            if directory and not directory.exists():
                directory.mkdir(parents=True, exist_ok=True)
                print(f"Created directory: {directory}")

    def _validate_config(self):
        """Validate the loaded configuration"""
        errors = []

        # Validate database URL format
        if not self.database.url.startswith(
            ("mysql+pymysql://", "mysql://", "sqlite:///")
        ):
            errors.append(
                "DATABASE_URL must start with mysql+pymysql://, mysql://, or sqlite:///"
            )

        # Validate API URL format
        if not self.api.base_url.startswith(("http://", "https://")):
            errors.append("NEWSFILTER_API_URL must start with http:// or https://")

        # Validate numeric ranges
        if not (1 <= self.rate_limit.max_daily_requests <= 1000):
            errors.append("MAX_DAILY_REQUESTS must be between 1 and 1000")

        if not (0 <= self.rate_limit.reset_hour <= 23):
            errors.append("RATE_LIMIT_RESET_HOUR must be between 0 and 23")

        if not (1 <= self.logging.max_size_mb <= 1000):
            errors.append("MAX_LOG_SIZE_MB must be between 1 and 1000")

        if not (1 <= self.logging.backup_count <= 50):
            errors.append("LOG_BACKUP_COUNT must be between 1 and 50")

        # Validate log level
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.logging.level not in valid_log_levels:
            errors.append(f"LOG_LEVEL must be one of: {', '.join(valid_log_levels)}")

        # Validate file permissions
        try:
            # Check if we can write to the data directory
            test_file = self.data_directory / "test_write.tmp"
            test_file.touch()
            test_file.unlink()
        except Exception as e:
            errors.append(f"Cannot write to data directory {self.data_directory}: {e}")

        try:
            # Check if we can write to the logs directory
            test_file = self.logs_directory / "test_write.tmp"
            test_file.touch()
            test_file.unlink()
        except Exception as e:
            errors.append(f"Cannot write to logs directory {self.logs_directory}: {e}")

        if errors:
            raise ValueError(
                f"Configuration validation failed:\n"
                + "\n".join(f"- {error}" for error in errors)
            )

    def _setup_legacy_attributes(self):
        """Setup legacy attributes for backward compatibility with existing code"""
        # Database
        self.DATABASE_URL = self.database.url

        # API
        self.NEWSFILTER_API_KEY = self.api.key
        self.NEWSFILTER_API_URL = self.api.base_url

        # Rate limiting
        self.MAX_DAILY_REQUESTS = self.rate_limit.max_daily_requests
        self.RATE_LIMIT_FILE = str(self.rate_limit.tracking_file)

        # Logging
        self.LOG_LEVEL = self.logging.level
        self.LOG_FILE = self.logging.file

    def get_database_engine_kwargs(self) -> Dict[str, Any]:
        """Get SQLAlchemy engine configuration"""
        return {
            "pool_size": self.database.pool_size,
            "max_overflow": self.database.max_overflow,
            "pool_timeout": self.database.pool_timeout,
            "pool_recycle": self.database.pool_recycle,
            "echo": self.logging.level == "DEBUG",
        }

    def get_api_client_kwargs(self) -> Dict[str, Any]:
        """Get API client configuration"""
        return {
            "timeout": self.api.timeout,
            "retry_attempts": self.api.retry_attempts,
            "retry_backoff": self.api.retry_backoff,
        }

    def is_development(self) -> bool:
        """Check if running in development mode"""
        return os.getenv("ENVIRONMENT", "production").lower() in [
            "development",
            "dev",
            "debug",
        ]

    def is_production(self) -> bool:
        """Check if running in production mode"""
        return not self.is_development()

    def print_config_summary(self):
        """Print a summary of the current configuration (without sensitive data)"""
        print("\n=== CONFIGURATION SUMMARY ===")
        print(
            f"Environment: {'Development' if self.is_development() else 'Production'}"
        )
        print(f"Project Root: {self.project_root}")
        print(f"Config File: {self.env_file}")

        print("\nDatabase:")
        print(f"  URL: {self._mask_credentials(self.database.url)}")
        print(f"  Pool Size: {self.database.pool_size}")
        print(f"  Max Overflow: {self.database.max_overflow}")

        print("\nAPI:")
        print(f"  Base URL: {self.api.base_url}")
        print(f"  Timeout: {self.api.timeout}s")
        print(f"  Retry Attempts: {self.api.retry_attempts}")

        print("\nRate Limiting:")
        print(f"  Max Daily Requests: {self.rate_limit.max_daily_requests}")
        print(f"  Tracking File: {self.rate_limit.tracking_file}")
        print(f"  Reset Hour: {self.rate_limit.reset_hour}:00")

        print("\nLogging:")
        print(f"  Level: {self.logging.level}")
        print(f"  File: {self.logging.file}")
        print(f"  Max Size: {self.logging.max_size_mb}MB")
        print(f"  Console Output: {self.logging.console_output}")

        print("\nDirectories:")
        print(f"  Data: {self.data_directory}")
        print(f"  Logs: {self.logs_directory}")
        print("=" * 30)

    def _mask_credentials(self, url: str) -> str:
        """Mask credentials in database URL for logging"""
        if "://" in url and "@" in url:
            protocol, rest = url.split("://", 1)
            if "@" in rest:
                credentials, host_part = rest.split("@", 1)
                return f"{protocol}://***:***@{host_part}"
        return url


# Global settings instance
_settings_instance: Optional[Settings] = None


def get_settings(reload: bool = False) -> Settings:
    """
    Get the global settings instance (singleton pattern)

    Args:
        reload: Whether to reload settings from environment

    Returns:
        Settings: The settings instance
    """
    global _settings_instance

    if _settings_instance is None or reload:
        _settings_instance = Settings()

    return _settings_instance
