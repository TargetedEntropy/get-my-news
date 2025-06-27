# Track and enforce 100 API calls per 24 hour limit

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any


class RateLimiter:
    """Manages API rate limiting to respect the 100 calls per 24 hour limit"""

    def __init__(
        self,
        max_requests: int = 100,
        tracking_file: str = "data/rate_limit.json",
        reset_hour: int = 0,
    ):
        """
        Initialize the rate limiter

        Args:
            max_requests: Maximum number of requests per 24 hour period
            tracking_file: File to store rate limit tracking data
            reset_hour: Hour of day when the limit resets (0-23)
        """
        self.max_requests = max_requests
        self.tracking_file = Path(tracking_file)
        self.reset_hour = reset_hour

        self.logger = logging.getLogger(__name__)

        # Ensure the tracking file directory exists
        self.tracking_file.parent.mkdir(parents=True, exist_ok=True)

        # Initialize tracking data
        self._load_tracking_data()

    def _load_tracking_data(self):
        """Load rate limit tracking data from file"""
        try:
            if self.tracking_file.exists():
                with open(self.tracking_file, "r") as f:
                    self.data = json.load(f)

                # Validate data structure
                if not isinstance(self.data, dict):
                    self._initialize_tracking_data()
                elif "daily_usage" not in self.data or "last_reset" not in self.data:
                    self._initialize_tracking_data()
                else:
                    # Convert last_reset string back to datetime
                    if self.data["last_reset"]:
                        self.data["last_reset"] = datetime.fromisoformat(
                            self.data["last_reset"]
                        )
            else:
                self._initialize_tracking_data()

        except (json.JSONDecodeError, ValueError, OSError) as e:
            self.logger.warning(
                f"Could not load rate limit data: {e}. Initializing fresh data."
            )
            self._initialize_tracking_data()

        # Check if we need to reset the counter
        self._check_reset_needed()

    def _initialize_tracking_data(self):
        """Initialize fresh tracking data"""
        self.data = {
            "daily_usage": 0,
            "last_reset": self._get_last_reset_time(),
            "max_requests": self.max_requests,
        }
        self._save_tracking_data()

    def _save_tracking_data(self):
        """Save tracking data to file"""
        try:
            # Convert datetime to string for JSON serialization
            data_to_save = self.data.copy()
            if data_to_save["last_reset"]:
                data_to_save["last_reset"] = data_to_save["last_reset"].isoformat()

            with open(self.tracking_file, "w") as f:
                json.dump(data_to_save, f, indent=2)

        except OSError as e:
            self.logger.error(f"Could not save rate limit data: {e}")

    def _get_last_reset_time(self) -> datetime:
        """Get the datetime of the last reset boundary"""
        now = datetime.now()

        # Calculate the most recent reset time
        reset_time = now.replace(
            hour=self.reset_hour, minute=0, second=0, microsecond=0
        )

        # If we haven't passed today's reset time yet, use yesterday's reset time
        if now < reset_time:
            reset_time = reset_time - timedelta(days=1)

        return reset_time

    def _check_reset_needed(self):
        """Check if we need to reset the usage counter"""
        current_reset_time = self._get_last_reset_time()

        if not self.data["last_reset"] or current_reset_time > self.data["last_reset"]:
            old_usage = self.data["daily_usage"]
            self.data["daily_usage"] = 0
            self.data["last_reset"] = current_reset_time
            self._save_tracking_data()

            self.logger.info(
                f"Rate limit counter reset. Previous usage: {old_usage}/{self.max_requests}"
            )

    def can_make_request(self) -> bool:
        """
        Check if we can make an API request without exceeding the rate limit

        Returns:
            bool: True if request is allowed, False otherwise
        """
        self._check_reset_needed()
        return self.data["daily_usage"] < self.max_requests

    def record_request(self):
        """Record that an API request was made"""
        self._check_reset_needed()

        if self.data["daily_usage"] >= self.max_requests:
            self.logger.warning("Attempting to record request when rate limit exceeded")

        self.data["daily_usage"] += 1
        self._save_tracking_data()

        self.logger.debug(
            f"API request recorded. Usage: {self.data['daily_usage']}/{self.max_requests}"
        )

        if self.data["daily_usage"] >= self.max_requests:
            next_reset = self._get_next_reset_time()
            self.logger.warning(f"Rate limit reached! Next reset: {next_reset}")

    def get_current_usage(self) -> Dict[str, Any]:
        """
        Get current usage statistics

        Returns:
            Dict: Current usage information
        """
        self._check_reset_needed()

        next_reset = self._get_next_reset_time()
        time_until_reset = next_reset - datetime.now()

        return {
            "daily_usage": self.data["daily_usage"],
            "max_requests": self.max_requests,
            "remaining": max(0, self.max_requests - self.data["daily_usage"]),
            "percentage_used": (self.data["daily_usage"] / self.max_requests) * 100,
            "last_reset": self.data["last_reset"],
            "next_reset": next_reset,
            "time_until_reset": str(time_until_reset).split(".")[
                0
            ],  # Remove microseconds
            "rate_limited": self.data["daily_usage"] >= self.max_requests,
        }

    def _get_next_reset_time(self) -> datetime:
        """Get the datetime of the next reset"""
        now = datetime.now()
        next_reset = now.replace(
            hour=self.reset_hour, minute=0, second=0, microsecond=0
        )

        # If we've already passed today's reset time, the next reset is tomorrow
        if now >= next_reset:
            next_reset = next_reset + timedelta(days=1)

        return next_reset

    def get_time_until_reset(self) -> timedelta:
        """Get the time remaining until the next reset"""
        return self._get_next_reset_time() - datetime.now()

    def force_reset(self):
        """Force a reset of the rate limit counter (use with caution)"""
        old_usage = self.data["daily_usage"]
        self.data["daily_usage"] = 0
        self.data["last_reset"] = datetime.now()
        self._save_tracking_data()

        self.logger.warning(
            f"Rate limit counter force reset. Previous usage: {old_usage}/{self.max_requests}"
        )

    def simulate_requests(self, count: int) -> Dict[str, Any]:
        """
        Simulate making requests to check impact on rate limit

        Args:
            count: Number of requests to simulate

        Returns:
            Dict: Impact analysis
        """
        current_usage = self.get_current_usage()
        new_usage = current_usage["daily_usage"] + count

        return {
            "current_usage": current_usage["daily_usage"],
            "simulated_requests": count,
            "new_usage": new_usage,
            "would_exceed_limit": new_usage > self.max_requests,
            "remaining_after": max(0, self.max_requests - new_usage),
            "percentage_after": (new_usage / self.max_requests) * 100,
        }
