# Statistics collection and reporting for scraper runs

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict


@dataclass
class RunStats:
    """Statistics for a single scraper run"""
    run_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    success: bool = False
    
    # Article statistics
    articles_fetched: int = 0
    articles_processed: int = 0
    articles_duplicate: int = 0
    articles_failed: int = 0
    
    # Entity statistics
    sources_created: int = 0
    symbols_created: int = 0
    industries_created: int = 0
    sectors_created: int = 0
    
    # API statistics
    api_calls_made: int = 0
    api_rate_limit_remaining: int = 0
    
    # Error information
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
    
    @property
    def duration(self) -> Optional[timedelta]:
        """Get the duration of the run"""
        if self.end_time and self.start_time:
            return self.end_time - self.start_time
        return None
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate for article processing"""
        total = self.articles_processed + self.articles_failed
        if total == 0:
            return 100.0
        return (self.articles_processed / total) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        data = asdict(self)
        # Convert datetime objects to ISO strings
        if self.start_time:
            data['start_time'] = self.start_time.isoformat()
        if self.end_time:
            data['end_time'] = self.end_time.isoformat()
        if self.duration:
            data['duration_seconds'] = self.duration.total_seconds()
        return data


class ScraperStats:
    """Manages statistics collection and reporting for scraper runs"""
    
    def __init__(self, stats_file: str = "data/scraper_stats.json"):
        """
        Initialize the stats manager
        
        Args:
            stats_file: File to store historical statistics
        """
        self.stats_file = Path(stats_file)
        self.logger = logging.getLogger(__name__)
        
        # Ensure stats directory exists
        self.stats_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Current run statistics
        self.reset_current_stats()
        
        # Historical statistics
        self.historical_stats = self._load_historical_stats()
    
    def reset_current_stats(self):
        """Reset statistics for current run"""
        self.articles_fetched = 0
        self.articles_processed = 0
        self.articles_duplicate = 0
        self.articles_failed = 0
        self.sources_created = 0
        self.symbols_created = 0
        self.industries_created = 0
        self.sectors_created = 0
        self.api_calls_made = 0
        self.errors = []
    
    def _load_historical_stats(self) -> List[Dict[str, Any]]:
        """Load historical statistics from file"""
        try:
            if self.stats_file.exists():
                with open(self.stats_file, 'r') as f:
                    return json.load(f)
            return []
        except (json.JSONDecodeError, OSError) as e:
            self.logger.warning(f"Could not load historical stats: {e}")
            return []
    
    def _save_historical_stats(self):
        """Save historical statistics to file"""
        try:
            with open(self.stats_file, 'w') as f:
                json.dump(self.historical_stats, f, indent=2, default=str)
        except OSError as e:
            self.logger.error(f"Could not save historical stats: {e}")
    
    def create_run_stats(self, run_id: str = None) -> RunStats:
        """
        Create a RunStats object for the current run
        
        Args:
            run_id: Optional run identifier (defaults to timestamp)
        
        Returns:
            RunStats: Statistics object for current run
        """
        if run_id is None:
            run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        return RunStats(
            run_id=run_id,
            start_time=datetime.now(),
            articles_fetched=self.articles_fetched,
            articles_processed=self.articles_processed,
            articles_duplicate=self.articles_duplicate,
            articles_failed=self.articles_failed,
            sources_created=self.sources_created,
            symbols_created=self.symbols_created,
            industries_created=self.industries_created,
            sectors_created=self.sectors_created,
            api_calls_made=self.api_calls_made,
            errors=self.errors.copy()
        )
    
    def finish_run_stats(self, run_stats: RunStats, success: bool):
        """
        Finalize run statistics and save to history
        
        Args:
            run_stats: The run statistics object
            success: Whether the run was successful
        """
        run_stats.end_time = datetime.now()
        run_stats.success = success
        
        # Update with final values
        run_stats.articles_fetched = self.articles_fetched
        run_stats.articles_processed = self.articles_processed
        run_stats.articles_duplicate = self.articles_duplicate
        run_stats.articles_failed = self.articles_failed
        run_stats.sources_created = self.sources_created
        run_stats.symbols_created = self.symbols_created
        run_stats.industries_created = self.industries_created
        run_stats.sectors_created = self.sectors_created
        run_stats.api_calls_made = self.api_calls_made
        run_stats.errors = self.errors.copy()
        
        # Add to historical stats
        self.historical_stats.append(run_stats.to_dict())
        
        # Keep only last 100 runs to prevent file from growing too large
        if len(self.historical_stats) > 100:
            self.historical_stats = self.historical_stats[-100:]
        
        self._save_historical_stats()
        
        # Log comprehensive statistics
        self._log_run_summary(run_stats)
    
    def add_error(self, error: str):
        """Add an error to the current run statistics"""
        self.errors.append(f"{datetime.now().isoformat()}: {error}")
        self.logger.error(error)
    
    def _log_run_summary(self, run_stats: RunStats):
        """Log a comprehensive summary of the run"""
        self.logger.info("=== RUN SUMMARY ===")
        self.logger.info(f"Run ID: {run_stats.run_id}")
        self.logger.info(f"Success: {run_stats.success}")
        self.logger.info(f"Duration: {run_stats.duration}")
        
        self.logger.info("Articles:")
        self.logger.info(f"  Fetched: {run_stats.articles_fetched}")
        self.logger.info(f"  Processed: {run_stats.articles_processed}")
        self.logger.info(f"  Duplicates: {run_stats.articles_duplicate}")
        self.logger.info(f"  Failed: {run_stats.articles_failed}")
        self.logger.info(f"  Success Rate: {run_stats.success_rate:.1f}%")
        
        self.logger.info("Entities Created:")
        self.logger.info(f"  Sources: {run_stats.sources_created}")
        self.logger.info(f"  Symbols: {run_stats.symbols_created}")
        self.logger.info(f"  Industries: {run_stats.industries_created}")
        self.logger.info(f"  Sectors: {run_stats.sectors_created}")
        
        self.logger.info(f"API Calls: {run_stats.api_calls_made}")
        
        if run_stats.errors:
            self.logger.info(f"Errors: {len(run_stats.errors)}")
            for error in run_stats.errors[-5:]:  # Show last 5 errors
                self.logger.info(f"  {error}")
    
    def get_recent_stats(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        Get statistics for recent runs
        
        Args:
            days: Number of days to look back
        
        Returns:
            List[Dict]: Recent run statistics
        """
        cutoff_date = datetime.now() - timedelta(days=days)
        recent_stats = []
        
        for stats in self.historical_stats:
            try:
                start_time = datetime.fromisoformat(stats['start_time'])
                if start_time >= cutoff_date:
                    recent_stats.append(stats)
            except (KeyError, ValueError):
                continue
        
        return recent_stats
    
    def get_summary_stats(self, days: int = 30) -> Dict[str, Any]:
        """
        Get summary statistics for a period
        
        Args:
            days: Number of days to analyze
        
        Returns:
            Dict: Summary statistics
        """
        recent_stats = self.get_recent_stats(days)
        
        if not recent_stats:
            return {
                'period_days': days,
                'total_runs': 0,
                'successful_runs': 0,
                'success_rate': 0.0,
                'total_articles_processed': 0,
                'total_api_calls': 0,
                'average_articles_per_run': 0.0,
                'total_errors': 0
            }
        
        successful_runs = sum(1 for stats in recent_stats if stats.get('success', False))
        total_articles = sum(stats.get('articles_processed', 0) for stats in recent_stats)
        total_api_calls = sum(stats.get('api_calls_made', 0) for stats in recent_stats)
        total_errors = sum(len(stats.get('errors', [])) for stats in recent_stats)
        
        return {
            'period_days': days,
            'total_runs': len(recent_stats),
            'successful_runs': successful_runs,
            'success_rate': (successful_runs / len(recent_stats)) * 100,
            'total_articles_processed': total_articles,
            'total_api_calls': total_api_calls,
            'average_articles_per_run': total_articles / len(recent_stats) if recent_stats else 0,
            'total_errors': total_errors,
            'first_run': recent_stats[0]['start_time'] if recent_stats else None,
            'last_run': recent_stats[-1]['start_time'] if recent_stats else None
        }
    
    def print_summary_report(self, days: int = 7):
        """
        Print a formatted summary report
        
        Args:
            days: Number of days to include in report
        """
        summary = self.get_summary_stats(days)
        
        print(f"\n=== SCRAPER PERFORMANCE REPORT ({days} days) ===")
        print(f"Total Runs: {summary['total_runs']}")
        print(f"Successful Runs: {summary['successful_runs']}")
        print(f"Success Rate: {summary['success_rate']:.1f}%")
        print(f"Total Articles Processed: {summary['total_articles_processed']}")
        print(f"Average Articles per Run: {summary['average_articles_per_run']:.1f}")
        print(f"Total API Calls: {summary['total_api_calls']}")
        print(f"Total Errors: {summary['total_errors']}")
        
        if summary['first_run']:
            print(f"Period: {summary['first_run']} to {summary['last_run']}")
        
        print("=" * 50)