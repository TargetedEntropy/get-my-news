#!/usr/bin/env python3
"""
Main scraper script - entry point for crontab execution

This script orchestrates the entire scraping process:
- Enforces single instance execution
- Respects API rate limits (100 calls/24h)
- Fetches articles from newsfilter.io API
- Processes and stores data in MySQL database
- Logs comprehensive statistics and status
"""

import sys
import traceback
from datetime import datetime
from typing import Dict, List, Any

# Import configuration and logging first
from config.settings import Settings
from utils.logger import setup_logger, get_logger

# Core functionality imports
from core.process_lock import ProcessLock
from core.rate_limiter import RateLimiter
from core.api_client import NewsfilterAPIClient
from core.database import DatabaseManager

# Models and utilities
from models.models import Article, Source, Symbol, Industry, Sector
from utils.stats import ScraperStats


class NewsfilterScraper:
    """Main scraper class that coordinates all scraping operations"""

    def __init__(self):
        self.settings = Settings()
        self.logger = get_logger(__name__)
        self.stats = ScraperStats()

        # Initialize core components
        self.process_lock = ProcessLock()
        self.rate_limiter = RateLimiter(
            max_requests=self.settings.MAX_DAILY_REQUESTS,
            tracking_file=self.settings.RATE_LIMIT_FILE,
        )
        self.api_client = NewsfilterAPIClient(
            api_key=self.settings.NEWSFILTER_API_KEY,
            base_url=self.settings.NEWSFILTER_API_URL,
        )
        self.db_manager = DatabaseManager(self.settings.DATABASE_URL)

    def run(self) -> bool:
        """
        Main execution method

        Returns:
            bool: True if successful, False otherwise
        """
        start_time = datetime.now()
        self.logger.info("Newsfilter scraper starting...")

        try:
            # Step 1: Acquire process lock
            if not self._acquire_lock():
                return False

            # Step 2: Check rate limits
            if not self._check_rate_limits():
                return False

            # Step 3: Fetch and process articles
            success = self._scrape_and_process()

            # Step 4: Log final statistics
            self._log_final_stats(start_time, success)

            return success

        except Exception as e:
            self.logger.error(f"Unexpected error in scraper: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return False

        finally:
            # Always release the lock
            self._release_lock()

    def _acquire_lock(self) -> bool:
        """Acquire process lock to ensure single instance"""
        try:
            if self.process_lock.acquire():
                self.logger.info("Process lock acquired successfully")
                return True
            else:
                self.logger.warning("Another scraper instance is already running")
                return False
        except Exception as e:
            self.logger.error(f"Failed to acquire process lock: {str(e)}")
            return False

    def _check_rate_limits(self) -> bool:
        """Check if we can make API calls within rate limits"""
        try:
            if self.rate_limiter.can_make_request():
                current_usage = self.rate_limiter.get_current_usage()
                self.logger.info(
                    f"Rate limit check passed: {current_usage['daily_usage']}/{current_usage['max_requests']} calls used today"
                )
                return True
            else:
                current_usage = self.rate_limiter.get_current_usage()
                self.logger.warning(
                    f"Rate limit exceeded: {current_usage['daily_usage']}/{current_usage['max_requests']} calls used today"
                )
                return False
        except Exception as e:
            self.logger.error(f"Failed to check rate limits: {str(e)}")
            return False

    def _scrape_and_process(self) -> bool:
        """Main scraping and processing logic"""
        session = None
        try:
            # Get database session
            session = self.db_manager.get_session()

            # Authenticate with API
            if not self.api_client.authenticate():
                self.logger.error("API authentication failed")
                return False

            self.logger.info("API authentication successful")

            # Fetch articles from API
            articles_data = self._fetch_articles()
            if articles_data is None:
                return False

            # Process and store articles
            return self._process_articles(session, articles_data)

        except Exception as e:
            self.logger.error(f"Error in scrape and process: {str(e)}")
            if session:
                session.rollback()
            return False

        finally:
            if session:
                session.close()

    def _fetch_articles(self) -> List[Dict[str, Any]] | None:
        """Fetch articles from the API"""
        try:
            # Record API usage
            self.rate_limiter.record_request()

            # Make API call
            articles = self.api_client.get_articles()

            if articles:
                self.logger.info(
                    f"Successfully fetched {len(articles)} articles from API"
                )
                self.stats.articles_fetched = len(articles)
                return articles
            else:
                self.logger.warning("No articles returned from API")
                return []

        except Exception as e:
            self.logger.error(f"Failed to fetch articles: {str(e)}")
            return None

    def _process_articles(self, session, articles_data: List[Dict[str, Any]]) -> bool:
        """Process and store articles in database"""
        try:
            for article_data in articles_data:
                try:
                    # Check if article already exists
                    existing_article = (
                        session.query(Article).filter_by(id=article_data["id"]).first()
                    )

                    if existing_article:
                        self.stats.articles_duplicate += 1
                        self.logger.debug(
                            f"Article {article_data['id']} already exists, skipping"
                        )
                        continue

                    # Create new article
                    article = self._create_article_from_data(session, article_data)

                    if article:
                        self.stats.articles_processed += 1
                        self.logger.debug(
                            f"Successfully processed article: {article.id}"
                        )

                except Exception as e:
                    self.logger.error(
                        f"Failed to process article {article_data.get('id', 'unknown')}: {str(e)}"
                    )
                    self.stats.articles_failed += 1
                    # Continue processing other articles
                    continue

            # Commit all changes
            session.commit()
            self.logger.info(
                f"Successfully committed {self.stats.articles_processed} new articles to database"
            )

            return True

        except Exception as e:
            self.logger.error(f"Database transaction failed: {str(e)}")
            session.rollback()
            return False

    def _create_article_from_data(
        self, session, article_data: Dict[str, Any]
    ) -> Article | None:
        """Create Article and related entities from API data"""
        try:
            from dateutil import parser

            # Parse published date
            published_at = parser.parse(article_data["publishedAt"])

            # Get or create source
            source = self._get_or_create_source(session, article_data["source"])

            # Create article
            article = Article(
                id=article_data["id"],
                title=article_data["title"],
                description=article_data.get("description", ""),
                source_url=article_data["sourceUrl"],
                image_url=article_data.get("imageUrl"),
                published_at=published_at,
                source_id=source.id,
            )

            # Add symbols
            for symbol_name in article_data.get("symbols", []):
                symbol = self._get_or_create_symbol(session, symbol_name)
                article.symbols.append(symbol)

            # Add industries
            for industry_name in article_data.get("industries", []):
                industry = self._get_or_create_industry(session, industry_name)
                article.industries.append(industry)

            # Add sectors
            for sector_name in article_data.get("sectors", []):
                sector = self._get_or_create_sector(session, sector_name)
                article.sectors.append(sector)

            session.add(article)
            session.flush()  # Ensure ID is available

            return article

        except Exception as e:
            self.logger.error(f"Failed to create article from data: {str(e)}")
            return None

    def _get_or_create_source(self, session, source_data: Dict[str, str]) -> Source:
        """Get existing source or create new one"""
        source = session.query(Source).filter_by(id=source_data["id"]).first()
        if not source:
            source = Source(id=source_data["id"], name=source_data["name"])
            session.add(source)
            session.flush()
            self.stats.sources_created += 1
        return source

    def _get_or_create_symbol(self, session, symbol_name: str) -> Symbol:
        """Get existing symbol or create new one"""
        symbol = session.query(Symbol).filter_by(symbol=symbol_name).first()
        if not symbol:
            symbol = Symbol(symbol=symbol_name)
            session.add(symbol)
            session.flush()
            self.stats.symbols_created += 1
        return symbol

    def _get_or_create_industry(self, session, industry_name: str) -> Industry:
        """Get existing industry or create new one"""
        industry = session.query(Industry).filter_by(name=industry_name).first()
        if not industry:
            industry = Industry(name=industry_name)
            session.add(industry)
            session.flush()
            self.stats.industries_created += 1
        return industry

    def _get_or_create_sector(self, session, sector_name: str) -> Sector:
        """Get existing sector or create new one"""
        sector = session.query(Sector).filter_by(name=sector_name).first()
        if not sector:
            sector = Sector(name=sector_name)
            session.add(sector)
            session.flush()
            self.stats.sectors_created += 1
        return sector

    def _log_final_stats(self, start_time: datetime, success: bool):
        """Log comprehensive statistics about the scraper run"""
        duration = datetime.now() - start_time

        # Log execution summary
        status = "SUCCESS" if success else "FAILED"
        self.logger.info(f"Scraper execution {status} - Duration: {duration}")

        # Log detailed statistics
        self.logger.info("=== SCRAPER STATISTICS ===")
        self.logger.info(f"Articles fetched from API: {self.stats.articles_fetched}")
        self.logger.info(f"Articles processed (new): {self.stats.articles_processed}")
        self.logger.info(
            f"Articles skipped (duplicates): {self.stats.articles_duplicate}"
        )
        self.logger.info(f"Articles failed: {self.stats.articles_failed}")
        self.logger.info(f"New sources created: {self.stats.sources_created}")
        self.logger.info(f"New symbols created: {self.stats.symbols_created}")
        self.logger.info(f"New industries created: {self.stats.industries_created}")
        self.logger.info(f"New sectors created: {self.stats.sectors_created}")

        # Log rate limit status
        current_usage = self.rate_limiter.get_current_usage()
        self.logger.info(
            f"API usage today: {current_usage['daily_usage']}/{current_usage['max_requests']}"
        )

        # Print statistics to stdout for cron monitoring
        print(f"Newsfilter Scraper {status}")
        print(f"Duration: {duration}")
        print(f"New articles: {self.stats.articles_processed}")
        print(
            f"API calls used: {current_usage['daily_usage']}/{current_usage['max_requests']}"
        )

    def _release_lock(self):
        """Release the process lock"""
        try:
            self.process_lock.release()
            self.logger.info("Process lock released")
        except Exception as e:
            self.logger.error(f"Failed to release process lock: {str(e)}")


def main():
    """Main entry point for the scraper"""
    # Setup logging
    setup_logger()

    # Create and run scraper
    scraper = NewsfilterScraper()
    success = scraper.run()

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
