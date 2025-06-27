# Database connection and session management

import logging
from typing import Optional
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool

from models.models import Base


class DatabaseError(Exception):
    """Custom exception for database-related errors"""
    pass


class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self, database_url: str, **engine_kwargs):
        """
        Initialize the database manager
        
        Args:
            database_url: SQLAlchemy database URL
            **engine_kwargs: Additional engine configuration
        """
        self.database_url = database_url
        self.logger = logging.getLogger(__name__)
        
        # Default engine configuration
        default_config = {
            'pool_size': 5,
            'max_overflow': 10,
            'pool_timeout': 30,
            'pool_recycle': 3600,
            'poolclass': QueuePool,
            'echo': False
        }
        
        # Merge with provided config
        engine_config = {**default_config, **engine_kwargs}
        
        try:
            self.engine = create_engine(database_url, **engine_config)
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            
            # Add event listeners
            self._setup_event_listeners()
            
            self.logger.info(f"Database manager initialized for: {self._mask_url(database_url)}")
            
        except Exception as e:
            raise DatabaseError(f"Failed to initialize database: {str(e)}")
    
    def _setup_event_listeners(self):
        """Setup SQLAlchemy event listeners for monitoring"""
        
        @event.listens_for(self.engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            """Set SQLite pragmas for better performance (if using SQLite)"""
            if 'sqlite' in self.database_url:
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.close()
        
        @event.listens_for(self.engine, "checkout")
        def receive_checkout(dbapi_connection, connection_record, connection_proxy):
            """Log when a connection is checked out"""
            self.logger.debug("Database connection checked out")
        
        @event.listens_for(self.engine, "checkin")
        def receive_checkin(dbapi_connection, connection_record):
            """Log when a connection is checked in"""
            self.logger.debug("Database connection checked in")
    
    def create_tables(self):
        """Create all tables in the database"""
        try:
            Base.metadata.create_all(bind=self.engine)
            self.logger.info("Database tables created successfully")
        except Exception as e:
            raise DatabaseError(f"Failed to create tables: {str(e)}")
    
    def drop_tables(self):
        """Drop all tables (use with caution!)"""
        try:
            Base.metadata.drop_all(bind=self.engine)
            self.logger.warning("All database tables dropped")
        except Exception as e:
            raise DatabaseError(f"Failed to drop tables: {str(e)}")
    
    def get_session(self) -> Session:
        """
        Get a database session
        
        Returns:
            Session: SQLAlchemy session
        """
        try:
            return self.SessionLocal()
        except Exception as e:
            raise DatabaseError(f"Failed to create session: {str(e)}")
    
    def test_connection(self) -> bool:
        """
        Test the database connection
        
        Returns:
            bool: True if connection successful
        """
        try:
            with self.engine.connect() as connection:
                connection.execute("SELECT 1")
            self.logger.info("Database connection test successful")
            return True
        except Exception as e:
            self.logger.error(f"Database connection test failed: {str(e)}")
            return False
    
    def get_connection_info(self) -> dict:
        """Get information about database connections"""
        try:
            pool = self.engine.pool
            return {
                'pool_size': pool.size(),
                'checked_in': pool.checkedin(),
                'checked_out': pool.checkedout(),
                'overflow': pool.overflow(),
                'invalid': pool.invalid()
            }
        except Exception as e:
            self.logger.error(f"Could not get connection info: {str(e)}")
            return {}
    
    def execute_raw_sql(self, sql: str, params: Optional[dict] = None):
        """
        Execute raw SQL (use with caution)
        
        Args:
            sql: SQL statement to execute
            params: Parameters for the SQL statement
        """
        try:
            with self.engine.connect() as connection:
                result = connection.execute(sql, params or {})
                return result
        except Exception as e:
            raise DatabaseError(f"Failed to execute SQL: {str(e)}")
    
    def _mask_url(self, url: str) -> str:
        """Mask credentials in database URL for logging"""
        if '://' in url and '@' in url:
            protocol, rest = url.split('://', 1)
            if '@' in rest:
                credentials, host_part = rest.split('@', 1)
                return f"{protocol}://***:***@{host_part}"
        return url
    
    def close(self):
        """Close all connections and cleanup"""
        try:
            self.engine.dispose()
            self.logger.info("Database connections closed")
        except Exception as e:
            self.logger.error(f"Error closing database connections: {str(e)}")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


class SessionManager:
    """Context manager for database sessions with automatic rollback on errors"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.session: Optional[Session] = None
        self.logger = logging.getLogger(__name__)
    
    def __enter__(self) -> Session:
        """Enter context and create session"""
        self.session = self.db_manager.get_session()
        return self.session
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context and handle session cleanup"""
        if self.session:
            try:
                if exc_type is not None:
                    # Exception occurred, rollback
                    self.session.rollback()
                    self.logger.warning(f"Session rolled back due to {exc_type.__name__}: {exc_val}")
                else:
                    # No exception, commit
                    self.session.commit()
                    self.logger.debug("Session committed successfully")
            except Exception as e:
                self.logger.error(f"Error during session cleanup: {str(e)}")
                try:
                    self.session.rollback()
                except:
                    pass
            finally:
                self.session.close()