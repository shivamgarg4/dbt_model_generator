from typing import Dict, List, Optional, Any, Tuple
import logging
from contextlib import contextmanager
from types import TracebackType
from typing import Type

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Custom exception for database-related errors"""
    pass


class DatabaseConnection:
    """A class to manage database connections with better error handling and connection management"""
    _instance: Optional['DatabaseConnection'] = None
    _connection: Any = None

    def __new__(cls, config: Dict[str, str]) -> 'DatabaseConnection':
        """Implement singleton pattern"""
        if cls._instance is None:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
        return cls._instance

    def __init__(self, config: Dict[str, str]) -> None:
        """
        Initialize database connection configuration

        Args:
            config (Dict[str, str]): Configuration dictionary containing connection parameters
        """
        if not hasattr(self, 'config'):
            self.config = config
            self._required_config_keys = {'ACCOUNT', 'USER', 'AUTHENTICATOR',
                                          'WAREHOUSE', 'ROLE', 'DATABASE'}
            self._validate_config()

    def _validate_config(self) -> None:
        """Validate that all required configuration parameters are present"""
        missing_keys = self._required_config_keys - set(self.config.keys())
        if missing_keys:
            raise DatabaseError(f"Missing required configuration keys: {missing_keys}")

    def _is_connection_alive(self) -> bool:
        """Check if the current connection is alive and valid"""
        if not self._connection:
            return False
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except Exception:
            return False

    def connect(self) -> Any:
        """
        Create and return a database connection using provided configuration.
        If connection already exists, verify it's alive and return it, otherwise create new.

        Returns:
            Connection object

        Raises:
            DatabaseError: If connection fails or required dependencies are missing
        """
        if self._connection is not None and self._is_connection_alive():
            return self._connection

        try:
            import snowflake.connector

            logger.info("Establishing database connection...")
            self._connection = snowflake.connector.connect(
                account=self.config['ACCOUNT'],
                user=self.config['USER'],
                authenticator=self.config['AUTHENTICATOR'],
                warehouse=self.config['WAREHOUSE'],
                role=self.config['ROLE'],
                database=self.config['DATABASE']
            )
            logger.info("Database connection established successfully")
            return self._connection

        except ImportError as e:
            logger.error("Failed to import Snowflake connector")
            raise DatabaseError("Snowflake connector not properly installed") from e
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise DatabaseError(f"Failed to connect to database: {str(e)}") from e

    @contextmanager
    def get_cursor(self):
        """
        Context manager for handling database cursors.

        Yields:
            Database cursor object

        Raises:
            DatabaseError: If cursor creation fails
        """
        if not self._connection:
            self.connect()

        cursor = None
        try:
            cursor = self._connection.cursor()
            yield cursor
        except Exception as e:
            logger.error(f"Error during cursor operation: {str(e)}")
            raise DatabaseError(f"Cursor operation failed: {str(e)}") from e
        finally:
            if cursor:
                cursor.close()

    @classmethod
    def get_connection(cls) -> Any:
        """Get the existing connection or return None if no connection exists"""
        if cls._instance and cls._instance._is_connection_alive():
            return cls._connection
        return None

    @classmethod
    def disconnect(cls) -> None:
        """Safely close the database connection"""
        if cls._connection:
            try:
                cls._connection.close()
                logger.info("Database connection closed successfully")
            except Exception as e:
                logger.warning(f"Error while closing connection: {str(e)}")
            finally:
                cls._connection = None

    def __enter__(self) -> 'DatabaseConnection':
        """Support for context manager protocol"""
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_val: Optional[BaseException],
                 exc_tb: Optional[TracebackType]) -> None:
        """Ensure connection is closed when exiting context"""
        self.disconnect()


def get_snowflake_connection(config: Dict[str, str]) -> Any:
    """
    Get or create a Snowflake database connection

    Args:
        config: Configuration dictionary containing connection parameters

    Returns:
        Database connection object

    Raises:
        DatabaseError: If connection creation fails
    """
    db = DatabaseConnection(config)
    return db.connect()


@contextmanager
def get_cursor(config: Dict[str, str]):
    """
    Convenience context manager for getting a cursor directly.

    Args:
        config: Configuration dictionary containing connection parameters

    Yields:
        Database cursor object

    Raises:
        DatabaseError: If cursor creation fails
    """
    db = DatabaseConnection(config)
    with db.get_cursor() as cursor:
        yield cursor
