import re
from functools import wraps

import settings
from settings import logger

psycopg2 = None


def _get_psycopg2():
    global psycopg2
    if psycopg2 is None:
        import importlib
        psycopg2 = importlib.import_module('psycopg2')
        importlib.import_module('psycopg2.extras')
    return psycopg2


class PostgresConnection:
    DEFAULT_QUERY = "SELECT * FROM {table_name}"

    # Safe identifier characters: alphanumeric, underscores, dots (for schema.table)
    _SAFE_IDENTIFIER = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.')

    @classmethod
    def _validate_identifier(cls, identifier: str) -> str:
        """Validate and quote a SQL identifier to prevent injection."""
        if not identifier or not identifier.strip():
            raise ValueError("Empty identifier")

        parts = identifier.strip().split('.')
        quoted_parts = []
        for part in parts:
            part = part.strip()
            if not part:
                raise ValueError(f"Empty identifier component in: {identifier}")
            if not all(c in cls._SAFE_IDENTIFIER for c in part):
                raise ValueError(
                    f"Invalid identifier: {part!r}. "
                    f"Only alphanumeric characters and underscores are allowed."
                )
            # Double-quote the identifier (escaping any internal quotes)
            quoted_parts.append(f'"{part.replace(chr(34), chr(34)+chr(34))}"')
        return '.'.join(quoted_parts)

    @staticmethod
    def ensure_connection(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if self._connection is None or self._connection.closed:
                self.connect()
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                pg = _get_psycopg2()
                if isinstance(e, pg.OperationalError):
                    logger.warning(f"Connection lost. Reconnecting... Error: {e}")
                    self.connect()
                    return func(self, *args, **kwargs)
                raise
        return wrapper

    def __init__(self):
        self._connection = None
        self._cursor = None

    def connect(self):
        pg = _get_psycopg2()
        db_config = settings.DATABASE_CONFIG
        logger.info('Connecting to PostgreSQL database')
        self._connection = pg.connect(
            host=db_config.get('host', 'localhost'),
            port=db_config.get('port', 5432),
            dbname=db_config.get('name', ''),
            user=db_config.get('user', ''),
            password=db_config.get('pass', ''),
            **{k: v for k, v in db_config.items() if k not in ('host', 'port', 'name', 'user', 'pass')}
        )
        self._connection.autocommit = False
        self._cursor = self._connection.cursor(cursor_factory=pg.extras.RealDictCursor)

    @property
    @ensure_connection
    def cursor(self):
        return self._cursor

    @property
    @ensure_connection
    def connection(self):
        return self._connection

    def execute_result(self, query, *params):
        """Execute a query and return results as list of dicts."""
        self.execute(query, *params)
        if self.cursor.description is None:
            return []
        rows = self.cursor.fetchall()
        return [dict(row) for row in rows]

    def execute_many_result(self, query, params):
        """Execute a query with multiple parameter sets."""
        logger.debug(f"Executing many: {query}")
        self.cursor.executemany(query, params)

    def execute(self, query, *params):
        """Execute a query with parameterized values."""
        logger.debug(f"Executing query: {query} with params: {params}")
        self.cursor.execute(query, params)

    def commit(self):
        self.connection.commit()

    def rollback(self):
        if self._connection and not self._connection.closed:
            self._connection.rollback()

    def close(self):
        logger.info('Closing PostgreSQL connection')
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()
        self._cursor = None
        self._connection = None

    def insert(self, insert_dict, table):
        """Insert a row and return the new row's id.

        All column names are validated and quoted.
        Values are always parameterized.
        """
        safe_table = self._validate_identifier(table)
        safe_columns = ', '.join(
            self._validate_identifier(col) for col in insert_dict.keys()
        )
        placeholders = ', '.join('%s' for _ in insert_dict.values())
        query = f"INSERT INTO {safe_table} ({safe_columns}) VALUES ({placeholders}) RETURNING id"
        result = self.execute_result(query, *insert_dict.values())
        self.commit()
        return result[0]['id']

    def insert_many(self, insert_list, table, batch_size=None):
        """Insert multiple rows."""
        if not insert_list:
            return
        safe_table = self._validate_identifier(table)
        safe_columns = ', '.join(
            self._validate_identifier(col) for col in insert_list[0].keys()
        )
        placeholders = ', '.join('%s' for _ in insert_list[0].values())
        query = f"INSERT INTO {safe_table} ({safe_columns}) VALUES ({placeholders})"
        self.execute_many_result(query, [tuple(row.values()) for row in insert_list])
        self.commit()

    @staticmethod
    def default_query(table):
        return PostgresConnection.DEFAULT_QUERY.format(table_name=table)
