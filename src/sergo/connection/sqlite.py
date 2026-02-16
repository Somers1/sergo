import sqlite3
import os
from functools import wraps

import settings
from settings import logger


class SQLiteConnection:
    DEFAULT_QUERY = "SELECT * FROM {table_name}"

    # Table/column name validation: only alphanumeric, underscores, dots (for schema.table)
    _SAFE_IDENTIFIER = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.')

    @classmethod
    def _validate_identifier(cls, identifier: str) -> str:
        """Validate and quote a SQL identifier to prevent injection.
        
        Rejects any identifier containing characters outside the safe set,
        then wraps each component in double quotes for safety.
        """
        if not identifier or not identifier.strip():
            raise ValueError("Empty identifier")

        # Split on dots for schema.table notation
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
            if self._connection is None:
                self.connect()
            return func(self, *args, **kwargs)
        return wrapper

    def __init__(self):
        self._connection: sqlite3.Connection | None = None
        self._cursor: sqlite3.Cursor | None = None

    def connect(self):
        db_path = settings.DATABASE_CONFIG.get('path', ':memory:')
        logger.info(f'Connecting to SQLite database: {db_path}')

        # Ensure directory exists for file-based databases
        if db_path != ':memory:':
            os.makedirs(os.path.dirname(db_path) or '.', exist_ok=True)

        self._connection = sqlite3.connect(db_path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._cursor = self._connection.cursor()

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
        rows = self.cursor.fetchall()
        if not rows:
            return []
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

    def close(self):
        logger.info('Closing SQLite connection')
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
        placeholders = ', '.join('?' for _ in insert_dict.values())
        query = f"INSERT INTO {safe_table} ({safe_columns}) VALUES ({placeholders})"
        self.execute(query, *insert_dict.values())
        last_id = self.cursor.lastrowid
        self.commit()
        return last_id

    def insert_many(self, insert_list, table, batch_size=None):
        """Insert multiple rows."""
        if not insert_list:
            return
        safe_table = self._validate_identifier(table)
        safe_columns = ', '.join(
            self._validate_identifier(col) for col in insert_list[0].keys()
        )
        placeholders = ', '.join('?' for _ in insert_list[0].values())
        query = f"INSERT INTO {safe_table} ({safe_columns}) VALUES ({placeholders})"
        self.execute_many_result(query, [list(row.values()) for row in insert_list])
        self.commit()

    def create_table(self, table_name, columns):
        """Create a table if it doesn't exist.
        
        columns: dict of {column_name: column_definition}
        Column names are validated. Definitions are validated for safe SQL types only.
        """
        safe_table = self._validate_identifier(table_name)
        col_defs = []
        for col_name, col_def in columns.items():
            safe_col = self._validate_identifier(col_name)
            # Validate column definition contains only safe SQL tokens
            self._validate_column_definition(col_def)
            col_defs.append(f"{safe_col} {col_def}")
        query = f"CREATE TABLE IF NOT EXISTS {safe_table} ({', '.join(col_defs)})"
        self.execute(query)
        self.commit()

    @staticmethod
    def _validate_column_definition(col_def: str):
        """Validate a column definition only contains safe SQL tokens."""
        safe_tokens = {
            'INTEGER', 'TEXT', 'REAL', 'BLOB', 'NULL', 'NOT', 'PRIMARY', 'KEY',
            'AUTOINCREMENT', 'DEFAULT', 'UNIQUE', 'CHECK', 'REFERENCES',
            'FOREIGN', 'ON', 'DELETE', 'UPDATE', 'CASCADE', 'SET', 'RESTRICT',
            'NO', 'ACTION', 'CURRENT_TIMESTAMP', 'CURRENT_DATE', 'CURRENT_TIME',
            'TRUE', 'FALSE', 'ASC', 'DESC', 'COLLATE', 'NOCASE', 'RTRIM',
            'BINARY', 'BOOLEAN', 'DATETIME', 'VARCHAR', 'NUMERIC',
        }
        # Tokenize: split on whitespace and parens, keep numbers and quoted strings
        import re
        tokens = re.findall(r"[A-Za-z_]+|\d+|'[^']*'|[(),]", col_def)
        for token in tokens:
            if token in ('(', ')', ','):
                continue
            if token.isdigit():
                continue
            if token.startswith("'") and token.endswith("'"):
                continue
            if token.upper() not in safe_tokens:
                raise ValueError(
                    f"Unsafe token in column definition: {token!r}. "
                    f"Only standard SQL type tokens are allowed."
                )

    @staticmethod
    def default_query(table):
        return SQLiteConnection.DEFAULT_QUERY.format(table_name=table)
