from functools import wraps
import pyodbc
import settings
from sergo import utils
from settings import logger


class AzureSQLConnection:
    CONNECTION_STRING = ('Driver={{ODBC Driver 18 for SQL Server}};'
                         'Server=tcp:{host},{port};'
                         'Database={name};Uid={user};Pwd={pass};'
                         'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    DEFAULT_QUERY = "SELECT * FROM {table_name}"

    @staticmethod
    def ensure_connection(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if self._connection is None or self._connection.closed:
                self.connect()
            try:
                return func(self, *args, **kwargs)
            except pyodbc.Error as e:
                if e.args[0] in ("08S01", "01000", "08003", "08007", "08S02"):
                    logger.warning(f"Connection lost. Reconnecting... Error: {e}")
                    self.connect()
                    return func(self, *args, **kwargs)
                else:
                    raise

        return wrapper

    def __init__(self):
        self._connection = None
        self._cursor = None

    def connect(self):
        logger.info('Forming connection')
        self._connection = pyodbc.connect(self.CONNECTION_STRING.format(**settings.DATABASE_CONFIG))
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
        self.execute(query, *params)
        return [dict(zip([column[0] for column in self.cursor.description], row))
                for row in self.cursor.fetchall()]

    def execute_many_result(self, query, params):
        # self.cursor.fast_executemany = True
        self.cursor.executemany(query, params)
        return [dict(zip([column[0] for column in self.cursor.description], row))
                for row in self.cursor.fetchall()]

    def execute(self, query, *params):
        settings.logger.debug(f"Executing query: {query} with params: {params}")
        self.cursor.execute(query, params)

    def commit(self):
        self.cursor.commit()

    def close(self):
        logger.info('Closing connection')
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()
        self._cursor = None
        self._connection = None

    def insert(self, insert_dict, table):
        columns = ', '.join(insert_dict.keys())
        placeholders = ', '.join('?' for _ in insert_dict.values())
        query = f"INSERT INTO {table} ({columns}) OUTPUT INSERTED.id VALUES ({placeholders})"
        result = self.execute_result(query, *insert_dict.values())
        self.commit()
        return result[0]['id']

    def insert_many(self, insert_list, table, batch_size=10):
        columns = ', '.join(insert_list[0].keys())
        placeholders = ', '.join('?' for _ in insert_list[0].values())
        query = f"INSERT INTO {table} ({columns}) OUTPUT INSERTED.id VALUES ({placeholders})"
        results = self.execute_many_result(query, [list(row.values()) for row in insert_list])
        self.commit()
        return results

    @staticmethod
    def default_query(table):
        return AzureSQLConnection.DEFAULT_QUERY.format(table_name=table)


Connection = utils.import_string(settings.DATABASE_ENGINE)
connection = Connection()
