from abc import ABC, abstractmethod
import settings
from sergo import utils
from sergo.connection import connection


class BaseQuery(ABC):
    def __init__(self, query, model, params=None):
        self.query = query
        self.model = model
        self.params = params or []

    @abstractmethod
    def filter(self, **kwargs):
        pass

    @abstractmethod
    def search(self, field, value):
        pass

    @abstractmethod
    def order(self, ordering):
        pass

    @abstractmethod
    def limit(self, limit):
        pass

    @abstractmethod
    def offset(self, offset):
        pass

    @abstractmethod
    def paginate(self, page, page_size):
        pass

    @abstractmethod
    def count(self):
        pass

    @abstractmethod
    def execute(self):
        pass

    @abstractmethod
    def delete(self):
        pass

    @abstractmethod
    def update(self, **kwargs):
        pass

    def __iter__(self):
        return iter(self.execute())

    def __len__(self):
        return self.count()

    def list(self):
        return self.execute()


class TransactSQLQuery(BaseQuery):

    def _build_filter_condition(self, field, operator, value):
        if operator == 'in':
            placeholders = ', '.join(['?' for _ in value])
            condition = f"{field} IN ({placeholders})"
            self.params.extend(value)
        elif operator == 'startswith':
            condition = f"{field} LIKE ?"
            self.params.append(f"{value}%")
        elif operator == 'endswith':
            condition = f"{field} LIKE ?"
            self.params.append(f"%{value}")
        elif operator == 'contains':
            condition = f"{field} LIKE ?"
            self.params.append(f"%{value}%")
        elif operator == 'gt':
            condition = f"{field} > ?"
            self.params.append(value)
        elif operator == 'gte':
            condition = f"{field} >= ?"
            self.params.append(value)
        elif operator == 'lt':
            condition = f"{field} < ?"
            self.params.append(value)
        elif operator == 'lte':
            condition = f"{field} <= ?"
            self.params.append(value)
        elif operator == 'exact':
            condition = f"{field} = ?"
            self.params.append(value)
        else:
            condition = f"{field} = ?"
            self.params.append(value)
        return condition

    def filter(self, **kwargs):
        split_query = self.query.split('GROUP BY')
        where_clause = ' WHERE ' if 'WHERE' not in split_query[0] else ' AND '
        conditions = []
        for key, value in kwargs.items():
            field_parts = key.split('__')
            if len(field_parts) > 1:
                field = field_parts[0]
                operator = field_parts[1]
            else:
                field = key
                operator = 'exact'
            condition = self._build_filter_condition(field, operator, value)
            conditions.append(condition)

        split_query[0] += where_clause + ' AND '.join(conditions)
        self.query = ' GROUP BY'.join(split_query)
        return self

    def exclude(self, **kwargs):
        split_query = self.query.split('GROUP BY')
        where_clause = ' WHERE ' if 'WHERE' not in split_query[0] else ' AND '
        conditions = []
        for key, value in kwargs.items():
            field_parts = key.split('__')
            if len(field_parts) > 1:
                field = field_parts[0]
                operator = field_parts[1]
            else:
                field = key
                operator = 'exact'
            condition = self._build_filter_condition(field, operator, value)
            conditions.append(f"NOT ({condition})")

        split_query[0] += where_clause + ' AND '.join(conditions)
        self.query = ' GROUP BY'.join(split_query)
        return self

    def first(self):
        self.query = self.query.replace('SELECT', 'SELECT TOP 1')
        try:
            return self.list()[0]
        except IndexError:
            return None

    def exists(self):
        return bool(self.first())

    def search(self, field, value):
        if field and value:
            search_string = f"UPPER({field}) LIKE UPPER(?)"
            self.params.append(f"%{value}%")
            if 'WHERE' in self.query:
                self.query += f" AND {search_string}"
            else:
                self.query += f" WHERE {search_string}"
        return self

    def order(self, ordering):
        if not ordering:
            return
        order_clauses = []
        for order_param in ordering:
            if order_param.startswith('-'):
                column = order_param[1:]
                direction = 'DESC'
            else:
                column = order_param
                direction = 'ASC'
            order_clauses.append(f"{column} {direction}")
        self.query += ' ORDER BY ' + ', '.join(order_clauses)
        return self

    def limit(self, limit):
        self.query += ' FETCH NEXT ? ROWS ONLY'
        self.params.append(limit)
        return self

    def offset(self, limit):
        self.query += ' OFFSET ? ROWS'
        self.params.append(limit)
        return self

    def paginate(self, page, page_size):
        self.offset(page_size * (page - 1))
        self.limit(page_size)
        return self

    def count(self):
        main_query = 'ORDER BY'.join(self.query.split('ORDER BY')[:-1])
        query = f"SELECT COUNT(*) as query_count FROM ({main_query}) AS t"
        result = connection.execute_result(query, *self.params)
        return result[0]['query_count']

    def execute(self):
        result = connection.execute_result(self.query, *self.params)
        return [self.model(**obj) for obj in result]

    def delete(self):
        split_query = self.query.split('FROM')
        if len(split_query) != 2:
            raise ValueError(f"Invalid delete query")
        query = f"DELETE FROM {self.query.split('FROM')[1]}"
        connection.execute(query, *self.params)
        connection.commit()

    def update(self, **kwargs):
        split_query = self.query.split('FROM')
        if len(split_query) != 2:
            raise ValueError(f"Invalid update query")
        set_clause = ', '.join([f"{key} = ?" for key in kwargs.keys()])
        table, where_clause = split_query[1].rsplit('WHERE', 1)
        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        values = list(kwargs.values()) + self.params
        connection.execute(query, *values)
        connection.commit()


class PostgresSQLQuery(BaseQuery):

    def filter(self, query, fields, filters):
        raise NotImplementedError


Query = utils.import_string(settings.QUERY_ENGINE)
