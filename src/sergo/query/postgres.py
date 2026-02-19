import copy
from sergo.query import BaseQuery
from sergo.connection import connection


class PostgresQuery(BaseQuery):
    """PostgreSQL query engine with parameterized queries throughout.

    Uses %s placeholders (psycopg standard). All field names are validated
    and quoted. All values are parameterized.

    Clauses are stored separately and assembled at execution time,
    so filter/order/limit can be called in any order.
    """

    _SAFE_FIELD_CHARS = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_')

    def __init__(self, query, model, params=None):
        self.model = model
        # Parse the base query (SELECT ... FROM ... possibly with WHERE)
        self._base_query = query
        self._where_conditions = []
        self._where_params = list(params) if params else []
        self._group_by = None
        self._order_clauses = []
        self._limit_value = None
        self._offset_value = None

        # Extract existing WHERE/GROUP BY from base query if present
        self._parse_base_query()

    def _clone(self):
        """Return a deep copy so mutations don't affect the original."""
        clone = self.__class__.__new__(self.__class__)
        clone.model = self.model
        clone._base_query = self._base_query
        clone._where_conditions = list(self._where_conditions)
        clone._where_params = list(self._where_params)
        clone._group_by = self._group_by
        clone._order_clauses = list(self._order_clauses)
        clone._limit_value = self._limit_value
        clone._offset_value = self._offset_value
        return clone

    @property
    def query(self):
        """Assemble and return the full query string."""
        q, _ = self._build_query()
        return q

    @query.setter
    def query(self, value):
        """Allow setting query directly for backwards compat."""
        self._base_query = value

    @property
    def params(self):
        """Return assembled params."""
        _, params = self._build_query()
        return params

    @params.setter
    def params(self, value):
        """Allow setting params directly for backwards compat."""
        self._where_params = list(value) if value else []

    def _parse_base_query(self):
        """Extract clauses from the initial SQL query string."""
        q = self._base_query

        # Extract GROUP BY
        if 'GROUP BY' in q:
            parts = q.split('GROUP BY', 1)
            q = parts[0].rstrip()
            self._group_by = parts[1].strip()

        # Extract ORDER BY
        if 'ORDER BY' in q:
            parts = q.split('ORDER BY', 1)
            q = parts[0].rstrip()
            # Don't parse — just store raw
            self._order_clauses.append(parts[1].strip())

        # Extract existing WHERE
        if 'WHERE' in q:
            parts = q.split('WHERE', 1)
            q = parts[0].rstrip()
            self._where_conditions.append(parts[1].strip())
            # Existing params belong to the where clause
            self._where_params = list(self.params)
            self.params = []

        self._base_query = q

    def _build_query(self):
        """Assemble the full query from structured clauses."""
        q = self._base_query

        # WHERE
        all_params = list(self._where_params)
        if self._where_conditions:
            q += ' WHERE ' + ' AND '.join(self._where_conditions)

        # GROUP BY
        if self._group_by:
            q += f' GROUP BY {self._group_by}'

        # ORDER BY
        if self._order_clauses:
            q += ' ORDER BY ' + ', '.join(self._order_clauses)

        # LIMIT
        if self._limit_value is not None:
            q += ' LIMIT %s'
            all_params.append(self._limit_value)

        # OFFSET
        if self._offset_value is not None:
            q += ' OFFSET %s'
            all_params.append(self._offset_value)

        return q, all_params

    @classmethod
    def _validate_field_name(cls, field: str) -> str:
        """Validate a field name contains only safe characters, then quote it."""
        if not field or not field.strip():
            raise ValueError("Empty field name")
        field = field.strip()
        if not all(c in cls._SAFE_FIELD_CHARS for c in field):
            raise ValueError(
                f"Invalid field name: {field!r}. "
                f"Only alphanumeric characters and underscores are allowed."
            )
        return f'"{field}"'

    def _build_filter_condition(self, field, operator, value):
        """Build a parameterized filter condition."""
        safe_field = self._validate_field_name(field)

        if operator == 'in':
            if not isinstance(value, (list, tuple, set)):
                raise ValueError(f"'in' operator requires a list, got {type(value).__name__}")
            if not value:
                return '1 = 0'
            placeholders = ', '.join('%s' for _ in value)
            condition = f"{safe_field} IN ({placeholders})"
            self._where_params.extend(value)
        elif operator == 'isnull':
            if value:
                condition = f"{safe_field} IS NULL"
            else:
                condition = f"{safe_field} IS NOT NULL"
        elif operator == 'startswith':
            condition = f"{safe_field} LIKE %s"
            self._where_params.append(f"{self._escape_like(value)}%")
        elif operator == 'endswith':
            condition = f"{safe_field} LIKE %s"
            self._where_params.append(f"%{self._escape_like(value)}")
        elif operator == 'contains':
            condition = f"{safe_field} LIKE %s"
            self._where_params.append(f"%{self._escape_like(value)}%")
        elif operator == 'icontains':
            condition = f"{safe_field} ILIKE %s"
            self._where_params.append(f"%{self._escape_like(value)}%")
        elif operator == 'istartswith':
            condition = f"{safe_field} ILIKE %s"
            self._where_params.append(f"{self._escape_like(value)}%")
        elif operator == 'iendswith':
            condition = f"{safe_field} ILIKE %s"
            self._where_params.append(f"%{self._escape_like(value)}")
        elif operator == 'gt':
            condition = f"{safe_field} > %s"
            self._where_params.append(value)
        elif operator == 'gte':
            condition = f"{safe_field} >= %s"
            self._where_params.append(value)
        elif operator == 'lt':
            condition = f"{safe_field} < %s"
            self._where_params.append(value)
        elif operator == 'lte':
            condition = f"{safe_field} <= %s"
            self._where_params.append(value)
        elif operator == 'exact':
            if value is None:
                condition = f"{safe_field} IS NULL"
            else:
                condition = f"{safe_field} = %s"
                self._where_params.append(value)
        else:
            condition = f"{safe_field} = %s"
            self._where_params.append(value)
        return condition

    @staticmethod
    def _escape_like(value: str) -> str:
        """Escape special LIKE characters."""
        return str(value).replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')

    def filter(self, **kwargs):
        clone = self._clone()
        for key, value in kwargs.items():
            field_parts = key.split('__')
            if len(field_parts) > 1:
                field = field_parts[0]
                operator = field_parts[1]
            else:
                field = key
                operator = 'exact'
            condition = clone._build_filter_condition(field, operator, value)
            clone._where_conditions.append(condition)
        return clone

    def exclude(self, **kwargs):
        clone = self._clone()
        for key, value in kwargs.items():
            field_parts = key.split('__')
            if len(field_parts) > 1:
                field = field_parts[0]
                operator = field_parts[1]
            else:
                field = key
                operator = 'exact'
            condition = clone._build_filter_condition(field, operator, value)
            clone._where_conditions.append(f"NOT ({condition})")
        return clone

    def get(self, **kwargs):
        from sergo import errors
        qs = self.filter(**kwargs) if kwargs else self._clone()
        result = qs.list()
        if len(result) > 1:
            raise errors.MultipleObjectsReturned("Multiple objects found")
        if not result:
            raise errors.DoesNotExist("Object not found")
        return result[0]

    def first(self):
        clone = self._clone()
        clone._limit_value = 1
        try:
            return clone.list()[0]
        except IndexError:
            return None

    def exists(self):
        return bool(self.first())

    def search(self, field, value):
        clone = self._clone()
        if field and value:
            safe_field = self._validate_field_name(field)
            condition = f"{safe_field} ILIKE %s"
            clone._where_params.append(f"%{self._escape_like(value)}%")
            clone._where_conditions.append(condition)
        return clone

    def order(self, ordering):
        if not ordering:
            return self
        clone = self._clone()
        if isinstance(ordering, str):
            ordering = [ordering]
        for order_param in ordering:
            if order_param.startswith('-'):
                column = order_param[1:]
                direction = 'DESC'
            else:
                column = order_param
                direction = 'ASC'
            safe_col = self._validate_field_name(column)
            clone._order_clauses.append(f"{safe_col} {direction}")
        return clone

    def limit(self, limit):
        clone = self._clone()
        clone._limit_value = int(limit)
        return clone

    def offset(self, offset):
        clone = self._clone()
        clone._offset_value = int(offset)
        return clone

    def paginate(self, page, page_size):
        """Postgres: LIMIT then OFFSET."""
        return self.limit(page_size).offset(page_size * (page - 1))

    def count(self):
        # Build query without ORDER BY, LIMIT, OFFSET for count
        q = self._base_query
        if self._where_conditions:
            q += ' WHERE ' + ' AND '.join(self._where_conditions)
        if self._group_by:
            q += f' GROUP BY {self._group_by}'

        query = f"SELECT COUNT(*) as query_count FROM ({q}) AS t"
        result = connection.execute_result(query, *self._where_params)
        return result[0]['query_count']

    def execute(self):
        query, params = self._build_query()
        result = connection.execute_result(query, *params)
        return [self.model(**obj) for obj in result]

    def delete(self):
        # Build WHERE clause only
        q = self._base_query
        parts = q.split('FROM', 1)
        if len(parts) < 2:
            raise ValueError("Invalid delete query")
        from_clause = parts[1]

        delete_q = f"DELETE FROM{from_clause}"
        if self._where_conditions:
            delete_q += ' WHERE ' + ' AND '.join(self._where_conditions)

        connection.execute(delete_q, *self._where_params)
        connection.commit()

    def update(self, **kwargs):
        parts = self._base_query.split('FROM', 1)
        if len(parts) < 2:
            raise ValueError("Invalid update query")
        table = parts[1].strip()

        import json as _json
        set_parts = []
        set_values = []
        for key, value in kwargs.items():
            safe_key = self._validate_field_name(key)
            set_parts.append(f"{safe_key} = %s")
            set_values.append(_json.dumps(value) if isinstance(value, (list, dict)) else value)

        set_clause = ', '.join(set_parts)
        query = f"UPDATE {table} SET {set_clause}"

        if self._where_conditions:
            query += ' WHERE ' + ' AND '.join(self._where_conditions)

        values = set_values + self._where_params
        connection.execute(query, *values)
        connection.commit()
