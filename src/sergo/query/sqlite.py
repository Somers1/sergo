from sergo.query import BaseQuery
from sergo.connection import connection


class SQLiteQuery(BaseQuery):
    """SQLite query engine with parameterized queries throughout.
    
    All user-provided values are passed as parameters (?) — never
    interpolated into query strings. Field names are validated via
    the _validate_field_name method to prevent injection through
    column name manipulation.
    """

    # Safe characters for field/column names
    _SAFE_FIELD_CHARS = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_')

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
        """Build a parameterized filter condition. 
        
        Field names are validated and quoted.
        Values are always parameterized with ?.
        """
        safe_field = self._validate_field_name(field)

        if operator == 'in':
            if not isinstance(value, (list, tuple, set)):
                raise ValueError(f"'in' operator requires a list, got {type(value).__name__}")
            if not value:
                # Empty IN clause — always false
                return '1 = 0'
            placeholders = ', '.join('?' for _ in value)
            condition = f"{safe_field} IN ({placeholders})"
            self.params.extend(value)
        elif operator == 'isnull':
            if value:
                condition = f"{safe_field} IS NULL"
            else:
                condition = f"{safe_field} IS NOT NULL"
        elif operator == 'startswith':
            condition = f"{safe_field} LIKE ? ESCAPE '\\'"
            self.params.append(f"{self._escape_like(value)}%")
        elif operator == 'endswith':
            condition = f"{safe_field} LIKE ? ESCAPE '\\'"
            self.params.append(f"%{self._escape_like(value)}")
        elif operator == 'contains':
            condition = f"{safe_field} LIKE ? ESCAPE '\\'"
            self.params.append(f"%{self._escape_like(value)}%")
        elif operator == 'gt':
            condition = f"{safe_field} > ?"
            self.params.append(value)
        elif operator == 'gte':
            condition = f"{safe_field} >= ?"
            self.params.append(value)
        elif operator == 'lt':
            condition = f"{safe_field} < ?"
            self.params.append(value)
        elif operator == 'lte':
            condition = f"{safe_field} <= ?"
            self.params.append(value)
        elif operator == 'exact':
            if value is None:
                condition = f"{safe_field} IS NULL"
            else:
                condition = f"{safe_field} = ?"
                self.params.append(value)
        else:
            # Default to exact match
            condition = f"{safe_field} = ?"
            self.params.append(value)
        return condition

    @staticmethod
    def _escape_like(value: str) -> str:
        """Escape special LIKE characters to prevent pattern injection."""
        return str(value).replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')

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
        if conditions:
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

    def get(self, **kwargs):
        from sergo import errors
        qs = self.filter(**kwargs) if kwargs else self
        result = qs.list()
        if len(result) > 1:
            raise errors.MultipleObjectsReturned("Multiple objects found")
        if not result:
            raise errors.DoesNotExist("Object not found")
        return result[0]

    def first(self):
        self.query += ' LIMIT 1'
        try:
            return self.list()[0]
        except IndexError:
            return None

    def exists(self):
        return bool(self.first())

    def search(self, field, value):
        if field and value:
            safe_field = self._validate_field_name(field)
            search_string = f"UPPER({safe_field}) LIKE UPPER(?) ESCAPE '\\'"
            self.params.append(f"%{self._escape_like(value)}%")
            if 'WHERE' in self.query:
                self.query += f" AND {search_string}"
            else:
                self.query += f" WHERE {search_string}"
        return self

    def order(self, ordering):
        if not ordering:
            return self
        order_clauses = []
        for order_param in ordering:
            if order_param.startswith('-'):
                column = order_param[1:]
                direction = 'DESC'
            else:
                column = order_param
                direction = 'ASC'
            safe_col = self._validate_field_name(column)
            order_clauses.append(f"{safe_col} {direction}")
        self.query += ' ORDER BY ' + ', '.join(order_clauses)
        return self

    def limit(self, limit):
        self.query += ' LIMIT ?'
        self.params.append(int(limit))
        return self

    def offset(self, offset):
        self.query += ' OFFSET ?'
        self.params.append(int(offset))
        return self

    def paginate(self, page, page_size):
        """SQLite requires LIMIT before OFFSET."""
        self.limit(page_size)
        self.offset(page_size * (page - 1))
        return self

    def count(self):
        # Strip ORDER BY for count queries
        count_query = self.query
        if 'ORDER BY' in count_query:
            count_query = 'ORDER BY'.join(count_query.split('ORDER BY')[:-1]).rstrip()
        query = f"SELECT COUNT(*) as query_count FROM ({count_query})"
        result = connection.execute_result(query, *self.params)
        return result[0]['query_count']

    def execute(self):
        result = connection.execute_result(self.query, *self.params)
        return [self.model(**obj) for obj in result]

    def delete(self):
        split_query = self.query.split('FROM')
        if len(split_query) < 2:
            raise ValueError("Invalid delete query")
        from_clause = 'FROM'.join(split_query[1:])
        query = f"DELETE FROM{from_clause}"
        connection.execute(query, *self.params)
        connection.commit()

    def update(self, **kwargs):
        split_query = self.query.split('FROM')
        if len(split_query) < 2:
            raise ValueError("Invalid update query")
        from_clause = 'FROM'.join(split_query[1:])

        set_parts = []
        set_values = []
        for key, value in kwargs.items():
            safe_key = self._validate_field_name(key)
            set_parts.append(f"{safe_key} = ?")
            set_values.append(value)

        set_clause = ', '.join(set_parts)

        if 'WHERE' in from_clause:
            table, where_clause = from_clause.rsplit('WHERE', 1)
            query = f"UPDATE{table} SET {set_clause} WHERE{where_clause}"
        else:
            query = f"UPDATE{from_clause} SET {set_clause}"

        values = set_values + self.params
        connection.execute(query, *values)
        connection.commit()
