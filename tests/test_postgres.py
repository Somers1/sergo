"""Tests for PostgreSQL connection and query engine.

Covers:
- Query building (filter, exclude, search, order, paginate)
- SQL injection prevention (field names, values, LIKE patterns, identifiers)
- Edge cases (empty IN, NULL handling, icontains/ILIKE)
- Connection with mocked psycopg2
- Insert/insert_many with identifier validation
"""
import sys
import types
import logging
from unittest import TestCase
from unittest.mock import MagicMock, patch, PropertyMock

# Patch settings before importing sergo modules
settings_module = types.ModuleType('settings')
settings_module.DATABASE_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'name': 'testdb',
    'user': 'testuser',
    'pass': 'testpass'
}
settings_module.DATABASE_ENGINE = 'sergo.connection.postgres.PostgresConnection'
settings_module.QUERY_ENGINE = 'sergo.query.postgres.PostgresQuery'
settings_module.SERGO_CONFIG = {}
settings_module.GLOBAL_CONFIG = {}
settings_module.HANDLER = 'sergo.handler.FastAPIHandler'
settings_module.logger = logging.getLogger('sergo_test')
sys.modules['settings'] = settings_module

# Mock psycopg (v3) before importing connection
mock_psycopg = MagicMock()
mock_psycopg_rows = MagicMock()
mock_psycopg_rows.dict_row = 'dict_row'
mock_psycopg.rows = mock_psycopg_rows
mock_psycopg.OperationalError = type('OperationalError', (Exception,), {})
sys.modules['psycopg'] = mock_psycopg
sys.modules['psycopg.rows'] = mock_psycopg_rows

from sergo.connection.postgres import PostgresConnection
from sergo.query.postgres import PostgresQuery


# ─── Query Building Tests ───

class TestPostgresQuery(TestCase):
    def setUp(self):
        self.base_query = 'SELECT * FROM thing'

    def test_filter_single_condition(self):
        query = PostgresQuery(self.base_query, model=object)
        query.filter(id=1)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "id" = %s')
        self.assertEqual(query.params, [1])

    def test_filter_multiple_conditions(self):
        query = PostgresQuery(self.base_query, model=object)
        query.filter(id=1, name='John')
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "id" = %s AND "name" = %s')
        self.assertEqual(query.params, [1, 'John'])

    def test_filter_in(self):
        query = PostgresQuery(self.base_query, model=object)
        query.filter(id__in=[1, 2, 3])
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "id" IN (%s, %s, %s)')
        self.assertEqual(query.params, [1, 2, 3])

    def test_filter_in_empty(self):
        query = PostgresQuery(self.base_query, model=object)
        query.filter(id__in=[])
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE 1 = 0')
        self.assertEqual(query.params, [])

    def test_filter_startswith(self):
        query = PostgresQuery(self.base_query, model=object)
        query.filter(name__startswith='Jo')
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "name" LIKE %s')
        self.assertEqual(query.params, ['Jo%'])

    def test_filter_endswith(self):
        query = PostgresQuery(self.base_query, model=object)
        query.filter(name__endswith='son')
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "name" LIKE %s')
        self.assertEqual(query.params, ['%son'])

    def test_filter_contains(self):
        query = PostgresQuery(self.base_query, model=object)
        query.filter(name__contains='oh')
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "name" LIKE %s')
        self.assertEqual(query.params, ['%oh%'])

    def test_filter_icontains(self):
        query = PostgresQuery(self.base_query, model=object)
        query.filter(name__icontains='john')
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "name" ILIKE %s')
        self.assertEqual(query.params, ['%john%'])

    def test_filter_istartswith(self):
        query = PostgresQuery(self.base_query, model=object)
        query.filter(name__istartswith='jo')
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "name" ILIKE %s')
        self.assertEqual(query.params, ['jo%'])

    def test_filter_iendswith(self):
        query = PostgresQuery(self.base_query, model=object)
        query.filter(name__iendswith='SON')
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "name" ILIKE %s')
        self.assertEqual(query.params, ['%SON'])

    def test_filter_gt(self):
        query = PostgresQuery(self.base_query, model=object)
        query.filter(age__gt=30)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "age" > %s')
        self.assertEqual(query.params, [30])

    def test_filter_gte(self):
        query = PostgresQuery(self.base_query, model=object)
        query.filter(age__gte=30)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "age" >= %s')
        self.assertEqual(query.params, [30])

    def test_filter_lt(self):
        query = PostgresQuery(self.base_query, model=object)
        query.filter(age__lt=30)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "age" < %s')
        self.assertEqual(query.params, [30])

    def test_filter_lte(self):
        query = PostgresQuery(self.base_query, model=object)
        query.filter(age__lte=30)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "age" <= %s')
        self.assertEqual(query.params, [30])

    def test_filter_isnull_true(self):
        query = PostgresQuery(self.base_query, model=object)
        query.filter(email__isnull=True)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "email" IS NULL')
        self.assertEqual(query.params, [])

    def test_filter_isnull_false(self):
        query = PostgresQuery(self.base_query, model=object)
        query.filter(email__isnull=False)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "email" IS NOT NULL')
        self.assertEqual(query.params, [])

    def test_filter_none_value(self):
        query = PostgresQuery(self.base_query, model=object)
        query.filter(email=None)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "email" IS NULL')
        self.assertEqual(query.params, [])

    def test_filter_with_existing_where(self):
        query = PostgresQuery('SELECT * FROM thing WHERE created_at > %s', model=object)
        query.params = ['2023-01-01']
        query.filter(status='active')
        self.assertIn('AND "status" = %s', query.query)
        self.assertEqual(query.params, ['2023-01-01', 'active'])

    def test_exclude_single(self):
        query = PostgresQuery(self.base_query, model=object)
        query.exclude(id=1)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE NOT ("id" = %s)')
        self.assertEqual(query.params, [1])

    def test_exclude_multiple(self):
        query = PostgresQuery(self.base_query, model=object)
        query.exclude(status='inactive', is_deleted=True)
        self.assertIn('NOT ("status" = %s)', query.query)
        self.assertIn('NOT ("is_deleted" = %s)', query.query)

    def test_exclude_in(self):
        query = PostgresQuery(self.base_query, model=object)
        query.exclude(category__in=['spam', 'trash'])
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE NOT ("category" IN (%s, %s))')
        self.assertEqual(query.params, ['spam', 'trash'])

    def test_combined_filter_exclude(self):
        query = PostgresQuery(self.base_query, model=object)
        query.filter(category='electronics', price__gt=100).exclude(brand__in=['X', 'Y'])
        self.assertIn('"category" = %s', query.query)
        self.assertIn('"price" > %s', query.query)
        self.assertIn('NOT ("brand" IN (%s, %s))', query.query)
        self.assertEqual(query.params, ['electronics', 100, 'X', 'Y'])

    def test_search_ilike(self):
        query = PostgresQuery(self.base_query, model=object)
        query.search('name', 'john')
        self.assertIn('"name" ILIKE %s', query.query)
        self.assertEqual(query.params, ['%john%'])

    def test_search_none(self):
        query = PostgresQuery(self.base_query, model=object)
        query.search(None, None)
        self.assertEqual(query.query, self.base_query)

    def test_order_asc(self):
        query = PostgresQuery(self.base_query, model=object)
        query.order(['name'])
        self.assertEqual(query.query, 'SELECT * FROM thing ORDER BY "name" ASC')

    def test_order_desc(self):
        query = PostgresQuery(self.base_query, model=object)
        query.order(['-created_at'])
        self.assertEqual(query.query, 'SELECT * FROM thing ORDER BY "created_at" DESC')

    def test_order_multiple(self):
        query = PostgresQuery(self.base_query, model=object)
        query.order(['-priority', 'name'])
        self.assertEqual(query.query, 'SELECT * FROM thing ORDER BY "priority" DESC, "name" ASC')

    def test_order_none(self):
        query = PostgresQuery(self.base_query, model=object)
        result = query.order(None)
        self.assertEqual(query.query, self.base_query)

    def test_limit(self):
        query = PostgresQuery(self.base_query, model=object)
        query.limit(10)
        self.assertEqual(query.query, 'SELECT * FROM thing LIMIT %s')
        self.assertEqual(query.params, [10])

    def test_offset(self):
        query = PostgresQuery(self.base_query, model=object)
        query.offset(20)
        self.assertEqual(query.query, 'SELECT * FROM thing OFFSET %s')
        self.assertEqual(query.params, [20])

    def test_paginate(self):
        query = PostgresQuery(self.base_query, model=object)
        query.paginate(3, 10)
        self.assertEqual(query.query, 'SELECT * FROM thing LIMIT %s OFFSET %s')
        self.assertEqual(query.params, [10, 20])


# ─── Injection Prevention Tests ───

class TestPostgresQueryInjection(TestCase):
    def test_inject_field_name_semicolon(self):
        query = PostgresQuery('SELECT * FROM thing', model=object)
        with self.assertRaises(ValueError):
            query.filter(**{"id; DROP TABLE thing": 1})

    def test_inject_field_name_quotes(self):
        query = PostgresQuery('SELECT * FROM thing', model=object)
        with self.assertRaises(ValueError):
            query.filter(**{'id"': 1})

    def test_inject_field_name_parens(self):
        query = PostgresQuery('SELECT * FROM thing', model=object)
        with self.assertRaises(ValueError):
            query.filter(**{"id)": 1})

    def test_inject_order_field(self):
        query = PostgresQuery('SELECT * FROM thing', model=object)
        with self.assertRaises(ValueError):
            query.order(["name; DROP TABLE thing"])

    def test_inject_search_field(self):
        query = PostgresQuery('SELECT * FROM thing', model=object)
        with self.assertRaises(ValueError):
            query.search("name'; DROP TABLE thing; --", "test")

    def test_inject_update_field(self):
        query = PostgresQuery('SELECT * FROM thing WHERE id = %s', model=object)
        query.params = [1]
        with self.assertRaises(ValueError):
            query.update(**{"role = 'admin'; --": "hacked"})

    def test_inject_like_pattern(self):
        query = PostgresQuery('SELECT * FROM thing', model=object)
        query.filter(name__contains='100%_done')
        self.assertEqual(query.params, ['%100\\%\\_done%'])

    def test_inject_value_with_sql(self):
        query = PostgresQuery('SELECT * FROM thing', model=object)
        query.filter(name="'; DROP TABLE thing; --")
        self.assertEqual(query.params, ["'; DROP TABLE thing; --"])
        self.assertNotIn('DROP', query.query)


# ─── Connection Tests (mocked) ───

class TestPostgresConnection(TestCase):
    def setUp(self):
        self.conn = PostgresConnection()

    def test_validate_identifier_valid(self):
        self.assertEqual(self.conn._validate_identifier('users'), '"users"')
        self.assertEqual(self.conn._validate_identifier('public.users'), '"public"."users"')
        self.assertEqual(self.conn._validate_identifier('my_table'), '"my_table"')

    def test_validate_identifier_empty(self):
        with self.assertRaises(ValueError):
            self.conn._validate_identifier('')

    def test_validate_identifier_injection(self):
        with self.assertRaises(ValueError):
            self.conn._validate_identifier('users; DROP TABLE users')

    def test_validate_identifier_quotes(self):
        with self.assertRaises(ValueError):
            self.conn._validate_identifier('users"')

    def test_connect(self):
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value = mock_conn
        self.conn.connect()
        mock_psycopg.connect.assert_called_once()
        self.assertIsNotNone(self.conn._connection)

    def test_insert_validates_identifiers(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [('id',)]
        mock_cursor.fetchall.return_value = [{'id': 1}]
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg.connect.return_value = mock_conn
        self.conn.connect()

        self.conn.insert({'name': 'test', 'age': 30}, 'users')
        executed_query = mock_cursor.execute.call_args[0][0]
        self.assertIn('"name"', executed_query)
        self.assertIn('"age"', executed_query)
        self.assertIn('"users"', executed_query)
        self.assertIn('RETURNING id', executed_query)

    def test_insert_rejects_bad_table(self):
        with self.assertRaises(ValueError):
            self.conn.insert({'name': 'test'}, 'users; DROP TABLE users')

    def test_insert_rejects_bad_column(self):
        with self.assertRaises(ValueError):
            self.conn.insert({'name"; DROP TABLE users': 'test'}, 'users')

    def test_insert_many_empty(self):
        # Should not raise
        self.conn.insert_many([], 'users')

    def test_default_query(self):
        self.assertEqual(
            PostgresConnection.default_query('users'),
            'SELECT * FROM users'
        )
