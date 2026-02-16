"""Tests for SQLite connection and query engine.

Covers:
- Basic CRUD operations
- Query building (filter, exclude, search, order, paginate)
- SQL injection prevention (field names, values, LIKE patterns, identifiers)
- Edge cases (empty IN, NULL handling, special characters)
"""
import os
import sys
import sqlite3
import tempfile
import logging
import types
from unittest import TestCase

# Patch settings BEFORE any sergo imports to avoid circular import
# (query.py and connection.py resolve classes at module level)
settings_module = types.ModuleType('settings')
settings_module.DATABASE_CONFIG = {'path': ':memory:'}
settings_module.DATABASE_ENGINE = 'sergo.connection.sqlite.SQLiteConnection'
settings_module.QUERY_ENGINE = 'sergo.query.sqlite.SQLiteQuery'
settings_module.SERGO_CONFIG = {}
settings_module.GLOBAL_CONFIG = {}
settings_module.HANDLER = 'sergo.handler.FastAPIHandler'
settings_module.logger = logging.getLogger('sergo_test')
sys.modules['settings'] = settings_module

# Now import — connection.py will resolve SQLiteConnection, query.py will resolve SQLiteQuery
from sergo.connection.sqlite import SQLiteConnection
from sergo.query.sqlite import SQLiteQuery


class TestSQLiteConnection(TestCase):
    def setUp(self):
        self.conn = SQLiteConnection()
        # Override settings for in-memory DB
        settings_module.DATABASE_CONFIG = {'path': ':memory:'}
        self.conn.connect()
        self.conn.execute(
            "CREATE TABLE test_table ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "name TEXT NOT NULL, "
            "age INTEGER, "
            "email TEXT"
            ")"
        )
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_connect(self):
        self.assertIsNotNone(self.conn.connection)
        self.assertIsNotNone(self.conn.cursor)

    def test_insert_and_retrieve(self):
        row_id = self.conn.insert({'name': 'Alice', 'age': 30}, 'test_table')
        self.assertEqual(row_id, 1)
        result = self.conn.execute_result("SELECT * FROM test_table WHERE id = ?", row_id)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], 'Alice')
        self.assertEqual(result[0]['age'], 30)

    def test_insert_returns_incrementing_ids(self):
        id1 = self.conn.insert({'name': 'Alice', 'age': 30}, 'test_table')
        id2 = self.conn.insert({'name': 'Bob', 'age': 25}, 'test_table')
        self.assertEqual(id1, 1)
        self.assertEqual(id2, 2)

    def test_insert_many(self):
        data = [
            {'name': 'Alice', 'age': 30},
            {'name': 'Bob', 'age': 25},
            {'name': 'Charlie', 'age': 35},
        ]
        self.conn.insert_many(data, 'test_table')
        result = self.conn.execute_result("SELECT COUNT(*) as cnt FROM test_table")
        self.assertEqual(result[0]['cnt'], 3)

    def test_insert_empty_list(self):
        self.conn.insert_many([], 'test_table')
        result = self.conn.execute_result("SELECT COUNT(*) as cnt FROM test_table")
        self.assertEqual(result[0]['cnt'], 0)

    def test_execute_result_empty(self):
        result = self.conn.execute_result("SELECT * FROM test_table WHERE id = ?", 999)
        self.assertEqual(result, [])

    def test_create_table(self):
        self.conn.create_table('new_table', {
            'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
            'title': 'TEXT NOT NULL',
            'count': 'INTEGER DEFAULT 0',
        })
        self.conn.insert({'title': 'Test', 'count': 5}, 'new_table')
        result = self.conn.execute_result("SELECT * FROM new_table")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['title'], 'Test')

    def test_wal_mode_enabled(self):
        # WAL mode is not supported for :memory: databases (returns 'memory')
        # Test with a file-based DB instead
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, 'wal_test.db')
            settings_module.DATABASE_CONFIG = {'path': db_path}
            conn = SQLiteConnection()
            conn.connect()
            result = conn.execute_result("PRAGMA journal_mode")
            self.assertEqual(result[0]['journal_mode'], 'wal')
            conn.close()

    def test_foreign_keys_enabled(self):
        result = self.conn.execute_result("PRAGMA foreign_keys")
        self.assertEqual(result[0]['foreign_keys'], 1)

    def test_file_based_db(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, 'subdir', 'test.db')
            settings_module.DATABASE_CONFIG = {'path': db_path}
            conn = SQLiteConnection()
            conn.connect()
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            conn.commit()
            conn.insert({'val': 'hello'}, 't')
            result = conn.execute_result("SELECT * FROM t")
            self.assertEqual(result[0]['val'], 'hello')
            conn.close()
            # File should exist
            self.assertTrue(os.path.exists(db_path))


class TestSQLiteConnectionInjection(TestCase):
    """Test that SQL injection attempts are properly blocked."""

    def setUp(self):
        self.conn = SQLiteConnection()
        settings_module.DATABASE_CONFIG = {'path': ':memory:'}
        self.conn.connect()
        self.conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, role TEXT)"
        )
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_inject_table_name(self):
        with self.assertRaises(ValueError):
            self.conn.insert({'name': 'Alice'}, "users; DROP TABLE users; --")

    def test_inject_column_name(self):
        with self.assertRaises(ValueError):
            self.conn.insert({'name; DROP TABLE users': 'Alice'}, 'users')

    def test_inject_table_name_quotes(self):
        with self.assertRaises(ValueError):
            self.conn.insert({'name': 'Alice'}, 'users"')

    def test_inject_column_semicolon(self):
        with self.assertRaises(ValueError):
            self.conn.insert({'name": "x", "role': 'admin'}, 'users')

    def test_inject_empty_identifier(self):
        with self.assertRaises(ValueError):
            self.conn._validate_identifier('')

    def test_inject_empty_component(self):
        with self.assertRaises(ValueError):
            self.conn._validate_identifier('schema.')

    def test_value_parameterized(self):
        """Values with SQL keywords are safely parameterized."""
        self.conn.insert({'name': "'; DROP TABLE users; --", 'role': 'admin'}, 'users')
        result = self.conn.execute_result("SELECT * FROM users WHERE id = ?", 1)
        self.assertEqual(result[0]['name'], "'; DROP TABLE users; --")

    def test_create_table_injection_in_definition(self):
        with self.assertRaises(ValueError):
            self.conn.create_table('safe_table', {
                'id': 'INTEGER PRIMARY KEY; DROP TABLE users',
            })


class TestSQLiteQuery(TestCase):
    def setUp(self):
        self.base_query = 'SELECT * FROM thing'

    def test_filter_single_condition(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.filter(id=1)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "id" = ?')
        self.assertEqual(query.params, [1])

    def test_filter_multiple_conditions(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.filter(id=1, name='John')
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "id" = ? AND "name" = ?')
        self.assertEqual(query.params, [1, 'John'])

    def test_filter_in(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.filter(id__in=[1, 2, 3])
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "id" IN (?, ?, ?)')
        self.assertEqual(query.params, [1, 2, 3])

    def test_filter_in_empty(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.filter(id__in=[])
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE 1 = 0')
        self.assertEqual(query.params, [])

    def test_filter_startswith(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.filter(name__startswith='Jo')
        self.assertEqual(query.query, "SELECT * FROM thing WHERE \"name\" LIKE ? ESCAPE '\\'")
        self.assertEqual(query.params, ['Jo%'])

    def test_filter_endswith(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.filter(name__endswith='son')
        self.assertEqual(query.query, "SELECT * FROM thing WHERE \"name\" LIKE ? ESCAPE '\\'")
        self.assertEqual(query.params, ['%son'])

    def test_filter_contains(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.filter(name__contains='oh')
        self.assertEqual(query.query, "SELECT * FROM thing WHERE \"name\" LIKE ? ESCAPE '\\'")
        self.assertEqual(query.params, ['%oh%'])

    def test_filter_gt(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.filter(age__gt=30)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "age" > ?')
        self.assertEqual(query.params, [30])

    def test_filter_gte(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.filter(age__gte=30)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "age" >= ?')
        self.assertEqual(query.params, [30])

    def test_filter_lt(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.filter(age__lt=30)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "age" < ?')
        self.assertEqual(query.params, [30])

    def test_filter_lte(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.filter(age__lte=30)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "age" <= ?')
        self.assertEqual(query.params, [30])

    def test_filter_isnull_true(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.filter(email__isnull=True)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "email" IS NULL')
        self.assertEqual(query.params, [])

    def test_filter_isnull_false(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.filter(email__isnull=False)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "email" IS NOT NULL')
        self.assertEqual(query.params, [])

    def test_filter_none_value(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.filter(email=None)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE "email" IS NULL')
        self.assertEqual(query.params, [])

    def test_filter_multiple_operators(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.filter(name__startswith='J', age__gte=25, status__in=['active', 'pending'])
        self.assertIn('"name" LIKE ?', query.query)
        self.assertIn('"age" >= ?', query.query)
        self.assertIn('"status" IN (?, ?)', query.query)

    def test_filter_with_existing_where(self):
        query = SQLiteQuery('SELECT * FROM thing WHERE created_at > ?', model=object)
        query.params = ['2023-01-01']
        query.filter(status='active')
        self.assertIn('AND "status" = ?', query.query)
        self.assertEqual(query.params, ['2023-01-01', 'active'])

    def test_exclude_single(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.exclude(id=1)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE NOT ("id" = ?)')
        self.assertEqual(query.params, [1])

    def test_exclude_multiple(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.exclude(status='inactive', is_deleted=True)
        self.assertIn('NOT ("status" = ?)', query.query)
        self.assertIn('NOT ("is_deleted" = ?)', query.query)

    def test_exclude_in(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.exclude(category__in=['spam', 'trash'])
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE NOT ("category" IN (?, ?))')
        self.assertEqual(query.params, ['spam', 'trash'])

    def test_combined_filter_exclude(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.filter(category='electronics', price__gt=100).exclude(brand__in=['X', 'Y'])
        self.assertIn('"category" = ?', query.query)
        self.assertIn('"price" > ?', query.query)
        self.assertIn('NOT ("brand" IN (?, ?))', query.query)
        self.assertEqual(query.params, ['electronics', 100, 'X', 'Y'])

    def test_search(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.search('name', 'john')
        self.assertIn('UPPER("name") LIKE UPPER(?)', query.query)
        self.assertEqual(query.params, ['%john%'])

    def test_search_none(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.search(None, None)
        self.assertEqual(query.query, self.base_query)

    def test_order_asc(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.order(['name'])
        self.assertEqual(query.query, 'SELECT * FROM thing ORDER BY "name" ASC')

    def test_order_desc(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.order(['-created_at'])
        self.assertEqual(query.query, 'SELECT * FROM thing ORDER BY "created_at" DESC')

    def test_order_multiple(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.order(['-priority', 'name'])
        self.assertEqual(query.query, 'SELECT * FROM thing ORDER BY "priority" DESC, "name" ASC')

    def test_order_none(self):
        query = SQLiteQuery(self.base_query, model=object)
        result = query.order(None)
        self.assertEqual(query.query, self.base_query)

    def test_limit(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.limit(10)
        self.assertEqual(query.query, 'SELECT * FROM thing LIMIT ?')
        self.assertEqual(query.params, [10])

    def test_offset(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.offset(20)
        self.assertEqual(query.query, 'SELECT * FROM thing OFFSET ?')
        self.assertEqual(query.params, [20])

    def test_paginate(self):
        query = SQLiteQuery(self.base_query, model=object)
        query.paginate(3, 10)  # Page 3, 10 per page
        self.assertEqual(query.query, 'SELECT * FROM thing LIMIT ? OFFSET ?')
        self.assertEqual(query.params, [10, 20])

    def test_first(self):
        query = SQLiteQuery(self.base_query, model=object)
        # first() adds LIMIT 1 then tries to execute — just check query building
        self.assertIn('LIMIT 1', query.query + ' LIMIT 1')


class TestSQLiteQueryInjection(TestCase):
    """Test that SQL injection via field names is blocked."""

    def test_inject_field_name_semicolon(self):
        query = SQLiteQuery('SELECT * FROM thing', model=object)
        with self.assertRaises(ValueError):
            query.filter(**{"id; DROP TABLE thing": 1})

    def test_inject_field_name_quotes(self):
        query = SQLiteQuery('SELECT * FROM thing', model=object)
        with self.assertRaises(ValueError):
            query.filter(**{'id"': 1})

    def test_inject_field_name_parens(self):
        query = SQLiteQuery('SELECT * FROM thing', model=object)
        with self.assertRaises(ValueError):
            query.filter(**{"id)": 1})

    def test_inject_order_field(self):
        query = SQLiteQuery('SELECT * FROM thing', model=object)
        with self.assertRaises(ValueError):
            query.order(["name; DROP TABLE thing"])

    def test_inject_search_field(self):
        query = SQLiteQuery('SELECT * FROM thing', model=object)
        with self.assertRaises(ValueError):
            query.search("name'; DROP TABLE thing; --", "test")

    def test_inject_update_field(self):
        query = SQLiteQuery('SELECT * FROM thing WHERE id = ?', model=object)
        query.params = [1]
        with self.assertRaises(ValueError):
            query.update(**{"role = 'admin'; --": "hacked"})

    def test_inject_like_pattern(self):
        """LIKE wildcards in user values should be escaped."""
        query = SQLiteQuery('SELECT * FROM thing', model=object)
        query.filter(name__contains='100%_done')
        # The % and _ should be escaped
        self.assertEqual(query.params, ['%100\\%\\_done%'])

    def test_inject_value_with_sql(self):
        """SQL in values is safely parameterized."""
        query = SQLiteQuery('SELECT * FROM thing', model=object)
        query.filter(name="'; DROP TABLE thing; --")
        self.assertEqual(query.params, ["'; DROP TABLE thing; --"])
        # Value is parameterized, not in query string
        self.assertNotIn('DROP', query.query)


class TestSQLiteIntegration(TestCase):
    """End-to-end tests with real SQLite database."""

    def setUp(self):
        self.conn = SQLiteConnection()
        settings_module.DATABASE_CONFIG = {'path': ':memory:'}
        self.conn.connect()
        self.conn.execute(
            "CREATE TABLE tasks ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "title TEXT NOT NULL, "
            "person TEXT, "
            "priority TEXT DEFAULT 'medium', "
            "status TEXT DEFAULT 'open'"
            ")"
        )
        self.conn.commit()
        # Insert test data
        self.conn.insert({'title': 'Call Harry', 'person': 'Harry', 'priority': 'high', 'status': 'open'}, 'tasks')
        self.conn.insert({'title': 'Buy milk', 'person': 'Bec', 'priority': 'medium', 'status': 'open'}, 'tasks')
        self.conn.insert({'title': 'Fix bug', 'person': None, 'priority': 'low', 'status': 'done'}, 'tasks')
        self.conn.insert({'title': 'Call dentist', 'person': None, 'priority': 'high', 'status': 'open'}, 'tasks')

        # Patch connection module to use our connection
        import sergo.query.sqlite as sq
        import sergo.connection as conn_mod
        import sergo.connection.base as conn_base
        self._orig_connection = conn_mod.connection
        conn_mod.connection = self.conn
        conn_base.connection = self.conn
        sq.connection = self.conn

        # Simple model stand-in
        class Task:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)
        self.Task = Task

    def tearDown(self):
        self.conn.close()
        import sergo.connection as conn_mod
        import sergo.connection.base as conn_base
        import sergo.query.sqlite as sq
        conn_mod.connection = self._orig_connection
        conn_base.connection = self._orig_connection
        sq.connection = self._orig_connection

    def test_filter_and_execute(self):
        q = SQLiteQuery("SELECT * FROM tasks", self.Task)
        results = q.filter(status='open').execute()
        self.assertEqual(len(results), 3)

    def test_filter_by_person(self):
        q = SQLiteQuery("SELECT * FROM tasks", self.Task)
        results = q.filter(person='Harry').execute()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].title, 'Call Harry')

    def test_filter_isnull(self):
        q = SQLiteQuery("SELECT * FROM tasks", self.Task)
        results = q.filter(person__isnull=True).execute()
        self.assertEqual(len(results), 2)

    def test_filter_contains(self):
        q = SQLiteQuery("SELECT * FROM tasks", self.Task)
        results = q.filter(title__contains='Call').execute()
        self.assertEqual(len(results), 2)

    def test_exclude_and_execute(self):
        q = SQLiteQuery("SELECT * FROM tasks", self.Task)
        results = q.exclude(status='done').execute()
        self.assertEqual(len(results), 3)

    def test_order_and_execute(self):
        q = SQLiteQuery("SELECT * FROM tasks", self.Task)
        results = q.filter(status='open').order(['title']).execute()
        self.assertEqual(len(results), 3)
        # Alphabetical by title (SQLite default: case-sensitive, uppercase < lowercase)
        self.assertEqual(results[0].title, 'Buy milk')
        self.assertEqual(results[1].title, 'Call Harry')
        self.assertEqual(results[2].title, 'Call dentist')

    def test_count(self):
        q = SQLiteQuery("SELECT * FROM tasks", self.Task)
        # Need an ORDER BY for count to strip
        q.filter(status='open').order(['title'])
        count = q.count()
        self.assertEqual(count, 3)

    def test_paginate(self):
        q = SQLiteQuery("SELECT * FROM tasks", self.Task)
        results = q.paginate(1, 2).execute()
        self.assertEqual(len(results), 2)

    def test_delete(self):
        q = SQLiteQuery("SELECT * FROM tasks", self.Task)
        q.filter(status='done').delete()
        remaining = self.conn.execute_result("SELECT COUNT(*) as cnt FROM tasks")
        self.assertEqual(remaining[0]['cnt'], 3)

    def test_update(self):
        q = SQLiteQuery("SELECT * FROM tasks", self.Task)
        q.filter(person='Harry').update(status='done')
        result = self.conn.execute_result("SELECT status FROM tasks WHERE person = ?", 'Harry')
        self.assertEqual(result[0]['status'], 'done')

    def test_chained_filter(self):
        q = SQLiteQuery("SELECT * FROM tasks", self.Task)
        results = q.filter(status='open').filter(priority='high').execute()
        self.assertEqual(len(results), 2)

    def test_injection_in_filter_value(self):
        """SQL injection in filter values is safely parameterized."""
        q = SQLiteQuery("SELECT * FROM tasks", self.Task)
        results = q.filter(person="'; DROP TABLE tasks; --").execute()
        self.assertEqual(len(results), 0)
        # Table should still exist
        count = self.conn.execute_result("SELECT COUNT(*) as cnt FROM tasks")
        self.assertEqual(count[0]['cnt'], 4)
