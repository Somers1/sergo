from unittest import TestCase

from sergo.query import TransactSQLQuery


class TestTransactSQLEngine(TestCase):
    def setUp(self):
        self.base_query = 'SELECT * FROM thing'

    def test_filter_single_condition(self):
        query = TransactSQLQuery(self.base_query, model=object)
        query.filter(id=1)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE id = ?')
        self.assertEqual(query.params, [1])

    def test_filter_multiple_conditions(self):
        query = TransactSQLQuery(self.base_query, model=object)
        query.filter(id=1, name='John')
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE id = ? AND name = ?')
        self.assertEqual(query.params, [1, 'John'])

    def test_filter_in_operation(self):
        query = TransactSQLQuery(self.base_query, model=object)
        query.filter(id__in=[1, 2, 3])
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE id IN (?, ?, ?)')
        self.assertEqual(query.params, [1, 2, 3])

    def test_filter_startswith_operation(self):
        query = TransactSQLQuery(self.base_query, model=object)
        query.filter(name__startswith='Jo')
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE name LIKE ?')
        self.assertEqual(query.params, ['Jo%'])

    def test_filter_contains_operation(self):
        query = TransactSQLQuery(self.base_query, model=object)
        query.filter(description__contains='important')
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE description LIKE ?')
        self.assertEqual(query.params, ['%important%'])

    def test_filter_gt_operation(self):
        query = TransactSQLQuery(self.base_query, model=object)
        query.filter(age__gt=30)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE age > ?')
        self.assertEqual(query.params, [30])

    def test_filter_multiple_operations(self):
        query = TransactSQLQuery(self.base_query, model=object)
        query.filter(name__startswith='J', age__gte=25, status__in=['active', 'pending'])
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE name LIKE ? AND age >= ? AND status IN (?, ?)')
        self.assertEqual(query.params, ['J%', 25, 'active', 'pending'])

    def test_filter_with_existing_where_clause(self):
        query = TransactSQLQuery('SELECT * FROM thing WHERE created_at > ?', model=object)
        query.params = ['2023-01-01']
        query.filter(status='active')
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE created_at > ? AND status = ?')
        self.assertEqual(query.params, ['2023-01-01', 'active'])

    def test_exclude_single_condition(self):
        query = TransactSQLQuery(self.base_query, model=object)
        query.exclude(id=1)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE NOT (id = ?)')
        self.assertEqual(query.params, [1])

    def test_exclude_multiple_conditions(self):
        query = TransactSQLQuery(self.base_query, model=object)
        query.exclude(status='inactive', is_deleted=True)
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE NOT (status = ?) AND NOT (is_deleted = ?)')
        self.assertEqual(query.params, ['inactive', True])

    def test_exclude_in_operation(self):
        query = TransactSQLQuery(self.base_query, model=object)
        query.exclude(category__in=['spam', 'trash'])
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE NOT (category IN (?, ?))')
        self.assertEqual(query.params, ['spam', 'trash'])

    def test_exclude_contains_operation(self):
        query = TransactSQLQuery(self.base_query, model=object)
        query.exclude(email__contains='spam')
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE NOT (email LIKE ?)')
        self.assertEqual(query.params, ['%spam%'])

    def test_exclude_with_existing_where_clause(self):
        query = TransactSQLQuery('SELECT * FROM thing WHERE created_at > ?', model=object)
        query.params = ['2023-01-01']
        query.exclude(status='inactive')
        self.assertEqual(query.query, 'SELECT * FROM thing WHERE created_at > ? AND NOT (status = ?)')
        self.assertEqual(query.params, ['2023-01-01', 'inactive'])

    def test_combined_filter_and_exclude(self):
        query = TransactSQLQuery(self.base_query, model=object)
        query.filter(category='electronics', price__gt=100).exclude(brand__in=['BrandX', 'BrandY'])
        expected_query = ('SELECT * FROM thing WHERE category = ? AND price > ? '
                          'AND NOT (brand IN (?, ?))')
        self.assertEqual(query.query, expected_query)
        self.assertEqual(query.params, ['electronics', 100, 'BrandX', 'BrandY'])