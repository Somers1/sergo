import unittest

from sergo import fields
from sergo.connection import connection
from sergo.handlers import get_handler
from sergo.model import Model
from sergo.request import StandardizedRequest
from sergo.serializer import Serializer
from sergo.viewset import ViewSet

handler = get_handler()


class Test(Model):
    id = fields.IntegerField()
    name = fields.StringField()

    class Meta:
        db_table = 'test_sergo_test'


class TestSerializer(Serializer):
    model_class = Test
    fields = ['__all__']


class TestViewSet(ViewSet):
    model_class = Test
    serializer_class = TestSerializer
    filter_fields = ['id', 'name']
    search_fields = ['name']
    order_fields = ['id', 'name']


urlpatterns = {
    '/api/test': TestViewSet
}


class TestSergo(unittest.TestCase):
    def setUp(self) -> None:
        connection.cursor.execute("DROP TABLE IF EXISTS test_sergo_test")
        connection.cursor.execute("CREATE TABLE test_sergo_test (id INT, name VARCHAR)")
        connection.cursor.execute("INSERT INTO test_sergo_test (id, name) VALUES (1, 'A'), (3, 'B'), (3, 'C')")
        connection.cursor.commit()
        handler.find_urlpatterns = lambda: urlpatterns

    def tearDown(self) -> None:
        connection.cursor.execute("DROP TABLE test_sergo_test")
        connection.cursor.commit()
        connection.close()

    def test_handle_basic(self):
        request = StandardizedRequest(method="GET", url="/api/test", headers={}, query_params={}, body=None)
        response = handler.process_request(request)
        self.assertEqual(response.body, [{"id": 1, "name": "A"}, {"id": 3, "name": "B"}, {"id": 3, "name": "C"}])

    def test_handle_filter(self):
        request = StandardizedRequest(method="GET", url="/api/test", headers={}, query_params={'id': '1'}, body=None)
        response = handler.process_request(request)
        self.assertEqual(response.body, [{"id": 1, "name": "A"}])

    def test_handle_ordering(self):
        request = StandardizedRequest(
            method="GET", url="/api/test", headers={}, query_params={'ordering': '-id,-name'}, body=None)
        response = handler.process_request(request)
        self.assertEqual(response.body, [{"id": 3, "name": "C"}, {"id": 3, "name": "B"}, {"id": 1, "name": "A"}])

    def test_handle_search(self):
        request = StandardizedRequest(
            method="GET", url="/api/test", headers={}, query_params={'search': 'A', 'only': 'name'}, body=None)
        response = handler.process_request(request)
        self.assertEqual(response.body, [{"id": 1, "name": "A"}])

    def test_handle_pagination(self):
        request = StandardizedRequest(
            method="GET", url="/api/test", headers={},
            query_params={'pagination': 'True', 'ordering': 'id,name', 'page_size': 1}, body=None)
        response = handler.process_request(request)
        body = {"count": 3,
                "next": '/api/test?page=2&page_size=1&pagination=True&ordering=id,name',
                "previous": None,
                "results": [{"id": 1, "name": "A"}]}
        self.assertEqual(response.body, body)

    def test_model_create(self):
        test = Test.objects.create(id=4, name="D")
        self.assertEqual(test.id, 4)

    def test_handle_post(self):
        request = StandardizedRequest(
            method="POST", url="/api/test", headers={}, query_params={}, body={"id": 4, "name": "D"})
        response = handler.process_request(request)
        test = Test.objects.filter(id=4, name="D").execute()
        self.assertEqual(response.body, {"id": 4, "name": "D"})
        self.assertEqual(len(test), 1)

    def test_handle_patch(self):
        body = {"id": 1, "name": "B"}
        request = StandardizedRequest(
            method="PATCH", url="/api/test", headers={}, query_params={}, body=body)
        response = handler.process_request(request)
        test = Test.objects.get(id=1, name="B")
        self.assertEqual(response.body, body)
        self.assertEqual(test.id, 1)

    def test_handle_delete(self):
        request = StandardizedRequest(method="DELETE", url="/api/test", headers={}, query_params={}, body={"id": 1})
        response = handler.process_request(request)
        results = Test.objects.filter(id=1).execute()
        self.assertEqual(response.body, 'Success')
        self.assertEqual(results, [])


if __name__ == '__main__':
    unittest.main()
