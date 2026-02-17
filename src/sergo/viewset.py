from sergo.pagination import Paginator
from sergo.query import Query
from sergo.request import StandardizedRequest


class ViewSet:
    DEFAULT_METHODS = ['GET', 'POST', 'PATCH', 'DELETE']

    @property
    def viable_methods(self):
        return getattr(self, 'methods', self.DEFAULT_METHODS)

    @property
    def viable_method_handlers(self):
        return {method: getattr(self, f'handle_{method.lower()}') for method in self.viable_methods}

    @property
    def _serializer_class(self):
        return getattr(self, 'serializer_class', self.model_class.get_serializer_class())

    def handle_post(self, request):
        serializer = self._serializer_class(data=request.body)
        serializer.save()
        return serializer.data

    def handle_patch(self, request):
        if query_param_id := request.query_params.get('id'):
            request.body['id'] = query_param_id
        instance = self.model_class.objects.get(id=request.body['id'])
        serializer = self._serializer_class(data=request.body, instance=instance)
        serializer.save()
        return serializer.data

    def handle_delete(self, request):
        try:
            object_id = request.query_params.get('id')
            if not object_id:
                object_id = request.body['id']
        except KeyError:
            raise ValueError("No id provided")
        self.model_class.objects.filter(id=object_id).delete()
        return 'Success'

    def query_param_subset(self, query_params, subset_attr):
        return {key: value for key, value in query_params.items() if key in getattr(self, subset_attr, [])}

    def valid_ordering(self, request):
        if ordering := request.query_params.get('ordering', None):
            return [field for field in ordering.split(',') if field.lstrip('-') in getattr(self, 'order_fields', [])]
        return getattr(self.model_class._meta, 'ordering', None)

    def handle_get(self, request: StandardizedRequest):
        query = Query(self.model_class.objects.query, self.model_class)
        query.filter(**self.query_param_subset(request.query_params, 'filter_fields'))
        search_field = request.query_params.get('only')
        if search_field in getattr(self, 'search_fields', []):
            query.search(field=search_field, value=request.query_params.get('search'))
        if ordering := self.valid_ordering(request):
            query.order(ordering)
        try:
            pagination = eval(request.query_params['pagination'])
        except KeyError:
            pagination = getattr(self, 'pagination', False)
        if pagination:
            if not ordering:
                raise ValueError("Pagination requires ordering")
            return Paginator(query, request, self._serializer_class).get_paginated_response()
        return self._serializer_class(data=query).data
