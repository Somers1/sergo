from collections import OrderedDict


class Paginator:
    DEFAULT_PAGE_SIZE = 20

    def __init__(self, query, request, serializer_class):
        self.query = query
        self.request = request
        self.serializer_class = serializer_class

    def append_params(self, page_url):
        for param, value in self.request.query_params.items():
            if param not in ['page', 'page_size']:
                page_url += f'&{param}={value}'
        return page_url

    def get_next_page(self, page, page_size, count):
        if count <= page_size * page:
            return None
        return self.append_params(f"{self.request.path}?page={page + 1}&page_size={page_size}")

    def get_previous_page(self, page, page_size, count):
        if count <= page_size or page == 1:
            return None
        return self.append_params(f"{self.request.path}?page={page - 1}&page_size={page_size}")

    def get_paginated_response(self):
        count = self.query.count()
        page_size = self.request.query_params.get('page_size', self.DEFAULT_PAGE_SIZE)
        page = self.request.query_params.get('page', 1)
        self.query.paginate(int(page), int(page_size))
        data = self.serializer_class(data=self.query).data
        return OrderedDict([
            ('count', count),
            ('next', self.get_next_page(page, page_size, count)),
            ('previous', self.get_previous_page(page, page_size, count)),
            ('results', data)
        ])
