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


class _LazyQuery:
    """Lazy proxy that resolves the Query class on first access."""
    _resolved = None

    def __call__(self, *args, **kwargs):
        if _LazyQuery._resolved is None:
            _LazyQuery._resolved = utils.import_string(settings.QUERY_ENGINE)
        return _LazyQuery._resolved(*args, **kwargs)

    def __getattr__(self, name):
        if _LazyQuery._resolved is None:
            _LazyQuery._resolved = utils.import_string(settings.QUERY_ENGINE)
        return getattr(_LazyQuery._resolved, name)


Query = _LazyQuery()
