from sergo import utils
import settings


_connection_instance = None


def _get_connection_class():
    return utils.import_string(settings.DATABASE_ENGINE)


class _LazyConnection:
    """Lazy proxy that resolves the connection on first attribute access."""
    def __getattr__(self, name):
        global _connection_instance
        if _connection_instance is None:
            Connection = _get_connection_class()
            _connection_instance = Connection()
        return getattr(_connection_instance, name)


Connection = None  # Resolved lazily
connection = _LazyConnection()
