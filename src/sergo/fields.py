from datetime import datetime
from abc import ABC, abstractmethod


class Field(ABC):
    def __init__(self, optional=True, readonly=False):
        self.optional = optional
        self.readonly = readonly

    def to_internal_value(self, value):
        if self.optional and value is None:
            return None
        return self._to_internal_value(value)

    def to_representation(self, value):
        if self.optional and value is None:
            return None
        return self._to_representation(value)

    @abstractmethod
    def _to_internal_value(self, value):
        pass

    @abstractmethod
    def _to_representation(self, value):
        pass


class ForeignKey(Field):
    def _to_internal_value(self, value):
        return value

    def _to_representation(self, value):
        return value


class IntegerField(Field):
    def _to_internal_value(self, value):
        return int(value)

    def _to_representation(self, value):
        return int(value)


class IDField(IntegerField):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.readonly = True


class RelatedField(Field):
    def _to_internal_value(self, value):
        return value

    def _to_representation(self, value):
        return value


class FloatField(Field):
    def _to_internal_value(self, value):
        return float(value)

    def _to_representation(self, value):
        return float(value)


class TimeField(Field):
    def _to_internal_value(self, value):
        return datetime.fromisoformat(value)

    def _to_representation(self, value):
        return value.isoformat()


class DecimalField(Field):
    def _to_internal_value(self, value):
        return float(value)

    def _to_representation(self, value):
        return float(value)


class StringField(Field):
    def _to_internal_value(self, value):
        if not value:
            return None
        return str(value)

    def _to_representation(self, value):
        if not value:
            return None
        return str(value)


class DateTimeField(Field):
    def _value_to_datetime(self, value):
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        if isinstance(value, int):
            if value == 0:
                return None
            if len(str(value)) >= 10:
                value = value / 1000
            return datetime.utcfromtimestamp(value)
        return value

    def _to_internal_value(self, value):
        return self._value_to_datetime(value)

    def _to_representation(self, value):
        dt = self._value_to_datetime(value)
        return dt.isoformat() if dt else None


class BoolField(Field):
    def _to_internal_value(self, value):
        return bool(value)

    def _to_representation(self, value):
        return bool(value)


class ArrayField(Field):
    def _to_internal_value(self, value):
        return value

    def _to_representation(self, value):
        return value


class MethodField(Field):
    def to_internal_value(self, value, key):
        if self.optional and value is None:
            return None
        return getattr(self, f'get_{key}')(value)

    def to_representation(self, value, key):
        if self.optional and value is None:
            return None
        return getattr(self, f'get_{key}')(value)
