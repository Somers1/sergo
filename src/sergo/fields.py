from datetime import datetime
from abc import ABC, abstractmethod

class Field(ABC):
    def __init__(self, optional=True):
        self.optional = optional

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

class IntegerField(Field):
    def _to_internal_value(self, value):
        return int(value)

    def _to_representation(self, value):
        return int(value)

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
        return str(value)

    def _to_representation(self, value):
        return str(value)

class DateField(Field):
    def _to_internal_value(self, value):
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        return value

    def _to_representation(self, value):
        return value.isoformat()

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