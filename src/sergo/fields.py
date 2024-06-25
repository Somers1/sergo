import json
from abc import ABC, abstractmethod


class Field(ABC):
    @abstractmethod
    def to_internal_value(self, value):
        pass


class IntegerField(Field):
    def to_internal_value(self, value):
        return int(value)


class RelatedField(Field):
    def to_internal_value(self, value):
        return value


class FloatField(Field):
    def to_internal_value(self, value):
        return value


class TimeField(Field):
    def to_internal_value(self, value):
        return value.isoformat()


class DecimalField(Field):
    def to_internal_value(self, value):
        try:
            return float(value)
        except TypeError:
            return None


class StringField(Field):
    def to_internal_value(self, value):
        return str(value)


class DateField(Field):
    def to_internal_value(self, value):
        return value.isoformat()


class BoolField(Field):
    def to_internal_value(self, value):
        return bool(value)


class ArrayField(Field):
    def to_internal_value(self, value):
        return value


class MethodField(Field):
    def to_internal_value(self, value, key):
        return getattr(self, f'get_{key}')(value)
