from functools import cached_property
from typing import Dict, List, Any
from sergo.query import Query
from sergo import fields


class Serializer:
    model_class = None
    fields = ['__all__']

    @property
    def many(self):
        return isinstance(self.data, list)

    @property
    def internal_data(self):
        return self.to_internal_value()

    @property
    def objects(self):
        if self.many:
            return [self.model_class(**item) for item in self.internal_data]
        return self.model_class(**self.internal_data)

    def __init__(self, data: Dict | Query | List[Dict], instance=None):
        self.raw_data = data
        self.data = self.to_representation()
        self.instance = instance

    def to_representation(self) -> List[Dict[str, Any]] | Dict[str, Any]:
        if isinstance(self.raw_data, Query):
            data = self.raw_data.execute()
        else:
            data = self.raw_data
        if isinstance(data, list):
            return [self.represent_obj(obj) for obj in data]
        else:
            return self.represent_obj(data)

    def represent_obj(self, obj: Any) -> Dict[str, Any]:
        if not self.model_class:
            raise ValueError("model_class must be set")
        if self.fields == ['__all__']:
            self.fields = list(self.model_class._meta.fields.keys())
        return {field: self.get_field_value(obj, field) for field in self.fields}

    def get_field_value(self, obj: Any, field: str) -> Any:
        value = getattr(obj, field, None) if not isinstance(obj, dict) else obj.get(field)
        field_instance = self.model_class._meta.get_field(field)
        if isinstance(field_instance, fields.MethodField):
            return field_instance.to_representation(value, field)
        else:
            return field_instance.to_representation(value)

    def to_internal_value(self) -> Dict[str, Any] | List[Dict[str, Any]]:
        if isinstance(self.data, list):
            return [self.internal_value_obj(item) for item in self.data]
        else:
            return self.internal_value_obj(self.data)

    def internal_value_obj(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.model_class:
            raise ValueError("model_class must be set")
        if self.fields == ['__all__']:
            self.fields = list(self.model_class._meta.fields.keys())
        return {field: self.get_internal_value(data, field) for field in self.fields}

    def get_internal_value(self, data: Dict[str, Any], field: str) -> Any:
        value = data.get(field)
        field_instance = self.model_class._meta.get_field(field)
        if isinstance(field_instance, fields.MethodField):
            return field_instance.to_internal_value(value, field)
        else:
            return field_instance.to_internal_value(value)

    def save(self):
        if self.many:
            return self.model_class.objects.bulk_create(self.internal_data)
        internal_data = self.internal_data
        internal_data.pop('id')
        if self.instance:
            self.model_class.objects.filter(id=self.instance.id).update(**internal_data)
            self.instance = self.model_class.objects.get(id=self.instance.id)
        else:
            self.instance = self.model_class.objects.create(**internal_data)
        return self.instance
