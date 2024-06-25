from typing import Dict, List, Any
from sergo.query import Query
from sergo import fields


class Serializer:
    model_class = None
    fields = []

    @property
    def many(self):
        return isinstance(self.data, list)

    def __init__(self, data: Dict | Query | List[Dict], instance=None):
        self.raw_data = data
        self.data = self.serialize()
        self.instance = instance

    def serialize(self) -> List[Dict[str, Any]] | Dict[str, Any]:
        if isinstance(self.raw_data, Query):
            data = self.raw_data.execute()
        else:
            data = self.raw_data
        if isinstance(data, list):
            return [self.serialize_obj(obj) for obj in data]
        else:
            return self.serialize_obj(data)

    def serialize_obj(self, obj: Dict[str, Any]) -> Dict[str, Any]:
        if not self.model_class:
            raise ValueError("model_class must be set")
        if self.fields == ['__all__']:
            self.fields = list(self.model_class._meta.fields.keys())
        return {field: self.get_field_value(obj, field) for field in self.fields}

    def get_field_value(self, obj: Dict[str, Any], field: str) -> Any:
        value = obj.get(field) if isinstance(obj, dict) else getattr(obj, field, None)
        field_instance = self.model_class._meta.get_field(field)
        if isinstance(field_instance, fields.MethodField):
            return field_instance.to_internal_value(value, field)
        else:
            return field_instance.to_internal_value(value)

    def save(self):
        if self.many:
            raise NotImplementedError("Bulk create is not implemented")
        if self.instance:
            self.model_class.objects.filter(id=self.instance.id).update(**self.data)
            self.instance = self.model_class.objects.get(id=self.instance.id)
        else:
            self.instance = self.model_class.objects.create(**self.data)
        return self.instance
