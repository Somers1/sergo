from functools import cached_property
from typing import Dict, List, Any, Type
from sergo.query import Query
from sergo import fields


class BaseSerializer:
    pass


class SerializerMetaclass(type):
    def __new__(cls, name, bases, attrs):
        # Move relevant attributes from Meta to the class
        if 'Meta' in attrs:
            meta = attrs['Meta']
            if hasattr(meta, 'model'):
                attrs['model_class'] = meta.model
            if hasattr(meta, 'fields'):
                attrs['fields'] = meta.fields
        new_class = super().__new__(cls, name, bases, attrs)
        declared_fields = {}
        for key, value in attrs.items():
            if isinstance(value, fields.Field):
                declared_fields[key] = value
            if isinstance(value, BaseSerializer):
                declared_fields[key] = value
        new_class._declared_fields = declared_fields
        return new_class


class Serializer(BaseSerializer, metaclass=SerializerMetaclass):
    model_class = None
    fields = None

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

    def __init__(self, data: Dict | Query | List[Dict] = None, instance=None):
        self.raw_data = data
        self.instance = instance
        if data is not None:
            self.data = self.to_representation(self.raw_data)
        elif instance is not None:
            self.data = self.represent_obj(instance)

    def fields_to_serialize(self, data):
        if self.fields is None:
            serialize_fields = []
            for field in self.model_class._meta.writeable_fields.keys():
                if isinstance(data, dict) and field in data:
                    serialize_fields.append(field)
                if isinstance(data, object) and hasattr(data, field):
                    serialize_fields.append(field)
            return serialize_fields
        if '__all__' in self.fields:
            return list(self.model_class._meta.fields.keys()) + list(self._declared_fields.keys())
        return self.fields

    def to_internal_value(self) -> Dict[str, Any] | List[Dict[str, Any]]:
        if self.many:
            return [self.internal_value_obj(item) for item in self.data]
        else:
            return self.internal_value_obj(self.data)

    def to_representation(self, data) -> List[Dict[str, Any]] | Dict[str, Any]:
        if isinstance(data, Query):
            data = data.execute()
        if isinstance(data, list):
            return [self.represent_obj(obj) for obj in data]
        else:
            return self.represent_obj(data)

    def represent_obj(self, obj: Any) -> Dict[str, Any]:
        return {field: self.get_field_value(obj, field) for field in self.fields_to_serialize(obj)}

    def internal_value_obj(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {field: self.get_internal_value(data, field) for field in self.fields_to_serialize(data)}

    def get_field_value(self, obj: Any, field: str) -> Any:
        value = getattr(obj, field, None) if not isinstance(obj, dict) else obj.get(field)
        field_instance = self.get_field_instance(field)

        if isinstance(field_instance, Serializer):
            if isinstance(value, list):
                return field_instance.to_representation(value)
            return field_instance.represent_obj(value)
        elif isinstance(field_instance, fields.MethodField):
            return field_instance.to_representation(value, field)
        else:
            return field_instance.to_representation(value)

    def get_internal_value(self, data: Dict[str, Any], field: str) -> Any:
        value = data.get(field)
        field_instance = self.get_field_instance(field)

        if isinstance(field_instance, Serializer):
            if isinstance(value, list):
                return field_instance.to_internal_value(value)
            return field_instance.internal_value_obj(value)
        elif isinstance(field_instance, fields.MethodField):
            return field_instance.to_internal_value(value, field)
        else:
            return field_instance.to_internal_value(value)

    def get_field_instance(self, field: str):
        if hasattr(self, field) and isinstance(getattr(self, field), Serializer):
            return getattr(self, field)
        return self.model_class._meta.get_field(field)

    def save(self):
        if self.many:
            return self.model_class.objects.bulk_create(self.internal_data)
        if self.instance:
            self.model_class.objects.filter(id=self.instance.id).update(**self.internal_data)
            self.instance = self.model_class.objects.get(id=self.instance.id)
        else:
            self.instance = self.model_class.objects.create(**self.internal_data)
        return self.instance
