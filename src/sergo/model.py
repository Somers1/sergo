from pathlib import Path

import sqlparse

from sergo import fields
from sergo.connection import connection
from sergo.query import Query


class ModelBase(type):
    def __new__(cls, name, bases, attrs):
        super_new = super().__new__
        parents = [b for b in bases if isinstance(b, ModelBase)]
        if not parents:
            return super_new(cls, name, bases, attrs)
        new_class = super_new(cls, name, bases, attrs)
        attr_meta = attrs.pop('Meta', None)
        if not attr_meta:
            meta = getattr(new_class, 'Meta', None)
        else:
            meta = attr_meta
        new_class._meta = Options(new_class)
        if meta:
            for key, value in meta.__dict__.items():
                if not key.startswith('__'):
                    setattr(new_class._meta, key, value)
        for obj_name, obj in attrs.items():
            if isinstance(obj, fields.Field):
                new_class._meta.add_field(obj_name, obj)
        if not hasattr(new_class, 'objects'):
            new_class.objects = Manager()
        new_class.objects.model = new_class
        return new_class


class Options:
    def __init__(self, model):
        self.model = model
        self.fields = {}
        self.db_table = None

    def add_field(self, name, field):
        self.fields[name] = field
        field.name = name

    def get_field(self, name):
        return self.fields[name]


class Manager:
    def __init__(self):
        self.model = None
        self._statements = None

    @property
    def statements(self):
        if self._statements is None:
            self._statements = {}
            for sql_file in Path('sql').glob('*.sql'):
                with open(sql_file, 'r') as file:
                    for statement in sqlparse.split(file.read()):
                        statement_name = statement.split('\n')[0].replace('--', '').strip()
                        if statement_name in self._statements:
                            raise ValueError(f"Duplicate statement name {statement_name}")
                        self._statements[statement_name] = statement
        return self._statements

    @property
    def model_name(self):
        return self.model.__name__

    @property
    def table_name(self):
        if self.model._meta.db_table:
            return self.model._meta.db_table
        return self.model.__name__.lower()

    @property
    def query(self):
        try:
            return self.statements[self.model_name].replace(';', '').replace(f'-- {self.model_name}', '')
        except KeyError:
            return connection.DEFAULT_QUERY.format(table_name=self.table_name)

    def create(self, **kwargs):
        for field in kwargs.keys():
            if field not in self.model._meta.fields:
                # Prevents SQL injection
                raise AttributeError(f"{field} is not a valid field for {self.model.__name__}")
        inserted_id = connection.insert(kwargs, self.table_name)
        return self.get(id=inserted_id)

    def get(self, **kwargs):
        query = Query(self.query, self.model)
        query.filter(**kwargs)
        result = query.execute()
        if len(result) > 1:
            raise ValueError("Multiple objects found")
        try:
            return result[0]
        except IndexError:
            raise ValueError("Object not found")

    def filter(self, **kwargs):
        return Query(self.query, self.model).filter(**kwargs)

    def from_query(self, query, params=None):
        return Query(query, self.model, params=params).execute()


class Model(metaclass=ModelBase):
    def __init__(self, **kwargs):
        for field, value in kwargs.items():
            if field not in self._meta.fields:
                raise AttributeError(f"{field} is not a valid field for {self.__class__.__name__}")
            setattr(self, field, value)

