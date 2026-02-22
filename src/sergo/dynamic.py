"""Dynamic model creation from field definitions.

Creates lightweight model-like objects with Sergo field validation/coercion,
backed by dicts (JSONB) rather than database tables.

Usage:
    from sergo.dynamic import DynamicSchema

    schema = DynamicSchema({
        "name": "StringField",
        "birthday": "DateField",
        "last_contact": "DateTimeField",
        "age": "IntegerField",
    })

    # Validate + coerce data
    clean = schema.validate({"name": "Bec", "birthday": "1990-03-15"})

    # Query a list of entries
    results = schema.query(entries).filter(last_contact__lt="2026-02-08").list()
"""

from sergo import fields as field_module
from sergo.query.memory import MemoryQuery

FIELD_MAP = {
    'StringField': field_module.StringField,
    'IntegerField': field_module.IntegerField,
    'FloatField': field_module.FloatField,
    'BoolField': field_module.BoolField,
    'DateTimeField': field_module.DateTimeField,
    'DateField': field_module.DateTimeField,  # reuse DateTime, coerces ISO strings
    'TimeField': field_module.TimeField,
    'ArrayField': field_module.ArrayField,
    # Short aliases for LLM convenience
    'str': field_module.StringField,
    'int': field_module.IntegerField,
    'float': field_module.FloatField,
    'bool': field_module.BoolField,
    'datetime': field_module.DateTimeField,
    'date': field_module.DateTimeField,
    'time': field_module.TimeField,
    'list': field_module.ArrayField,
}


class DynamicSchema:
    """A schema built from a {field_name: field_type} dict. Validates and queries JSONB data."""

    def __init__(self, fields_def: dict):
        self._fields_def = fields_def
        self._fields = {}
        for name, type_name in fields_def.items():
            field_cls = FIELD_MAP.get(type_name)
            if not field_cls:
                raise ValueError(f"Unknown field type '{type_name}'. Valid: {', '.join(FIELD_MAP.keys())}")
            self._fields[name] = field_cls(optional=True)

    @property
    def field_names(self):
        return list(self._fields.keys())

    def validate(self, data: dict) -> dict:
        """Validate and coerce data against the schema. Returns cleaned data.
        Unknown fields are silently dropped. Missing optional fields become None."""
        clean = {}
        for name, field in self._fields.items():
            value = data.get(name)
            if value is not None:
                clean[name] = field.to_internal_value(value)
            else:
                clean[name] = None
        return clean

    def serialize(self, data: dict) -> dict:
        """Serialize data for storage (e.g. datetime → ISO string)."""
        out = {}
        for name, field in self._fields.items():
            value = data.get(name)
            if value is not None:
                out[name] = field.to_representation(value)
            else:
                out[name] = None
        return out

    def query(self, items: list) -> MemoryQuery:
        """Create a type-aware in-memory queryset over a list of entry objects."""
        return MemoryQuery(items, self._fields)
