"""In-memory queryset for filtering lists of dicts using Sergo's field__operator syntax.

Used by dynamic schema models to query JSONB data with type-aware filtering.
Supports the same operators as PostgresQuery: exact, contains, icontains, gt, gte, lt, lte, in, isnull.
"""

from __future__ import annotations
from datetime import datetime, date


class MemoryQuery:
    """Queryset-like interface for filtering a list of objects with .data dicts."""

    def __init__(self, items: list, fields: dict = None):
        self._items = list(items)
        self._fields = fields or {}  # {field_name: Field instance} for type coercion
        self._ordering = None
        self._limit_value = None

    def _clone(self):
        clone = MemoryQuery(self._items, self._fields)
        clone._ordering = self._ordering
        clone._limit_value = self._limit_value
        return clone

    def filter(self, **kwargs) -> MemoryQuery:
        clone = self._clone()
        for key, value in kwargs.items():
            parts = key.split('__')
            field, operator = (parts[0], parts[1]) if len(parts) > 1 else (parts[0], 'exact')
            clone._items = [item for item in clone._items if self._match(item, field, operator, value)]
        return clone

    def exclude(self, **kwargs) -> MemoryQuery:
        clone = self._clone()
        for key, value in kwargs.items():
            parts = key.split('__')
            field, operator = (parts[0], parts[1]) if len(parts) > 1 else (parts[0], 'exact')
            clone._items = [item for item in clone._items if not self._match(item, field, operator, value)]
        return clone

    def order(self, field: str) -> MemoryQuery:
        clone = self._clone()
        reverse = field.startswith('-')
        field_name = field.lstrip('-')
        clone._items.sort(key=lambda item: self._get_value(item, field_name) or '', reverse=reverse)
        return clone

    def first(self):
        return self._items[0] if self._items else None

    def count(self):
        return len(self._items)

    def list(self):
        items = self._items
        if self._limit_value is not None:
            items = items[:self._limit_value]
        return items

    def __iter__(self):
        return iter(self.list())

    def __len__(self):
        return self.count()

    def __getitem__(self, key):
        if isinstance(key, slice):
            clone = self._clone()
            clone._items = clone._items[key]
            return clone
        return self._items[key]

    def _get_value(self, item, field):
        """Get a field value from an item, coercing via the field type if available."""
        raw = item.data.get(field) if hasattr(item, 'data') else item.get(field)
        if raw is None:
            return None
        if field in self._fields:
            try:
                return self._fields[field].to_internal_value(raw)
            except (ValueError, TypeError):
                return raw
        return raw

    def _coerce_value(self, field, value):
        """Coerce a filter value to the field's type for comparison."""
        if value is None:
            return None
        if field in self._fields:
            try:
                return self._fields[field].to_internal_value(value)
            except (ValueError, TypeError):
                return value
        return value

    def _match(self, item, field, operator, value) -> bool:
        item_value = self._get_value(item, field)
        comp_value = self._coerce_value(field, value)

        if operator == 'exact':
            if comp_value is None: return item_value is None
            return item_value == comp_value
        elif operator == 'isnull':
            return (item_value is None) == bool(value)
        elif operator == 'in':
            return item_value in [self._coerce_value(field, v) for v in value]
        elif operator == 'contains':
            return comp_value is not None and item_value is not None and str(comp_value) in str(item_value)
        elif operator == 'icontains':
            return comp_value is not None and item_value is not None and str(comp_value).lower() in str(item_value).lower()
        elif operator in ('gt', 'gte', 'lt', 'lte'):
            if item_value is None or comp_value is None: return False
            try:
                if operator == 'gt': return item_value > comp_value
                if operator == 'gte': return item_value >= comp_value
                if operator == 'lt': return item_value < comp_value
                if operator == 'lte': return item_value <= comp_value
            except TypeError:
                return False
        elif operator == 'startswith':
            return item_value is not None and str(item_value).startswith(str(comp_value))
        elif operator == 'istartswith':
            return item_value is not None and str(item_value).lower().startswith(str(comp_value).lower())
        return item_value == comp_value
