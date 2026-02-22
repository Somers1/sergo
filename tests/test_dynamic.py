import pytest
from datetime import datetime, timezone
from sergo.dynamic import DynamicSchema
from sergo.query.memory import MemoryQuery


class FakeEntry:
    """Mimics an Entry model with a .data dict."""
    def __init__(self, data):
        self.data = data


class TestDynamicSchema:
    def setup_method(self):
        self.schema = DynamicSchema({
            "name": "str",
            "age": "int",
            "birthday": "date",
            "last_contact": "datetime",
            "active": "bool",
        })

    def test_validate_coerces_types(self):
        clean = self.schema.validate({"name": "Bec", "age": "30", "active": "1"})
        assert clean["name"] == "Bec"
        assert clean["age"] == 30
        assert clean["active"] is True

    def test_validate_missing_fields_are_none(self):
        clean = self.schema.validate({"name": "Bec"})
        assert clean["age"] is None
        assert clean["birthday"] is None

    def test_validate_drops_unknown_fields(self):
        clean = self.schema.validate({"name": "Bec", "unknown": "foo"})
        assert "unknown" not in clean

    def test_validate_datetime_from_string(self):
        clean = self.schema.validate({"last_contact": "2026-02-20T10:00:00+00:00"})
        assert isinstance(clean["last_contact"], datetime)

    def test_serialize_roundtrip(self):
        data = {"name": "Bec", "age": 30, "last_contact": "2026-02-20T10:00:00+00:00"}
        clean = self.schema.validate(data)
        serialized = self.schema.serialize(clean)
        assert serialized["name"] == "Bec"
        assert serialized["age"] == 30
        assert isinstance(serialized["last_contact"], str)

    def test_unknown_field_type_raises(self):
        with pytest.raises(ValueError, match="Unknown field type"):
            DynamicSchema({"x": "UnknownField"})

    def test_field_names(self):
        assert set(self.schema.field_names) == {"name", "age", "birthday", "last_contact", "active"}


class TestMemoryQuery:
    def setup_method(self):
        self.fields = DynamicSchema({"name": "str", "age": "int", "score": "float", "joined": "datetime"})._fields
        self.items = [
            FakeEntry({"name": "Alice", "age": 30, "score": 9.5, "joined": "2026-01-01T00:00:00+00:00"}),
            FakeEntry({"name": "Bob", "age": 25, "score": 7.0, "joined": "2026-02-15T00:00:00+00:00"}),
            FakeEntry({"name": "Charlie", "age": 35, "score": 8.2, "joined": "2025-06-01T00:00:00+00:00"}),
        ]

    def test_filter_exact(self):
        result = MemoryQuery(self.items, self.fields).filter(name="Alice").list()
        assert len(result) == 1 and result[0].data["name"] == "Alice"

    def test_filter_gt(self):
        result = MemoryQuery(self.items, self.fields).filter(age__gt=28).list()
        assert len(result) == 2

    def test_filter_lt(self):
        result = MemoryQuery(self.items, self.fields).filter(age__lt=30).list()
        assert len(result) == 1 and result[0].data["name"] == "Bob"

    def test_filter_gte(self):
        result = MemoryQuery(self.items, self.fields).filter(age__gte=30).list()
        assert len(result) == 2

    def test_filter_lte(self):
        result = MemoryQuery(self.items, self.fields).filter(age__lte=25).list()
        assert len(result) == 1

    def test_filter_contains(self):
        result = MemoryQuery(self.items, self.fields).filter(name__contains="li").list()
        assert len(result) == 2  # Alice, Charlie

    def test_filter_icontains(self):
        result = MemoryQuery(self.items, self.fields).filter(name__icontains="ALICE").list()
        assert len(result) == 1

    def test_filter_in(self):
        result = MemoryQuery(self.items, self.fields).filter(name__in=["Alice", "Bob"]).list()
        assert len(result) == 2

    def test_filter_isnull(self):
        items = self.items + [FakeEntry({"name": None, "age": 20})]
        result = MemoryQuery(items, self.fields).filter(name__isnull=True).list()
        assert len(result) == 1

    def test_filter_datetime_lt(self):
        result = MemoryQuery(self.items, self.fields).filter(joined__lt="2026-02-01T00:00:00+00:00").list()
        assert len(result) == 2  # Alice (Jan) + Charlie (Jun 2025)

    def test_filter_datetime_gt(self):
        result = MemoryQuery(self.items, self.fields).filter(joined__gt="2026-02-01T00:00:00+00:00").list()
        assert len(result) == 1 and result[0].data["name"] == "Bob"

    def test_chained_filters(self):
        result = MemoryQuery(self.items, self.fields).filter(age__gt=30).filter(score__lt=9.0).list()
        assert len(result) == 1 and result[0].data["name"] == "Charlie"  # age 35, score 8.2

    def test_exclude(self):
        result = MemoryQuery(self.items, self.fields).exclude(name="Alice").list()
        assert len(result) == 2

    def test_order(self):
        result = MemoryQuery(self.items, self.fields).order("age").list()
        assert [e.data["age"] for e in result] == [25, 30, 35]

    def test_order_descending(self):
        result = MemoryQuery(self.items, self.fields).order("-age").list()
        assert [e.data["age"] for e in result] == [35, 30, 25]

    def test_first(self):
        result = MemoryQuery(self.items, self.fields).filter(age__gt=28).order("age").first()
        assert result.data["name"] == "Alice"

    def test_count(self):
        assert MemoryQuery(self.items, self.fields).filter(age__gt=20).count() == 3

    def test_slicing(self):
        result = MemoryQuery(self.items, self.fields).order("age")[:2].list()
        assert len(result) == 2

    def test_empty_result(self):
        result = MemoryQuery(self.items, self.fields).filter(name="Nobody").list()
        assert result == []

    def test_startswith(self):
        result = MemoryQuery(self.items, self.fields).filter(name__startswith="Al").list()
        assert len(result) == 1 and result[0].data["name"] == "Alice"
