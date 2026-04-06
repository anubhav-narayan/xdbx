import pytest
from typing import Generator
from xdbx import Database
from xdbx.storages import Table


@pytest.fixture
def mem_db() -> Generator[Database, None, None]:
    """In-memory xdbx Database, closed after each test."""
    db = Database(":memory:", autocommit=True, journal_mode="WAL")
    yield db
    db.close(do_log=False, force=True)

@pytest.fixture
def table(mem_db: Database) -> Table:
    """Empty Table inside an in-memory Database."""
    return mem_db["users", "table"]

@pytest.mark.unit
class TestTable:
    """Table CRUD, schema inspection, and dict-like behaviour."""

    def test_repr(self, table):
        assert "Table" in repr(table)
        assert "users" in repr(table)

    def test_setitem_and_getitem_roundtrip(self, table):
        table["u1"] = ("Alice",)
        item = table["u1"]
        print(item)

    def test_setitem_dict_update(self, table):
        table["u2"] = {"col1": "Bob"}
        table["u2"] = {"col1": "Robert"}
        assert table["u2"][1] == "Robert"

    def test_contains_and_len(self, table):
        table["u3"] = ("Charlie",)
        assert "u3" in table
        assert len(table) == 1

    def test_delete_item(self, table):
        table["u4"] = ("Dave",)
        del table["u4"]
        assert "u4" not in table

    def test_columns_property(self, table):
        cols = table.columns
        assert "key" in cols
        assert "col1" in cols

    def test_describe_returns_grid_string(self, table):
        desc = table.describe()
        assert "key" in desc and "col1" in desc

    def test_xschema_has_expected_keys(self, table):
        schema = table.xschema
        assert set(schema.keys()) == {"name", "cols", "sql"}
        assert schema["name"] == "users"

    def test_to_dict_column_format(self, table):
        table["u5"] = ("Eve",)
        d = table.to_dict(otype="column")
        assert "col1" in d and d["col1"] == ["Eve"]