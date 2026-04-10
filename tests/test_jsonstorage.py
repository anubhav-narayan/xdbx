from typing import Generator
import pytest
from db86 import Database
from db86.storages import JSONStorage
import json

@pytest.fixture
def mem_db() -> Generator[Database, None, None]:
    """In-memory xdbx Database, closed after each test."""
    db = Database(":memory:", autocommit=True, journal_mode="WAL")
    yield db
    db.close(do_log=False, force=True)

@pytest.fixture
def json_storage(mem_db: Database) -> JSONStorage:
    """Empty JSONStorage inside an in-memory Database."""
    return mem_db["items", "json"]

@pytest.fixture
def populate_storage(mem_db: Database) -> JSONStorage:
    """Helper to populate a JSONStorage with sample records."""
    json_storage = mem_db["items", "json"]
    # Insert some sample records
    json_storage["u1"] = {"name": "Alice", "age": 30, "dept": "eng", "salary": 100}
    json_storage["u2"] = {"name": "Bob",   "age": 25, "dept": "eng", "salary": 150}
    json_storage["u3"] = {"name": "Cara",  "age": 40, "dept": "hr",  "salary": 200}
    json_storage["u4"] = {"name": "Dan",   "age": 20, "dept": "hr",  "salary": 120}
    return json_storage

@pytest.mark.unit
class TestJSONStorage:
    """JSONStorage CRUD, path queries, and serialisation helpers."""
 
    def test_repr(self, json_storage):
        assert "JSON Storage" in repr(json_storage)
        assert "items" in repr(json_storage)
 
    def test_setitem_and_getitem_roundtrip(self, json_storage):
        json_storage["k1"] = {"x": 1, "y": [2, 3]}
        assert json_storage["k1"] == {"x": 1, "y": [2, 3]}
 
    def test_setitem_update_existing(self, json_storage):
        json_storage["k"] = {"v": 1}
        json_storage["k"] = {"v": 99}
        assert json_storage["k"]["v"] == 99
 
    def test_setitem_non_dict_raises_type_error(self, json_storage):
        with pytest.raises(TypeError, match="use dict"):
            json_storage["k"] = "string"  # type: ignore[assignment]
 
    def test_getitem_missing_raises_key_error(self, json_storage):
        with pytest.raises(KeyError):
            _ = json_storage["missing"]
 
    def test_contains_true(self, json_storage):
        json_storage["present"] = {"a": 1}
        assert "present" in json_storage
 
    def test_contains_false(self, json_storage):
        assert "absent" not in json_storage
 
    def test_iter_yields_insertion_order_keys(self, json_storage):
        json_storage["z"] = {"n": 3}
        json_storage["a"] = {"n": 1}
        json_storage["m"] = {"n": 2}
        keys = [k for k in json_storage]     # safe: uses __iter__ not __len__
        assert keys == ["z", "a", "m"]
 
    def test_columns_property(self, json_storage):
        cols = json_storage.columns
        assert "key" in cols
        assert "object" in cols
 
    def test_describe_returns_grid_string(self, json_storage):
        desc = json_storage.describe()
        assert "key" in desc and "object" in desc
 
    def test_xschema_has_expected_keys(self, json_storage):
        schema = json_storage.xschema
        assert set(schema.keys()) == {"name", "cols", "sql"}
        assert schema["name"] == "items"
 
    def test_commit_is_callable(self, json_storage):
        json_storage["x"] = {"v": 1}
        json_storage.commit(blocking=True)   # must not raise
 
    def test_readonly_raises_on_write(self, mem_db):
        st = mem_db["ro", "json"]
        st.flag = "r"
        with pytest.raises(RuntimeError, match="read-only"):
            st["k"] = {"v": 1}
 
    # ── get_path ──────────────────────────────────────────────────────────────
 
    def test_get_path_direct_key(self, json_storage):
        json_storage["rec"] = {"score": 42}
        results = list(json_storage.get_path("rec/score"))
        assert results == [{"rec/score": 42}]
 
    def test_get_path_wildcard_all_records(self, json_storage):
        json_storage["alice"] = {"age": 30}
        json_storage["bob"]   = {"age": 25}
        results = list(json_storage.get_path("*/age"))
        age_map = {list(d.keys())[0].split("/")[0]: list(d.values())[0]
                   for d in results}
        assert age_map == {"alice": 30, "bob": 25}
 
    def test_get_path_missing_field_yields_none(self, json_storage):
        json_storage["x"] = {"a": 1}
        json_storage["y"] = {"b": 2}   # no "a" field
        results = list(json_storage.get_path("*/a"))
        values = {list(d.keys())[0].split("/")[0]: list(d.values())[0]
                  for d in results}
        assert values["x"] == 1
        assert values["y"] is None
 
    def test_get_path_returns_generator(self, json_storage):
        import types
        json_storage["r"] = {"k": "v"}
        result = json_storage.get_path("*/k")
        assert isinstance(result, types.GeneratorType)
 
    # ── serialisation helpers ─────────────────────────────────────────────────
 
    def test_to_dict(self, json_storage):
        json_storage["p"] = {"val": 1}
        json_storage["q"] = {"val": 2}
        d = json_storage.to_dict()
        assert d == {"p": {"val": 1}, "q": {"val": 2}}
 
    def test_to_json_is_valid_json(self, json_storage):
        json_storage["r"] = {"x": 99}
        payload = json_storage.to_json()
        parsed = json.loads(payload)
        assert parsed == {"r": {"x": 99}}
 
    def test_to_sql_contains_create_and_insert(self, json_storage):
        json_storage["s"] = {"data": "hello"}
        sql = json_storage.to_sql()
        assert "CREATE" in sql
        assert "INSERT" in sql
 
    def test_merge_adds_new_keys(self, json_storage):
        json_storage["base"] = {"a": 1, "b": 2}
        json_storage.merge({"extra": {"c": 3}})
        assert "extra" in json_storage
        assert json_storage["extra"] == {"c": 3}
 
    def test_merge_updates_existing_nested(self, json_storage):
        json_storage["rec"] = {"nested": {"x": 1, "y": 2}}
        json_storage.merge({"rec": {"nested": {"y": 99, "z": 3}}})
        result = json_storage["rec"]
        assert result["nested"]["x"] == 1    # preserved
        assert result["nested"]["y"] == 99   # updated
        assert result["nested"]["z"] == 3    # added
    
    def test_len_empty_storage_is_zero(self, json_storage):
        assert len(json_storage) == 0
 
    def test_len_increments_on_insert(self, json_storage):
        json_storage["a"] = {"v": 1}
        assert len(json_storage) == 1
        json_storage["b"] = {"v": 2}
        assert len(json_storage) == 2
 
    def test_len_unchanged_on_overwrite(self, json_storage):
        json_storage["k"] = {"v": 1}
        json_storage["k"] = {"v": 99}   # update, not insert
        assert len(json_storage) == 1
 
    def test_len_decrements_after_delete(self, json_storage):
        json_storage["x"] = {"v": 1}
        json_storage["y"] = {"v": 2}
        assert len(json_storage) == 2
        del json_storage["x"]
        assert len(json_storage) == 1
 
    def test_len_consistent_with_iter_count(self, json_storage):
        for i in range(5):
            json_storage[f"k{i}"] = {"n": i}
        assert len(json_storage) == sum(1 for _ in json_storage)
    
    def test_delitem_removes_key(self, json_storage):
        json_storage["x"] = {"v": 1}
        del json_storage["x"]
        assert "x" not in json_storage
 
    def test_delitem_missing_key_raises_key_error(self, json_storage):
        with pytest.raises(KeyError):
            del json_storage["nobody"]
 
    def test_delitem_reduces_len(self, json_storage):
        json_storage["a"] = {"v": 1}
        json_storage["b"] = {"v": 2}
        del json_storage["a"]
        assert len(json_storage) == 1
 
    def test_delitem_key_no_longer_in_iter(self, json_storage):
        json_storage["gone"] = {"v": 1}
        json_storage["stay"] = {"v": 2}
        del json_storage["gone"]
        assert "gone" not in [k for k in json_storage]
        assert "stay" in [k for k in json_storage]
 
    def test_delitem_get_raises_after_delete(self, json_storage):
        json_storage["tmp"] = {"v": 1}
        del json_storage["tmp"]
        with pytest.raises(KeyError):
            _ = json_storage["tmp"]
 
    def test_delitem_readonly_raises_runtime_error(self, json_storage):
        json_storage["k"] = {"v": 1}
        json_storage.flag = "r"
        with pytest.raises(RuntimeError, match="read-only"):
            del json_storage["k"]
    # ── filter ──────────────────────────────────────────────────────────────
    def test_filter_and_select(self, populate_storage):
        result = populate_storage.query({
            "filter": {"path": "age", "op": "gte", "value": 30},
            "select": ["name", "dept"],
            "sort":   [{"field": "name", "order": "asc"}],
        })
        assert [r["name"] for r in result.values()] == ["Alice", "Cara"]


    def test_compound_filter(self, populate_storage):
        result = populate_storage.query({
            "filter": {
                "and": [
                    {"path": "dept", "op": "eq", "value": "eng"},
                    {"path": "salary", "op": "gte", "value": 120},
                ]
            }
        })
        assert len(result) == 1
        assert 'u2' in result
        assert result['u2']["name"] == "Bob"


    def test_scalar_aggregate(self, populate_storage):
        result = populate_storage.query({
            "filter": {"path": "dept", "op": "eq", "value": "eng"},
            "aggregate": {"op": "avg", "field": "salary"},
        })
        assert result == pytest.approx(125.0)


    def test_group_by_aggregate(self, populate_storage):
        result = populate_storage.query({
            "aggregate": {
                "op": "group_by", "by": "dept",
                "field": "salary", "sub_op": "avg",
            },
            "sort": [{"field": "dept", "order": "asc"}],
        })
        # Expect averages per department
        assert result["eng"] == pytest.approx(125.0)
        assert result["hr"] == pytest.approx(160.0)


    def test_pagination(self, populate_storage):
        result = populate_storage.query({
            "sort": [{"field": "name", "order": "asc"}],
            "limit": 2,
            "offset": 1,
        })
        # Sorted names: Alice, Bob, Cara, Dan → offset 1, limit 2 → Bob, Cara
        assert [r["name"] for r in result.values()] == ["Bob", "Cara"]