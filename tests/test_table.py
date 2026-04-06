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

@pytest.fixture
def populated(mem_db):
    """Table pre-loaded with three rows; yields (db, table)."""
    t = mem_db["people", "table"]
    t["alice"] = ("hello",)
    t["bob"]   = ("world",)
    t["carol"] = ("xdbx",)
    return mem_db, t

@pytest.fixture
def int_table(mem_db):
    """
    Table with an INTEGER score column, pre-loaded for get_col_filt tests.
    Schema: key TEXT PK, col1 TEXT, score INTEGER
    """
    mem_db.conn.execute(
        'CREATE TABLE "scores" '
        '("key" TEXT PRIMARY KEY, "col1" TEXT, "score" INTEGER)'
    )
    mem_db.conn.commit()
    t = mem_db["scores", "table"]
    t["low"]  = {"col1": "a", "score": 10}
    t["mid"]  = {"col1": "b", "score": 50}
    t["high"] = {"col1": "c", "score": 90}
    return t

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
    
    def test_string_value_raises_type_error(self, table):
        with pytest.raises(TypeError, match="use tuple or dict"):
            table["k"] = "plain string"
 
    def test_integer_value_raises_type_error(self, table):
        with pytest.raises(TypeError, match="use tuple or dict"):
            table["k"] = 42
 
    def test_list_value_raises_type_error(self, table):
        with pytest.raises(TypeError, match="use tuple or dict"):
            table["k"] = ["a", "b"]
    
    def test_contains_true_for_existing_key(self, table):
        table["x"] = ("v",)
        assert "x" in table
 
    def test_contains_false_for_missing_key(self, table):
        assert "ghost" not in table
 
    def test_contains_false_on_empty_table(self, table):
        assert "anything" not in table
 
    def test_iter_yields_keys_in_insertion_order(self, table):
        table["z"] = ("c",)
        table["a"] = ("a",)
        table["m"] = ("b",)
        assert [k for k in table] == ["z", "a", "m"]
 
    def test_iter_empty_table_yields_nothing(self, table):
        assert list(table) == []
 
    def test_len_empty_is_zero(self, table):
        assert len(table) == 0
 
    def test_len_increments_on_insert(self, table):
        table["r1"] = ("a",)
        assert len(table) == 1
        table["r2"] = ("b",)
        assert len(table) == 2
 
    def test_len_unchanged_on_overwrite(self, table):
        table["k"] = ("v1",)
        table["k"] = ("v2",)
        assert len(table) == 1
 
    def test_len_consistent_with_iter(self, populated):
        _, t = populated
        assert len(t) == sum(1 for _ in t)
    
    def test_slice_returns_list(self, populated):
        _, t = populated
        assert isinstance(t[1:4], list)
 
    def test_slice_full_range_returns_all_rows(self, populated):
        _, t = populated
        assert len(t[1:4]) == 3
 
    def test_slice_partial_range(self, populated):
        _, t = populated
        result = t[1:2]
        assert len(result) == 1
        assert result[0][0] == "alice"
 
    def test_slice_out_of_range_returns_empty(self, populated):
        _, t = populated
        assert t[100:200] == []
 
    def test_slice_with_step(self, populated):
        _, t = populated
        result = t[1:4:2]
        assert len(result) == 2
    
    def test_column_name_returns_list(self, populated):
        _, t = populated
        assert isinstance(t["col1"], list)
 
    def test_column_name_returns_all_values(self, populated):
        _, t = populated
        assert set(t["col1"]) == {"hello", "world", "xdbx"}
 
    def test_key_column_returns_all_primary_keys(self, populated):
        _, t = populated
        assert set(t["key"]) == {"alice", "bob", "carol"}
 
    def test_column_name_preserves_insertion_order(self, table):
        table["z"] = ("last",)
        table["a"] = ("first",)
        assert table["col1"] == ["last", "first"]
 
    def test_column_name_takes_priority_over_row_key(self, table):
        """A string matching a column name always returns column data."""
        table["key"] = ("oops",)   # row whose PK is "key"
        result = table["key"]
        assert isinstance(result, list)
    
    def test_existing_row_key_returns_list(self, populated):
        _, t = populated
        assert isinstance(t["alice"], tuple)
 
    def test_existing_row_key_first_element_is_key(self, populated):
        _, t = populated
        assert t["alice"][0] == "alice"
 
    def test_existing_row_key_second_element_is_data(self, populated):
        _, t = populated
        assert t["alice"][1] == "hello"
 
    def test_existing_row_key_length_equals_column_count(self, populated):
        _, t = populated
        assert len(t["alice"]) == len(t.columns)
 
    def test_existing_row_key_reflects_update(self, table):
        table["alice"] = ("hello",)
        table["alice"] = ("updated",)
        assert table["alice"][1] == "updated"
    
    def test_missing_string_key_raises_key_error(self, populated):
        _, t = populated
        with pytest.raises(KeyError):
            _ = t["nobody"]
    
    def test_col_sel_returns_tuple(self, populated):
        _, t = populated
        assert isinstance(t["alice", "col1"], tuple)
 
    def test_col_sel_single_column_value(self, populated):
        _, t = populated
        assert t["alice", "col1"] == ("hello",)
    
    def test_col_filt_open_end(self, int_table):
        """score >= 50 (stop=None)."""
        rows = int_table["score", 50:]
        assert len(rows) == 2
        assert all(r[2] >= 50 for r in rows)
 
    def test_col_filt_open_start(self, int_table):
        """score < 50 (start=None)."""
        rows = int_table["score", :50]
        assert len(rows) == 1
        assert all(r[2] < 50 for r in rows)
 
    def test_col_filt_closed_range(self, int_table):
        """10 <= score <= 89 (BETWEEN 10 AND 89)."""
        rows = int_table["score", 10:90]
        assert len(rows) == 2
        assert all(10 <= r[2] < 90 for r in rows)
 
    def test_col_filt_empty_range_returns_empty_list(self, int_table):
        assert int_table["score", 200:300] == []
 
    def test_col_filt_returns_list(self, int_table):
        assert isinstance(int_table["score", 0:], list)
 
    def test_col_filt_with_step(self, int_table):
        rows = int_table["score", 0::2]
        assert isinstance(rows, list)
    
    # ── add_column ────────────────────────────────────────────────────────────
 
    def test_add_column_appears_in_columns(self, table):
        table.add_column("notes", "TEXT")
        assert "notes" in table.columns
 
    def test_add_column_preserves_existing_columns(self, table):
        table.add_column("notes", "TEXT")
        assert "key" in table.columns and "col1" in table.columns
 
    def test_add_column_integer_dtype(self, table):
        table.add_column("score", "INTEGER")
        assert "score" in table.columns
 
    def test_add_column_readonly_raises(self, table):
        table.flag = "r"
        with pytest.raises(RuntimeError):
            table.add_column("x", "TEXT")
 
    def test_add_column_usable_in_dict_insert(self, table):
        table.add_column("notes", "TEXT")
        table["alice"] = {"col1": "hello", "notes": "extra"}
        assert "alice" in table
 
    # ── drop_column ───────────────────────────────────────────────────────────
 
    def test_drop_column_removes_from_columns(self, table):
        table.add_column("tmp", "TEXT")
        table.drop_column("tmp")
        assert "tmp" not in table.columns
 
    def test_drop_column_preserves_other_columns(self, table):
        table.add_column("tmp", "TEXT")
        table.drop_column("tmp")
        assert "key" in table.columns and "col1" in table.columns
 
    def test_drop_column_readonly_raises(self, table):
        table.add_column("tmp", "TEXT")
        table.flag = "r"
        with pytest.raises(RuntimeError):
            table.drop_column("tmp")
 
    # ── rename_column ─────────────────────────────────────────────────────────
 
    def test_rename_column_new_name_appears(self, table):
        table.rename_column("col1", "value")
        assert "value" in table.columns
 
    def test_rename_column_old_name_gone(self, table):
        table.rename_column("col1", "value")
        assert "col1" not in table.columns
 
    def test_rename_column_data_preserved(self, table):
        table["alice"] = ("hello",)
        table.rename_column("col1", "value")
        assert "alice" in table
 
    def test_rename_column_readonly_raises(self, table):
        table.flag = "r"
        with pytest.raises(RuntimeError):
            table.rename_column("col1", "value")
 
    # ── add_foreign_key ───────────────────────────────────────────────────────

    def test_add_foreign_key_adds_column(self, mem_db):
        mem_db["referenced", "table"]
        child = mem_db["child", "table"]
        child.add_foreign_key("parent_id", "referenced")
        assert "parent_id" in child.columns
 
    def test_add_foreign_key_column_is_text(self, mem_db):
        mem_db["referenced", "table"]
        child = mem_db["child", "table"]
        child.add_foreign_key("parent_id", "referenced")
        info = list(mem_db.conn.select('PRAGMA TABLE_INFO("child")'))
        fk_col = next(row for row in info if row[1] == "parent_id")
        assert fk_col[2].upper() == "TEXT"
 
    def test_add_foreign_key_readonly_raises(self, mem_db):
        mem_db["referenced", "table"]
        child = mem_db["child", "table"]
        child.flag = "r"
        with pytest.raises(RuntimeError):
            child.add_foreign_key("parent_id", "referenced")
 
    def test_add_multiple_foreign_keys(self, mem_db):
        mem_db["ref_a", "table"]
        mem_db["ref_b", "table"]
        child = mem_db["child", "table"]
        child.add_foreign_key("a_id", "ref_a")
        child.add_foreign_key("b_id", "ref_b")
        assert "a_id" in child.columns and "b_id" in child.columns
    
    def test_columns_on_fresh_table(self, table):
        assert table.columns == ["key", "col1"]
 
    def test_columns_reflects_add_column(self, table):
        table.add_column("extra", "TEXT")
        assert "extra" in table.columns
 
    def test_columns_reflects_drop_column(self, table):
        table.add_column("extra", "TEXT")
        table.drop_column("extra")
        assert "extra" not in table.columns
 
    def test_columns_reflects_rename_column(self, table):
        table.rename_column("col1", "value")
        assert "value" in table.columns and "col1" not in table.columns
    
    # ── to_sql ────────────────────────────────────────────────────────────────
 
    def test_to_sql_is_string(self, populated):
        _, t = populated
        assert isinstance(t.to_sql(), str)
 
    def test_to_sql_contains_create_if_not_exists(self, populated):
        _, t = populated
        assert "CREATE TABLE IF NOT EXISTS" in t.to_sql()
 
    def test_to_sql_contains_table_name(self, populated):
        _, t = populated
        assert "people" in t.to_sql()
 
    def test_to_sql_contains_insert(self, populated):
        _, t = populated
        assert "INSERT" in t.to_sql()
    
    # ── to_dict("dict") ──────────────────────────────────────────────────────
 
    def test_to_dict_dict_no_longer_crashes(self, populated):
        _, t = populated
        result = t.to_dict("dict")
        assert isinstance(result, dict)
 
    def test_to_dict_dict_has_all_row_keys(self, populated):
        _, t = populated
        assert set(t.to_dict("dict").keys()) == {"alice", "bob", "carol"}
 
    def test_to_dict_dict_values_are_dicts(self, populated):
        _, t = populated
        assert all(isinstance(v, dict) for v in t.to_dict("dict").values())
 
    def test_to_dict_dict_values_correct(self, table):
        table["alice"] = ("hello",)
        print(table["alice"])
        result = table.to_dict("dict")
        print(result)
        assert result["alice"]["col1"] == "hello"
 
    def test_to_dict_invalid_otype_raises_type_error(self, populated):
        _, t = populated
        with pytest.raises(TypeError):
            t.to_dict("invalid")
    
    # ── to_dict("list") ──────────────────────────────────────────────────────
 
    def test_to_dict_list_uses_table_name_as_key(self, populated):
        _, t = populated
        assert "people" in t.to_dict("list")
 
    def test_to_dict_list_value_is_list_of_dicts(self, populated):
        _, t = populated
        rows = t.to_dict("list")["people"]
        assert isinstance(rows, list) and all(isinstance(r, dict) for r in rows)
 
    def test_to_dict_list_each_row_has_all_columns(self, populated):
        _, t = populated
        rows = t.to_dict("list")["people"]
        assert all("key" in r and "col1" in r for r in rows)
 
    def test_to_dict_list_row_count(self, populated):
        _, t = populated
        assert len(t.to_dict("list")["people"]) == 3
 
    def test_to_dict_list_row_values_correct(self, table):
        table["alice"] = ("hello",)
        row = table.to_dict("list")["users"][0]
        assert row["key"] == "alice" and row["col1"] == "hello"
    
    # ── to_dict("column") ────────────────────────────────────────────────────
 
    def test_to_dict_column_returns_dict(self, populated):
        _, t = populated
        assert isinstance(t.to_dict("column"), dict)
 
    def test_to_dict_column_has_all_column_keys(self, populated):
        _, t = populated
        d = t.to_dict("column")
        assert "key" in d and "col1" in d
 
    def test_to_dict_column_values_are_lists(self, populated):
        _, t = populated
        d = t.to_dict("column")
        assert isinstance(d["key"], list) and isinstance(d["col1"], list)
 
    def test_to_dict_column_all_keys_present(self, populated):
        _, t = populated
        assert set(t.to_dict("column")["key"]) == {"alice", "bob", "carol"}
 
    def test_to_dict_column_all_values_present(self, populated):
        _, t = populated
        assert set(t.to_dict("column")["col1"]) == {"hello", "world", "xdbx"}
 
    def test_to_dict_column_lengths_match(self, populated):
        _, t = populated
        d = t.to_dict("column")
        assert len(d["key"]) == len(d["col1"]) == 3
    
    def test_columns_on_fresh_table(self, table):
        assert table.columns == ["key", "col1"]
 
    def test_columns_reflects_add_column(self, table):
        table.add_column("extra", "TEXT")
        assert "extra" in table.columns
 
    def test_columns_reflects_drop_column(self, table):
        table.add_column("extra", "TEXT")
        table.drop_column("extra")
        assert "extra" not in table.columns
 
    def test_columns_reflects_rename_column(self, table):
        table.rename_column("col1", "value")
        assert "value" in table.columns and "col1" not in table.columns