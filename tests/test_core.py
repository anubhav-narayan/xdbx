import pytest
from xdbx.threads import SqliteMultiThread

@pytest.mark.unit
class TestSqliteMultiThread:
    """Low-level queue-backed SQLite thread wrapper."""
 
    def _conn(self) -> SqliteMultiThread:
        return SqliteMultiThread(":memory:", autocommit=True,
                                 journal_mode="WAL", timeout=5)
 
    def test_initialises_without_error(self):
        conn = self._conn()
        assert conn.is_alive()
        conn.close(force=True)
 
    def test_execute_and_select(self):
        conn = self._conn()
        conn.execute("CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT)")
        conn.execute("INSERT INTO t VALUES (?, ?)", ("hello", "world"))
        rows = list(conn.select("SELECT * FROM t"))
        assert rows == [("hello", "world")]
        conn.close(force=True)
 
    def test_select_one_returns_first_row(self):
        conn = self._conn()
        conn.execute("CREATE TABLE t (k TEXT PRIMARY KEY, v INTEGER)")
        conn.execute("INSERT INTO t VALUES (?, ?)", ("a", 1))
        conn.execute("INSERT INTO t VALUES (?, ?)", ("b", 2))
        row = conn.select_one("SELECT v FROM t ORDER BY k")
        assert row == (1,)
        conn.close(force=True)
 
    def test_select_one_missing_returns_none(self):
        conn = self._conn()
        conn.execute("CREATE TABLE t (k TEXT PRIMARY KEY)")
        result = conn.select_one("SELECT * FROM t WHERE k = ?", ("nope",))
        assert result is None
        conn.close(force=True)
 
    def test_executemany(self):
        conn = self._conn()
        conn.execute("CREATE TABLE t (k TEXT PRIMARY KEY, v INTEGER)")
        pairs = [("a", 1), ("b", 2), ("c", 3)]
        conn.executemany("INSERT INTO t VALUES (?, ?)", pairs)
        rows = list(conn.select("SELECT k, v FROM t ORDER BY k"))
        assert rows == pairs
        conn.close(force=True)
 
    def test_commit_blocking(self):
        conn = self._conn()
        conn.execute("CREATE TABLE t (k TEXT PRIMARY KEY)")
        conn.execute("INSERT INTO t VALUES (?)", ("x",))
        conn.commit(blocking=True)   # must not deadlock
        rows = list(conn.select("SELECT * FROM t"))
        assert len(rows) == 1
        conn.close(force=True)
 
    def test_close_joins_thread(self):
        conn = self._conn()
        conn.close()
        assert not conn.is_alive()
 
    def test_daemon_flag(self):
        conn = self._conn()
        assert conn.daemon is True
        conn.close(force=True)
 