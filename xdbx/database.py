"""
NoSQLite3 Database Class
"""
import os
import sys
import logging
import traceback

from collections import UserDict
from .threads import SqliteMultiThread
from .connectors import Table, JSON_Storage


class Database(UserDict):
    """
    Initialize a thread-safe SQLite3 Database. The dictionary will
    be a database file `filename` containing multiple tables.
    This class provides an upper level hierarchy of the SqliteDict
    by using a similar structure, modifications are limited.

    If no `filename` is given, the database is in memory.

    If you enable `autocommit`, changes will be committed after each
    operation (more inefficient but safer). Otherwise, changes are
    committed on `self.commit()`, `self.clear()` and `self.close()`.

    Set `journal_mode` to 'OFF' if you're experiencing sqlite I/O problems
    or if you need performance and don't care about crash-consistency.

    The `flag` parameter. Exactly one of:
      'c': default mode, open for read/write, creating the dbif necessary.
      'w': open for r/w, but drop contents first (start with empty table)
      'r': open as read-only

    The `encode` and `decode` parameters are used to customize how the
    values are serialized and deserialized.
    The `encode` parameter must be a function that takes a single Python
    object and returns a serialized representation.
    The `decode` function must be a function that takes the serialized
    representation produced by `encode` and returns a deserialized Python
    object.
    The default is to use pickle.

    The `timeout` defines the maximum time (in seconds) to wait for
    initial Thread startup.
    """
    VALID_FLAGS = ['c', 'r', 'w']

    def __init__(self, filename=':memory:', flag='c',
                 autocommit=False, journal_mode="DELETE", timeout=5):
        if flag not in Database.VALID_FLAGS:
            raise RuntimeError(f"Unrecognized flag: {flag}")
        self.flag = flag
        if flag == 'w':
            if os.path.exists(filename):
                os.remove(filename)
        dir_ = os.path.dirname(filename)
        if dir_:
            if not os.path.exists(dir_):
                raise RuntimeError(
                    f'Error! The directory does not exist, {dir_}'
                )
        self.filename = filename
        self.autocommit = autocommit
        self.journal_mode = journal_mode
        self.timeout = timeout
        self.conn = self.__connect()

    def __connect(self):
        return SqliteMultiThread(self.filename, autocommit=self.autocommit,
                                 journal_mode=self.journal_mode,
                                 timeout=self.timeout)

    def __enter__(self):
        if not hasattr(self, 'conn') or self.conn is None:
            self.conn = self._new_conn()
        return self

    def __exit__(self, *exc_info):
        self.close()

    def __str__(self):
        return f'Database: {self.filename}'

    def __getitem__(self, table_name: str, astype: str = 'table'):
        if astype == 'table':
            return Table(table_name, self.conn, self.flag)
        if astype == 'json':
            if table_name in self.keys():
                GET_COLS = f'PRAGMA TABLE_INFO("{table_name}")'
                data = self.conn.select(GET_COLS)
                if len(data) > 2:
                    raise TypeError(f"{table_name} has more than 2 columns, can't be cast as JSON Storage")
                return JSON_Storage(table_name, self.conn, self.flag)
            return JSON_Storage(table_name, self.conn, self.flag)

    def __repr__(self):
        return self.__str__()

    def keys(self):
        GET_TABLES = 'SELECT name FROM sqlite_master WHERE type="table"'
        for key in self.conn.select(GET_TABLES):
            yield key[0]

    def __contains__(self, name):
        HAS_ITEM = 'SELECT 1 FROM sqlite_master WHERE name = ?'
        return self.conn.select_one(HAS_ITEM, (name,)) is not None

    @property
    def storages(self):
        GET_TABLES = 'SELECT * FROM sqlite_master WHERE type="table" ORDER BY rowid'
        res = self.conn.select(GET_TABLES)
        res = [x for x in res]
        return res

    def __delitem__(self, table_name: str):
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete in read-only mode')

        DEL_ITEM = f'DROP TABLE "{table_name}"'
        self.__conn.execute(DEL_ITEM)
        if self.__conn.autocommit:
            self.commit()
