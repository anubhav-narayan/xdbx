"""
NoSQLite3 Database Class
"""
import os
from collections import UserDict
from .threads import SqliteMultiThread
from .logger import logger
from .storages import Table, JSONStorage


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
      'c': default mode, open for read/write, creating the db if necessary.
      'w': open for r/w, but drop contents first (start with empty table)
      'r': open as read-only

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
        self.filename = filename if filename == ':memory:'\
            else os.path.abspath(filename)
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

    def __getitem__(self, *args):
        if type(args[0]) == str:  # Single arg
            table_name = args[0]
            return JSONStorage(table_name, self.conn, self.flag)
        elif type(args[0]) == tuple:  # Type arg
            table_name = args[0][0]
            astype = args[0][1]
            if astype == 'table':
                return Table(table_name, self.conn, self.flag)
            elif astype == 'json':
                return JSONStorage(table_name, self.conn, self.flag)

    def __setitem__(self, key, item):
        if type(item) is Table or JSONStorage:
            # Only copy schema not data
            if item.filename is not self.filename:
                COPY = item.xschema['sql']
                key = key.replace('"', '""')
                COPY.replace(f'{item.name}', f'{key}')
                self.conn.execute(COPY)
            elif item.name not in self:
                # Fancy rename without data
                COPY = item.xschema['sql']
                key = key.replace('"', '""')
                COPY.replace(f'{item.name}', f'{key}')
                self.conn.execute(COPY)

    def __repr__(self):
        return self.__str__()

    def describe(self):
        GET_ALL = 'SELECT * FROM sqlite_master\
                   ORDER BY rowid'
        head = ['type', 'name', 'tbl_name', 'rootpage']
        items = self.conn.select(GET_ALL)
        from tabulate import tabulate
        return tabulate([x[:4] for x in items], head, tablefmt='grid')

    def __iter__(self):
        GET_TABLES = 'SELECT name FROM sqlite_master WHERE type="table"\
                      ORDER BY rowid'
        for key in self.conn.select(GET_TABLES):
            yield key[0]

    def __contains__(self, name):
        HAS_ITEM = 'SELECT 1 FROM sqlite_master WHERE name = ?'
        return self.conn.select_one(HAS_ITEM, (name,)) is not None

    @property
    def storages(self):
        return [x for x in self.keys()]

    @property
    def indices(self):
        GET_INDEX = 'SELECT name FROM sqlite_master WHERE type="index"\
                     ORDER BY rowid'
        items = self.conn.select(GET_INDEX)
        return [x[0] for x in items]

    @property
    def views(self):
        GET_VIEW = 'SELECT name FROM sqlite_master WHERE type="view"\
                    ORDER BY rowid'
        items = self.conn.select(GET_VIEW)
        return [x[0] for x in items]

    def close(self, do_log=True, force=False):
        if do_log:
            logger.debug(f"Closing {self}")
        if hasattr(self, 'conn') and self.conn is not None:
            if self.conn.autocommit and not force:
                # typically calls to commit are non-blocking when autocommit is
                # used.  However, we need to block on close() to ensure any
                # awaiting exceptions are handled and that all data is
                # persisted to disk before returning.
                self.conn.commit(blocking=True)
            self.conn.close(force=force)
            self.conn = None

    def __delitem__(self, table_name: str):
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete in read-only mode')

        DEL_ITEM = f'DROP TABLE "{table_name}"'
        self.conn.execute(DEL_ITEM)
        if self.conn.autocommit:
            self.commit()
