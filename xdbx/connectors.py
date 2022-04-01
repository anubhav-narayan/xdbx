"""
Connector Classes for Collections,
Tables and BLOBs
"""
from .threads import SqliteMultiThread
from collections import UserDict


class Table(UserDict):
    def __init__(self, name: str, connection: SqliteMultiThread, flag: str,
                 primary_key_dtype: str = 'TEXT'):
        self.__conn = connection
        self.flag = flag
        self.name = name.replace('"', '""')
        # Check for the table or create new with
        # two columns named key(Primary Key) and
        # col1
        GET_ITEM = 'SELECT name FROM sqlite_master WHERE name = ?'
        item = self.__conn.select_one(GET_ITEM, (name,))
        if item is None:
            MAKE_TABLE = f'''\
            CREATE TABLE IF NOT EXISTS "{self.name}" (
                "key" {primary_key_dtype} PRIMARY KEY,
                "col1" TEXT
            )
            '''
            self.__conn.execute(MAKE_TABLE)
            self.__conn.commit()

    def describe(self):
        GET_COLS = f'PRAGMA TABLE_INFO("{self.name}")'
        data = self.__conn.select(GET_COLS)
        head = ['cid', 'name', 'type', 'notnull', 'default', 'primary key']
        from tabulate import tabulate
        return tabulate([x for x in data], head, tablefmt='grid')

    @property
    def xschema(self):
        GET_SQL = f'SELECT sql FROM sqlite_master WHERE "name" = "{self.name}"'
        schema = self.__conn.select_one(GET_SQL)[0]
        GET_COLS = f'PRAGMA TABLE_INFO("{self.name}")'
        data = self.__conn.select(GET_COLS)
        regex = r'\((?P<cols>.*)\)'
        import re
        cols = re.findall(regex, schema)[0].split(', ')
        return {
            'name': self.name,
            'cols': cols,
            'sql': schema
        }

    def __enter__(self):
        if not hasattr(self, 'conn') or self.conn is None:
            raise RuntimeError('Instance not connected')
        return self

    def __exit__(self, *exc_info):
        if self.__conn.autocommit:
            self.commit()

    def __repr__(self):
        from tabulate import tabulate
        header = self.columns
        items = self.__conn.select(f'SELECT * FROM "{self.name}"')
        data = [x for x in items] if items is not None else []
        return tabulate(data, header, tablefmt='grid')

    def __setitem__(self, key, value):
        if self.flag == 'r':
            raise RuntimeError('Refusing to write in read-only mode')

        if type(value) == tuple:
            if len(value) != len(self.columns) - 1:
                raise ValueError("Incorrect number of values")
            data = [key]
            data.extend([x for x in value])
            ADD_ITEM = f'REPLACE INTO "{self.name}" {tuple(self.columns)} VALUES ({", ".join(["?" for x in data])})'
        elif type(value) == dict:
            if key not in self:
                value[self.columns[0]] = key
                refs = [x for x in value]
                data = [x for x in value.values()]
                ADD_ITEM = f'REPLACE INTO "{self.name}" {tuple(refs)} VALUES ({", ".join(["?" for x in data])})'
            else:
                data = [f'{x} = ?' for x in value]
                ADD_ITEM = f'UPDATE "{self.name}" SET {", ".join(data)} WHERE {self.columns[0]} = ?'
                data = [x for x in value.values()]
                data.append(key)
        else:
            raise TypeError("Incorrect value format, use tuple or dict")
        self.__conn.execute(ADD_ITEM, tuple(data))
        if self.__conn.autocommit:
            self.commit()

    # Refactor This
    def __getitem__(self, args):
        # args is key
        if type(args) is not tuple:
            if isinstance(args, slice):
                GET_ITEM = f'SELECT * FROM "{self.name}" WHERE '\
                         + f'"{self.columns[0]}" BETWEEN {args.start or min(self.keys())} '\
                         + f'AND {args.stop-1 if args.stop is not None else max(self.keys())}'
                item = self.__conn.select(GET_ITEM)
                if item is None:
                    raise KeyError(args[0])
                return [x for x in item][::args.step]
            elif args in self.columns:
                GET_ITEM = f'SELECT "{args}" FROM "{self.name}"'
                item = self.__conn.select(GET_ITEM)
                return [x[0] for x in item]
            else:
                GET_ITEM = f'SELECT * FROM "{self.name}" WHERE "{self.columns[0]}" = ?'
                item = self.__conn.select_one(GET_ITEM, (args, ))
                if item is None:
                    raise KeyError(args[0])
                return item[1:]
        # column select
        if len(args) == 2:
            if args[0] in self:
                GET_ITEM = f'SELECT "{args[1]}" FROM "{self.name}" WHERE "{self.columns[0]}" = ?'
                item = self.__conn.select_one(GET_ITEM, (args[0], ))
                if item is None:
                    raise KeyError(args[0])
                return item
            elif args[0] in self.columns:
                if isinstance(args[1], slice):
                    if args[1].stop is None:
                        GET_ITEM = f'SELECT * FROM "{self.name}" WHERE '\
                                 + f'"{args[0]}" >= {args[1].start} '
                        item = self.__conn.select(GET_ITEM)
                        if item is None:
                            raise KeyError("No entry for given condition")
                        return [x for x in item]
                    elif args[1].start is None:
                        GET_ITEM = f'SELECT * FROM "{self.name}" WHERE '\
                                 + f'"{args[0]}" < {args[1].stop}'
                        item = self.__conn.select(GET_ITEM)
                        if item is None:
                            raise KeyError("No entry for given condition")
                        return [x for x in item]
                    else:
                        GET_ITEM = f'SELECT * FROM "{self.name}" WHERE '\
                                 + f'"{args[0]}" BETWEEN {args[1].start} '\
                                 + f'AND {args[1].stop-1}'
                        item = self.__conn.select(GET_ITEM)
                        if item is None:
                            raise KeyError("No entry for given condition")
                        return [x for x in item]
                else:
                    GET_ITEM = f'SELECT * FROM "{self.name}" WHERE '\
                             + f'"{args[0]}" = ?'
                    item = self.__conn.select(GET_ITEM, (args[1], ))
                    if item is None:
                        raise KeyError("No Entry for given condition")
                    return [x for x in item]

    def __contains__(self, key):
        HAS_ITEM = f'SELECT 1 FROM "{self.name}" WHERE "{self.columns[0]}" = ?'
        return self.__conn.select_one(HAS_ITEM, (key,)) is not None

    def __delitem__(self, key):
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete in read-only mode')

        if key not in self:
            raise KeyError(key)
        DEL_ITEM = f'DELETE FROM "{self.name}" WHERE {self.columns[0]} = ?'
        self.__conn.execute(DEL_ITEM, (key,))
        if self.__conn.autocommit:
            self.commit()

    def __del__(self):
        if self.__conn.autocommit:
            self.commit()
        self.__conn.close(True)

    def rename(self):
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete in read-only mode')

    def keys(self):
        '''
        Return Primary Keys Generator
        '''
        GET_KEYS = f'SELECT "{self.columns[0]}" FROM "{self.name}" ORDER BY rowid'
        for x in self.__conn.select(GET_KEYS):
            yield x[0]

    def __iter__(self):
        return self.keys()

    @property
    def columns(self) -> list:
        '''
        Return s a list of column names
        '''
        GET_COLS = f'PRAGMA TABLE_INFO("{self.name}")'
        data = self.__conn.select(GET_COLS)
        return [x[1] for x in data]

    def add_foreign_key(self, colname: str, references: str):
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete in read-only mode')

        FRN_KEY = f'ALTER TABLE "{self.name}" ADD COLUMN "{colname}" TEXT'\
            + f' REFERENCES "{references}"'
        self.__conn.execute(FRN_KEY)
        if self.__conn.autocommit:
            self.commit()

    def add_column(self, colname: str, dtype: str = 'TEXT'):
        '''
        Simply add a new column
        '''
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete in read-only mode')

        NEW_COL = f'ALTER TABLE "{self.name}" ADD COLUMN "{colname}" TEXT'
        self.__conn.execute(NEW_COL)
        if self.__conn.autocommit:
            self.commit()

    def drop_column(self, colname: str):
        '''
        Drop a Column
        '''
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete in read-only mode')

        DROP_COL = f'ALTER TABLE "{self.name}" DROP COLUMN "{colname}"'
        self.__conn.execute(DROP_COL)
        if self.__conn.autocommit:
            self.commit()

    def rename_column(self, colname: str, new_colname: str,
                      dtype: str = 'TEXT'):
        '''
        Rename a column
        '''
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete in read-only mode')

        REM_COL = f'ALTER TABLE "{self.name}"\
         RENAME COLUMN "{colname}" TO "{new_colname}"'
        self.__conn.execute(REM_COL)
        if self.__conn.autocommit:
            self.commit()

    def __len__(self):
        GET_LEN = f'SELECT COUNT(*) FROM "{self.name}"'
        rows = self.__conn.select_one(GET_LEN)[0]
        return rows if rows is not None else 0

    def query(self, query: str, select: bool = False):
        if select:
            return [x for x in self.__conn.select(query)]
        self.__conn.execute(query)

    def commit(self, blocking=True):
        '''
        From sqlitedict
        Persist all data to disk.

        When `blocking` is False, the commit command is queued, but the data is
        not guaranteed persisted (default implication when autocommit=True).
        '''
        if self.__conn is not None:
            self.__conn.commit(blocking)

    def to_dict(self, otype: str = 'column'):
        '''
        Return the Table is a dict
        otype: str - dict format to use
        '''
        cols = self.columns
        ret_dict = {}
        # Much faster
        if otype == 'column':
            for x in cols:
                ret_dict[x] = self[x]
            return ret_dict
        elif otype == 'list':
            ret_dict[self.name] = []
            temp_dict = {}
            for x in cols:
                temp_dict[x] = self[x]
            for x in range(0, len(temp_dict[cols[0]])):
                _add_temp = {}
                for y in cols:
                    _add_temp[y] = temp_dict[y][x]
                ret_dict[self.name].append(_add_temp)
            return ret_dict
        # Very VERY Slow
        # But true dict
        elif otype == 'dict':
            for x in self:
                ret_dict[x] = {
                    k: v for k, v in zip(cols[1:], self[x])
                }
            return ret_dict
        else:
            raise TypeError(
                'Please use \'column\', \'list\' or \'dict\' as otype'
            )

    def to_sql(self):
        '''
        Returns the table in SQLite Syntax
        '''
        GET_SQL = f'SELECT sql FROM sqlite_master WHERE "name" = "{self.name}"'
        CREATE = self.__conn.select_one(GET_SQL)[0]
        VALUES = ',\n'.join([str(x) for x in self[::]])
        INSERT = f'INSERT INTO "{self.name}" VALUES\n' + VALUES
        return f'{CREATE};\n{INSERT};'


class JSON_Storage():
    def __init__(self, name: str, connection: SqliteMultiThread, flag: str,
                 primary_key_dtype: str = 'TEXT'):
        self.__conn = connection
        self.flag = flag
        self.name = name.replace('"', '""')
        # Check for the table or create new with
        # two columns named key(Primary Key) and
        # col1
        GET_ITEM = 'SELECT name FROM sqlite_master WHERE name = ?'
        item = self.__conn.select_one(GET_ITEM, (name,))
        if item is None:
            MAKE_TABLE = f'''\
            CREATE TABLE IF NOT EXISTS "{self.name}" (
                "key" {primary_key_dtype} PRIMARY KEY,
                "object" TEXT
            )
            '''
            self.__conn.execute(MAKE_TABLE)
            self.__conn.commit()

    def describe(self):
        GET_COLS = f'PRAGMA TABLE_INFO("{self.name}")'
        data = self.__conn.select(GET_COLS)
        head = ['cid', 'name', 'type', 'notnull', 'default', 'primary key']
        from tabulate import tabulate
        return tabulate([x for x in data], head, tablefmt='grid')

    @property
    def xschema(self):
        GET_SQL = f'SELECT sql FROM sqlite_master WHERE "name" = "{self.name}"'
        schema = self.__conn.select_one(GET_SQL)[0]
        GET_COLS = f'PRAGMA TABLE_INFO("{self.name}")'
        data = self.__conn.select(GET_COLS)
        regex = r'\((?P<cols>.*)\)'
        import re
        cols = re.findall(regex, schema)[0].split(', ')
        return {
            'name': self.name,
            'cols': cols,
            'sql': schema
        }

    def __enter__(self):
        if not hasattr(self, 'conn') or self.conn is None:
            raise RuntimeError('Instance not connected')
        return self

    def __exit__(self, *exc_info):
        if self.__conn.autocommit:
            self.commit()

    def __repr__(self):
        from tabulate import tabulate
        header = self.columns
        items = self.__conn.select(f'SELECT `key` FROM "{self.name}"')
        data = [x for x in items] if items is not None else []
        return tabulate(data, header, tablefmt='grid')

    def __len__(self):
        GET_LEN = f'SELECT COUNT(*) FROM "{self.name}"'
        rows = self.__conn.select_one(GET_LEN)[0]
        return rows if rows is not None else 0

    def __contains__(self, key):
        HAS_ITEM = f'SELECT 1 FROM "{self.name}" WHERE "key" = ?'
        return self.__conn.select_one(HAS_ITEM, (key,)) is not None

    def keys(self):
        '''
        Return Primary Keys Generator
        '''
        GET_KEYS = f'SELECT "key" FROM "{self.name}" ORDER BY rowid'
        for x in self.__conn.select(GET_KEYS):
            yield x[0]

    def __setitem__(self, key, value: dict):
        if self.flag == 'r':
            raise RuntimeError('Refusing to write in read-only mode')

        if type(value) == dict:
            import json
            if key not in self:
                data = (key, json.dumps(value))
                ADD_ITEM = f'REPLACE INTO "{self.name}"\
                 ("key", "object") VALUES ({key}, {value})'
            else:
                ADD_ITEM = f'UPDATE "{self.name}"\
                 SET "value" = ? WHERE "key" = ?'
                data = (value, key)
        else:
            raise TypeError("Incorrect value format, use dict")
        self.__conn.execute(ADD_ITEM, data)
        if self.__conn.autocommit:
            self.commit()

    def __getitem__(self, key):
        # args is key
        GET_ITEM = f'SELECT value FROM "{self.name}" WHERE "key" = ?'
        item = self.__conn.select_one(GET_ITEM, (key,))
        if item is None:
            raise KeyError(key)
        return item[0]

