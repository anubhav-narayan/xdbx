from .threads import SqliteMultiThread


class Transaction:
    def __init__(self, name: str, connection: SqliteMultiThread):
        self.name = name.replace('"', '""')
        self.conn = connection
        self.active = False

    def begin(self):
        if self.active:
            raise RuntimeError("Transaction already active")
        self.conn.execute("BEGIN TRANSACTION;")
        self.conn.execute(f'SAVEPOINT "{self.name}";')
        self.active = True

    def commit(self):
        if not self.active:
            raise RuntimeError("No active transaction")
        self.conn.commit()
        self.active = False

    def rollback(self, to: str = ""):
        target = to or self.name
        self.conn.execute(f'ROLLBACK TO "{target}";')
        self.active = False

    def release(self, from_: str = ""):
        target = from_ or self.name
        self.conn.execute(f'RELEASE SAVEPOINT "{target}";')
        self.active = False

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
            raise exc_val  # re-raise the original exception
        else:
            self.commit()