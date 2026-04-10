from .threads import SqliteMultiThread


class Transaction:
    def __init__(self, name: str, connection: SqliteMultiThread):
        self.name = name.replace('"', '""')
        self.conn = connection
        self.active = False

    def begin(self):
        if self.active:
            raise RuntimeError("Transaction already active")
        self.conn.transaction_depth += 1
        self.conn.execute("BEGIN TRANSACTION;")
        self.active = True

    def commit(self):
        if not self.active:
            raise RuntimeError("No active transaction")
        self.conn.execute("COMMIT;")
        self.conn.transaction_depth = 0
        self.active = False
    
    def savepoint(self, name: str = ""):
        sp_name = name.replace('"', '""') if name else self.name
        self.conn.transaction_depth += 1
        self.conn.execute(f'SAVEPOINT "{sp_name}";')

    def rollback(self):
        self.conn.execute(f'ROLLBACK;')
        self.conn.transaction_depth = 0
        self.active = False
    
    def rollback_to(self, to: str):
        self.conn.execute(f'ROLLBACK TO SAVEPOINT "{to}";')
        self.conn.transaction_depth = max(0, self.conn.transaction_depth - 1)

    def release(self, from_: str = ""):
        target = from_ or self.name
        self.conn.execute(f'RELEASE SAVEPOINT "{target}";')
        self.conn.transaction_depth = max(0, self.conn.transaction_depth - 1)

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
            return False
        self.commit()
        return False