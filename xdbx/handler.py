from .protocol import parse_request, format_response
from .database import Database

def dispatch(data, store):
    try:
        req = parse_request(data)
        command = req["command"]
        db = req["database"]
        st = req["storage"]
        key = req["key"]
        value = req.get("value", None)

        if command == "CREATE":
            store[db] = Database(db, journal_mode='WAL')
            st_ = store[db][st, 'json']
            return format_response("success", f"Created {db}/{st}")

        elif command == "GET":
            try:
                db = store[db]
            except KeyError:
                return format_response("error", f"Database {db} not found")
            try:
                st_ = db[st]
            except KeyError:
                return format_response("error", f"Storage {db}/{st} not found")
            try:
                value = st_[key]
            except KeyError:
                return format_response("error", f"Key {db}/{st}/{key} not found")
            return format_response("success", value=value)

        elif command == "SET":
            try:
                db = store[db]
            except KeyError:
                return format_response("error", f"Database {db} not found")
            try:
                st_ = db[st]
            except KeyError:
                return format_response("error", f"Storage {st} not found")
            st_[key] = value
            return format_response("success", f"Updated {db}/{st}/{key}")

        elif command == "DELETE":
            # store.delete(db, st, key)
            return format_response("success", "Deleted")

        return format_response("error", "Unknown command")
    except Exception as e:
        return format_response("error", str(e))
