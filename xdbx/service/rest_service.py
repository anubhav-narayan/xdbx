import logging
from traceback import TracebackException
from typing import Generator, Optional, Any
from fastapi import FastAPI, Request, status
from fastapi.responses import StreamingResponse, JSONResponse
from contextlib import asynccontextmanager
from pydantic import BaseModel

from xdbx.storages import JSONStorage
from ..database import Database


store: dict[str, Database] = {}

logging.basicConfig(
    level=logging.DEBUG,
    format='%(levelname)s:\t[%(name)s] %(message)s'
)
log = logging.getLogger("XDBX REST Service")

class QueryPacket(BaseModel):
    query: Optional[str] = ''
    value: Optional[Any] = None

class ControlStorePacket(BaseModel):
    command: str
    database: str
    value: Optional[Any]=None

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Starting Up!")
    yield
    log.info("Shutting Down...")
    for x in store:
        log.info(f"Closing {x}...")
        store[x].close()
    log.info("Goodbye!")

app = FastAPI(
    title="XDBX REST Service",
    description="REST service for XDBX based SQLite3 databases",
    version='0.1.0',
    lifespan=lifespan
)

@app.post('/store')
def control_store(req: ControlStorePacket):
    if req.command == "CREATE":
        try:
            store[req.database] = Database(req.database, autocommit=True, journal_mode='WAL')
            return JSONResponse(
                content={"status": "Success", "message": f"Created {req.database}"},
                status_code=201
            )
        except Exception as e:
            e = TracebackException.from_exception(e)
            log.error('\n'.join([x for x in e.format()]))
            return JSONResponse(
                content={"status": "Error", "message": f"Server Error!"},
                status_code=500
            )
    if req.command == "DELETE":
        try:
            del store[req.database]
            return JSONResponse(
                content={"status": "Success", "message": f"Deleted {req.database}"},
                status_code=200
            )
        except KeyError:
            return JSONResponse(
                content={"status": "Error", "message": f"No Database {req.database} found!"},
                status_code=404
            )
        except Exception as e:
            e = TracebackException.from_exception(e)
            log.error('\n'.join([x for x in e.format()]))
            return JSONResponse(
                content={"status": "Error", "message": f"{e}"},
                status_code=500
            )
    if req.command == "LIST":
        try:
            db = store[req.database]
            return JSONResponse(
                content={"status": "Success", "message": f"Found {len(db.storages)} rows", "value": f"{db.storages}"},
                status_code=200
            )
        except KeyError:
            return JSONResponse(
                content={"status": "Error", "message": f"No Database {req.database} found!"},
                status_code=404
            )
        except Exception as e:
            e = TracebackException.from_exception(e)
            log.error('\n'.join([x for x in e.format()]))
            return JSONResponse(
                content={"status": "Error", "message": f"{e}"},
                status_code=500
            )


@app.get("/{db}")
def query(db: str):
    try:
        db_ = store[db]
        return JSONResponse(
                content={"status": "Success", "message": f"Found {len(db_.storages)} rows", "value": f"{db_.storages}"},
                status_code=200
            )
    except KeyError:
        return JSONResponse(
            content={"status": "Error", "message": f"No Database {db} found!"},
            status_code=404
        )
    except Exception as e:
        e = TracebackException.from_exception(e)
        log.error('\n'.join([x for x in e.format()]))
        return JSONResponse(
            content={"status": "Error", "message": f"{e}"},
            status_code=500
        )

@app.get("/{db}/{storage}")
def query_db(db: str, storage: str):
    import json
    try:
        db_ = store[db]
        st_ = db_[storage]
        def json_array_stream(st):
            yield '['  # start of JSON array
            first = True
            for item in st:
                if not first:
                    yield ','  # comma before subsequent items
                else:
                    first = False
                yield json.dumps(item)
            yield ']'  # end of JSON array
        return StreamingResponse(
            content=json_array_stream(st_.get_path("*", None)),
            media_type='application/json'
        )
    except KeyError:
        return JSONResponse(
            content={"status": "Error", "message": f"No Database {db}/{storage} found!"},
            status_code=404
        )
    except Exception as e:
        e = TracebackException.from_exception(e)
        log.error('\n'.join([x for x in e.format()]))
        return JSONResponse(
            content={"status": "Error", "message": f"{e}"},
            status_code=500
        )


@app.get("/{db}/{storage}/{query:path}")
def query_st(db: str, storage: str, query: str):
    import json
    try:
        db_ = store[db]
        st_ = db_[storage]
        def json_array_stream(st):
            yield '['  # start of JSON array
            first = True
            for item in st:
                if not first:
                    yield ','  # comma before subsequent items
                else:
                    first = False
                yield json.dumps(item)
            yield ']'  # end of JSON array
        return StreamingResponse(
            content=json_array_stream(st_.get_path(query, None)),
            media_type='application/json'
        )
    except KeyError:
        return JSONResponse(
            content={"status": "Error", "message": f"No Database {db}/{storage} found!"},
            status_code=404
        )
    except Exception as e:
        e = TracebackException.from_exception(e)
        log.error('\n'.join([x for x in e.format()]))
        return JSONResponse(
            content={"status": "Error", "message": f"{e}"},
            status_code=500
        )