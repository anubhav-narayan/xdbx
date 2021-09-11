XDBX - XDBX DataBase eXtention
====================================

![License](https://img.shields.io/badge/licence-MIT-green)

A Python3 wrapper around sqlite3 database to use it as a NoSQL storage,
Pythonic dict-like interface and support for multi-thread access derived
from [sqlitedict](https://github.com/RaRe-Technologies/sqlitedict) exclusively for
Python3.6+


```python
from xdbx.database import Database
mydict = SqliteDict('./my_db.sqlite', autocommit=True)
mydict['some_key'] = 'any_picklable_object'
print(mydict['some_key'])  # prints the new value
any_picklable_object
for key, value in mydict.items():
    print(key, value)
some_key any_picklable_object
print(len(mydict)) # etc. all dict functions work
mydict.close()
```

`dill` is used internally to (de)serialize the values. Keys are
arbitrary strings, values arbitrary pickle-able objects.

If you don't use autocommit (default is no autocommit for performance),
then don't forget to call `mydict.commit()` when done with a
transaction:

```python
# using SqliteDict as context manager works too (RECOMMENDED)
with SqliteDict('./my_db.sqlite') as mydict:  # note no autocommit=True
    mydict['some_key'] = "first value"
    mydict['another_key'] = range(10)
    mydict.commit()
    mydict['some_key'] = "new value"
    # no explicit commit here
with SqliteDict('./my_db.sqlite') as mydict:  # re-open the same DB
    print(mydict['some_key'])  # outputs 'first value', not 'new value'
```

Features
--------

-   Values can be **any picklable objects** (uses [`dill`](https://pypi.org/project/dill/)).
-   Support for **multiple tables** (as dicts) living in the same database
    file.
-   Support for **access from multiple threads** to the same connection
    (needed by e.g. Pyro). Vanilla sqlite3 gives you
    `ProgrammingError: SQLite objects created in a thread can only be used in that same thread.`

    Concurrent requests are still serialized internally, so this
    "multithreaded support" **doesn't** give you any performance
    benefits. It is a work-around for sqlite limitations in Python.

-   Support for **custom serialization or compression**:

``` python
# use JSON instead of dill
import json
mydict = SqliteDict('./my_db.sqlite', encode=json.dumps, decode=json.loads)

# apply zlib compression after pickling
import zlib, pickle, sqlite3
def my_encode(obj):
    return sqlite3.Binary(zlib.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)))
def my_decode(obj):
    return pickle.loads(zlib.decompress(bytes(obj)))
mydict = SqliteDict('./my_db.sqlite', encode=my_encode, decode=my_decode)
```

Installation
------------

Via PyPI:
<!-- ```bash -->
<!-- pip install -U sqlitedict -->
<!-- ``` -->
We will be on PyPI soon

From the source:
```bash
python setup.py install
```

Documentation
-------------

Standard Python document strings are inside the module:

``` python
import sqlitedict
help(sqlitedict)
```

(but it's just `dict` with a commit, really).

**Beware**: because of Python semantics, `sqlitedict` cannot know when a
mutable SqliteDict-backed entry was modified in RAM. For example,
`mydict.setdefault('new_key', []).append(1)` will leave
`mydict['new_key']` equal to empty list, not `[1]`. You'll need to
explicitly assign the mutated object back to SqliteDict to achieve the
same effect:

``` python
val = mydict.get('new_key', [])
val.append(1)  # sqlite DB not updated here!
mydict['new_key'] = val  # now updated
```

For developers
--------------
To perform all tests with coverage:
```bash
pytest --cov=sqlitedict
```

Comments, bug reports
---------------------

`sqlitedict3` resides on
[github](). You can file
issues or pull requests there.

* * * * *

`sqlitedict3` is open source software released under the [MIT](https://opensource.org/licenses/mit).\
Original work by [Radim Řehůřek](http://radimrehurek.com) and contributors.\
Copyright (c) 2021, Anubhav Mattoo
