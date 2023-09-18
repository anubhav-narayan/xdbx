XDBX - XDBX DataBase eXtention
====================================

![License](https://img.shields.io/badge/licence-MIT-green)

A Python3 wrapper around sqlite3 database, can also be used as a JSON document storage.
Pythonic dict-like interface and support for multi-thread access derived
from [sqlitedict](https://github.com/RaRe-Technologies/sqlitedict) exclusively for
Python3.6+.


```python
from xdbx import Database
db = Database('./my_db.sqlite', autocommit=True)
tab = db['mytab']
tab['some_key'] = 'any_picklable_object'
print(tab['some_key'])  # prints the value
for key, value in mydict.items():
    print(key, value)
print(len(mydict)) # etc. most dict functions work
mydict.close() # close connection
```

