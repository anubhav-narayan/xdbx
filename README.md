XDBX - XDBX DataBase eXtention
====================================

![License](https://img.shields.io/badge/licence-MIT-green)

A Python3 wrapper around sqlite3 database to use it as a NoSQL storage,
Pythonic dict-like interface and support for multi-thread access derived
from [sqlitedict](https://github.com/RaRe-Technologies/sqlitedict) exclusively for
Python3.6+


```python
from xdbx.database import Database
mydict = Database('./my_db.sqlite', autocommit=True)
mydict['some_key'] = 'any_picklable_object'
print(mydict['some_key'])  # prints the new value
any_picklable_object
for key, value in mydict.items():
    print(key, value)
some_key any_picklable_object
print(len(mydict)) # etc. all dict functions work
mydict.close()
