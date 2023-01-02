from typing import Any, Iterable, Mapping, MutableMapping, Optional
import pickle

class APICursor:

    def __init__(self, **kwargs):
        self._cursor = {} # empty cursor

    @property
    def cursor(self) -> Mapping[str, Any]:
        return self._cursor

    @cursor.setter
    def cursor(self, value: Mapping[str, Any]):
        self._cursor = value
    
    def load_cursor(self, filename: str):
        try: # try in case file does not exist
            dbfile = open(filename, 'rb')
            self._cursor = pickle.load(dbfile)
            dbfile.close()
        except OSError:
            pass
    
    def store_cursor(self, filename: str):
        dbfile = open(filename, 'w+b')
        pickle.dump(self._cursor, dbfile)
        dbfile.close()
