# coding: utf_8
import collections
from contextlib import closing
import os
from pathlib import Path
import pickle
import sqlite3
from typing import Any, Dict, Iterator, Union, cast

SQL_DDL = '''
CREATE TABLE IF NOT EXISTS "sdbm" (
    "key" BLOB,
    "value" BLOB NOT NULL,
    PRIMARY KEY("key")
);
CREATE UNIQUE INDEX IF NOT EXISTS "sdbm_index" ON "sdbm" ("key");
'''
SQL_ITER = 'SELECT "key" FROM "sdbm"'
SQL_LEN = 'SELECT count(*) FROM "sdbm"'
SQL_DELITEM = 'DELETE FROM "sdbm" WHERE "key" = ?'
SQL_GETITEM = 'SELECT "key", "value" FROM "sdbm" WHERE "key" = ?'
SQL_SETITEM_S = 'SELECT "key", "value" FROM "sdbm" WHERE "key" = ?'
SQL_SETITEM_U = 'UPDATE "sdbm" SET "value" = ? WHERE "key" = ?'
SQL_SETITEM_I = 'INSERT INTO "sdbm" VALUES (?, ?)'


class _Database(collections.abc.MutableMapping):
    _con: sqlite3.Connection
    _readonly: bool

    def __init__(self: '_Database', name: Path, mode: int, flag='c'):
        self._readonly = (flag == 'r')

        if flag == 'n':
            try:
                name.unlink()
            except FileNotFoundError:
                pass
        if flag in 'rw' and not name.is_file():
            raise FileNotFoundError()

        self._con = sqlite3.connect(name)
        self._con.executescript(SQL_DDL)
        name.chmod(mode)

    def __del__(self: '_Database') -> None:
        self.close()

    def __enter__(self: '_Database') -> '_Database':
        return self

    def __exit__(self: '_Database', exc_type, exc_value, traceback) -> None:
        self.close()

    def __iter__(self: '_Database') -> Iterator[str]:
        with closing(self._con.cursor()) as cur:
            cur.execute(SQL_ITER)
            for r in cur:
                yield r[0]

    def __len__(self: '_Database') -> int:
        with closing(self._con.cursor()) as cur:
            cur.execute(SQL_LEN)
            return cur.fetchone()[0]

    def __delitem__(self: '_Database', key: bytes) -> None:
        with closing(self._con.cursor()) as cur:
            cur.execute(SQL_DELITEM, (key, ))

    def __getitem__(self: '_Database', key: bytes) -> Any:
        with closing(self._con.cursor()) as cur:
            cur.execute(SQL_GETITEM, (key, ))
            kv = cur.fetchone()
            if not kv:
                raise KeyError()
            return pickle.loads(kv[1])

    def __setitem__(self: '_Database', key: bytes, value: bytes) -> None:
        if self._readonly:
            raise error('The database is opened for reading only')
        if not isinstance(key, (str, bytes, bytearray)):
            raise TypeError('keys must be bytes or strings')

        pv = pickle.dumps(value)
        with closing(self._con.cursor()) as cur:
            cur.execute(SQL_SETITEM_S, (key, ))
            kv = cur.fetchone()
            try:
                if kv:
                    cur.execute(SQL_SETITEM_U, (pv, key))
                else:
                    cur.execute(SQL_SETITEM_I, (key, pv))
                self._con.commit()
            except Exception:
                self._con.rollback()
                raise

    def close(self: '_Database') -> None:
        self._con.close()

    def sync(self: '_Database') -> None:
        pass


class _DatabaseType(Dict[bytes, Any]):
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class error(Exception):
    pass


def open(name: Union[Path, str],
         flag: str = 'c',
         mode: int = 0o666) -> _DatabaseType:
    try:
        um = os.umask(0)
        os.umask(um)
    except AttributeError:
        pass
    else:
        mode = mode & (~um)
    if flag not in ('r', 'w', 'c', 'n'):
        raise ValueError("Flag must be one of 'r', 'w', 'c', or 'n'")
    return cast(_DatabaseType, _Database(Path(name), mode, flag=flag))
