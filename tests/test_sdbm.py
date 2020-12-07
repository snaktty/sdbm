# coding: utf_8
from pathlib import Path
import shelve
import tempfile
import pytest  # type: ignore
import sdbm


def test_del_get_set_item() -> None:
    with tempfile.TemporaryDirectory() as d:
        with sdbm.open(Path(d) / 'test.db') as db:
            with pytest.raises(KeyError):
                db[b'a']
            db[b'a'] = b'b'
            assert db[b'a'] == b'b'
            db[b'a'] = b'c'
            assert db[b'a'] == b'c'
            del db[b'a']
            with pytest.raises(KeyError):
                db[b'a']


def test_iter_len() -> None:
    with tempfile.TemporaryDirectory() as d:
        with sdbm.open(Path(d) / 'test.db') as db:
            db[b'a'] = b'b'
            db[b'c'] = b'd'
            assert set(db) == {b'a', b'c'}
            assert len(db) == 2


def test_open_r() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / 'test.db'
        with pytest.raises(FileNotFoundError):
            with sdbm.open(p, flag='r'):
                pass

        with sdbm.open(p) as db:
            db[b'a'] = b'b'

        with sdbm.open(p, flag='r') as db:
            assert db[b'a'] == b'b'
            with pytest.raises(sdbm.error):
                db[b'a'] = b'c'


def test_open_w() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / 'test.db'
        with pytest.raises(FileNotFoundError):
            with sdbm.open(p, flag='w'):
                pass

        with sdbm.open(p) as db:
            db[b'a'] = b'b'

        with sdbm.open(p, flag='w') as db:
            assert db[b'a'] == b'b'
            db[b'a'] = b'c'
            assert db[b'a'] == b'c'


def test_open_c() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / 'test.db'
        with sdbm.open(p) as db:
            db[b'a'] = b'b'
            assert db[b'a'] == b'b'

        with sdbm.open(p) as db:
            assert db[b'a'] == b'b'


def test_open_n() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / 'test.db'
        with sdbm.open(p, flag='n') as db:
            db[b'a'] = b'b'
            assert db[b'a'] == b'b'

        with sdbm.open(p, flag='n') as db:
            with pytest.raises(KeyError):
                db[b'a']


def test_shelf() -> None:
    with tempfile.TemporaryDirectory() as d:
        with sdbm.open(Path(d) / 'test.db') as db:
            with shelve.Shelf(db) as s:
                s['a'] = ('b', )

        with sdbm.open(Path(d) / 'test.db') as db:
            with shelve.Shelf(db) as s:
                assert s['a'] == ('b', )
