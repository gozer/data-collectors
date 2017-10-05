from io import StringIO
from unittest import mock

import pytest

from collectors import adjust

test_data = """
adj_date,os,daus,waus,maus,installs
2017-01-01,android,10,20,30,1
2017-01-02,android,20,40,60,2
"""


@pytest.fixture
def odbc():
    return mock.Mock()


def test_colllect():
    input = StringIO(test_data)
    df = adjust.collect_app('foo', input)
    assert df.ndim is 2
    print("Loaded successfully")


def test_foo():
    url = adjust.build_dau_url("abc", "123")
    assert url is not None
    assert url.__contains__("https://api.adjust.com")


def test_load(odbc):
    adjust.load(odbc, "foo", "output.foo", "rejects", "exceptions")
    assert odbc.execute.call_count is 2 #Called for both truncate and copy


if __name__ == '__main__':
    test_colllect()
    test_foo()
