from io import StringIO
from unittest import mock

import pytest

from collectors.redash import collect

test_data = """
foo,bar,baz
1,2,3
3,4,5
"""


@pytest.fixture
def odbc():
    return mock.Mock()


def test_collect(tmpdir):
    input = StringIO(test_data)
    path = tmpdir.mkdir("redash")

    output = collect(input, path)
    assert output == path.join('output.csv')
