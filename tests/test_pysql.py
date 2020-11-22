import pysql
import pytest


# For testing purposes let's pretend that we have a project `pysql-123456`, dataset `test_dataset` and
# table `test_table`. Its schema is [{"name": "test_string_column", "type": "STRING"}, {"name": "test_integer_column",
# "type": "INTEGER"}]. It contains [{"test_string_column": "string_value1", "test_integer_column": 1},
# {"test_string_column": "string_value2", "test_integer_column": 2}].
# pysql-123456
#          test_dataset
#                    test_table{test_string_column, test_integer_column} = [("string_value1", 1), {"string_value2", 2}]


# TODO: split tests by parameters for pysql.gbq and by functions

def test_auth():
    assert 1 == 1


def test_table_exists():
    table_id = 'moon_phases.moon_phases'
    assert pysql.table_exists(table_id) is True


def test_table_not_exists():
    table_id = 'not_existing_dataset.not_existing_table'
    assert pysql.table_exists(table_id) is False


def test_not_existing_table():
    query = 'not_existing_table'
    with pytest.raises(pysql.MyError):  # f"Your query '{query}' is incorrect"
        pysql.table(query)


def test_fail():
    how = 'fail'
    table_id = 'moon_phases.moon_phases'

    with pytest.raises(pysql.MyError):
        @pysql.gbq(table_id=table_id, how=how)
        def table():
            return []


def test_bad_return_type():
    with pytest.raises(pysql.MyError):  # f"Your query '{query}' is incorrect"
        @pysql.gbq()
        def data():
            return 1, 2, 3


def test_bad_after():
    after = 'SELECT * FROM'

    with pytest.raises(pysql.MyError):  # f"Your query '{query}' is incorrect"
        @pysql.gbq(after=after)
        def data():
            return []


def test_read_jsonl():
    bad_filename = "test_bad_filename"
    with pytest.raises(pysql.MyError):  # f"Your query '{query}' is incorrect"
        pysql.read_jsonl(name=bad_filename)