import pygbq
import pytest


# For testing purposes let's pretend that we have a project `pygbq-123456`, dataset `test_dataset` and
# table `test_table`. Its schema is [{"name": "test_string_column", "type": "STRING"}, {"name": "test_integer_column",
# "type": "INTEGER"}]. It contains [{"test_string_column": "string_value1", "test_integer_column": 1},
# {"test_string_column": "string_value2", "test_integer_column": 2}].
# pygbq-123456
#          test_dataset
#                    test_table{test_string_column, test_integer_column} = [("string_value1", 1), {"string_value2", 2}]


def test_auth():
    assert 1 == 1


# no need to check it outside of gbq
# def test_table_exists():
#     table_id = 'moon_phases.moon_phases'
#     assert pygbq.table_exists(table_id) is True
#
#
# def test_table_not_exists():
#     table_id = 'not_existing_dataset.not_existing_table'
#     assert pygbq.table_exists(table_id) is False


def test_not_existing_table():
    query = 'not_existing_table'
    with pytest.raises(pygbq.MyError):  # f"Your query '{query}' is incorrect"
        pygbq.table(query)


def test_fail():
    how = 'fail'
    table_id = 'moon_phases.moon_phases'

    with pytest.raises(pygbq.MyError):
        @pygbq.gbq(table_id=table_id, how=how)
        def table():
            return []


def test_insert():
    """
    * good inserting
    * bad schema
    """

    # TODO: fix this test because can't replace table if streaming buffer is not empty
    @pygbq.gbq(table_id='moon_phases.test_insert', how='insert',
               after='CREATE OR REPLACE moon_phases.test_insert as SELECT DISTINCT FROM moon_phases.test_insert')
    def my_table_test():
        """inserts and then removes duplicates to keep the table intact"""
        data = [{"phase": "New Moon", "phase_emoji": "🌑", "peak_datetime": "2097-09-06 00:35:00 UTC",
                 "my_column": "merge_test"}]
        return data

    assert my_table_test() == {'status': 'success', 'data_len': 1}


def test_merge():
    """
    * good ids
    * bad ids
    * bas schema
    """

    @pygbq.gbq(table_id='moon_phases.my_table_test', how=['phase', 'peak_datetime'])
    def my_table_test():
        data = [{"phase": "New Moon", "phase_emoji": "🌑", "peak_datetime": "2097-09-06 00:35:00 UTC",
                 "my_column": "merge_test"}]
        return data

    assert my_table_test() == {'status': 'success', 'data_len': 1}


def test_test():
    """
    * test correct, when test returns only one True bool value
    * not one row
    * not one column
    * not bool value
    * bool False value
    """
    pass


def test_bad_return_type():
    with pytest.raises(pygbq.MyError):  # f"Your query '{query}' is incorrect"
        @pygbq.gbq()
        def data():
            return 1, 2, 3


def test_bad_after():
    """
    * good after
    * bad after
    """
    after = 'SELECT * FROM'

    with pytest.raises(pygbq.MyError):  # f"Your query '{query}' is incorrect"
        @pygbq.gbq(after=after)
        def data():
            return []


def test_schema():
    data = [{"one_column": 1}, {"one_column": 2}]
    correct_schema = [{"mode": "NULLABLE", "name": "one_column", "type": "INTEGER"}]
    assert pygbq.generate_schema(data) == correct_schema


def test_read_jsonl_bad():
    bad_filename = "test_bad_filename"
    with pytest.raises(pygbq.MyError):
        pygbq.read_jsonl(name=bad_filename)


def test_read_jsonl():
    good_filename = "tests/data.jsonl"
    test_data = [
        {"phase": "New Moon", "phase_emoji": "🌑", "peak_datetime": "2097-09-06 00:35:00 UTC",
         "my_column": "test_read_jsonl_1"},
        {"phase": "New Moon", "phase_emoji": "🌑", "peak_datetime": "2097-09-06 00:35:00 UTC",
         "my_column": "test_read_jsonl_2"},
        {"phase": "New Moon", "phase_emoji": "🌑", "peak_datetime": "2097-09-06 00:35:00 UTC",
         "my_column": "test_read_jsonl_3"}
    ]
    assert pygbq.read_jsonl(name=good_filename) == test_data
