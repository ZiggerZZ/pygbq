import pygbq
import pytest

client = pygbq.Client()


# For testing purposes let's pretend that we have a project `pygbq-123456`, dataset `test_dataset` and
# table `test_table`. Its schema is [{"name": "test_string_column", "type": "STRING"}, {"name": "test_integer_column",
# "type": "INTEGER"}]. It contains [{"test_string_column": "string_value1", "test_integer_column": 1},
# {"test_string_column": "string_value2", "test_integer_column": 2}].
# pygbq-123456
#          test_dataset
#                    test_table{test_string_column, test_integer_column} = [("string_value1", 1), {"string_value2", 2}]


# TODO: fix this test because can't replace table if streaming buffer is not empty
# @client.gbq(table_id='moon_phases.test_insert', how='insert',
#           after='CREATE OR REPLACE moon_phases.test_insert as SELECT DISTINCT FROM moon_phases.test_insert')


def test_replace():
    data = [{'id': 1, 'string_column': 'test_replace_1'}, {'id': 2, 'string_column': 'test_replace_2'}]
    results = client.update_table_using_temp(data=data, table_id='test_dataset.test_table', how='replace')
    assert results == {"status": 200}


def test_insert():
    data = [{'id': 3, 'string_column': 'test_insert_3'}, {'id': 4, 'string_column': 'test_insert_4'}]
    results = client.update_table_using_temp(data=data, table_id='test_dataset.test_table', how='insert')
    assert results == {"status": 200}


def test_merge():
    data = [{'id': 1, 'string_column': 'test_merge_1'}, {'id': 2, 'string_column': 'test_merge_2'},
            {'id': 3, 'string_column': 'test_merge_3'}, {'id': 4, 'string_column': 'test_merge_4'}]
    results = client.update_table_using_temp(data=data, table_id='test_dataset.test_table', how=['id'])
    assert results == {"status": 200}


# def test_gbq():
#
#     @client.gbq(table='test_dataset.test_table', how=['id'])
#     def test_table():
#         data = [{'id': 5, 'string_column': 'test_gbq_5'}, {'id': 6, 'string_column': 'test_gbq_6'}]
#         return data
#
#     assert test_table() == {'status': 'success', 'data_len': 2}


def test_test():
    assert client.test(test='SELECT 1=1', arguments={}) is True
    assert client.test(test='SELECT 1=2', arguments={}) is False


# no need to check it outside of gbq
# def test_table_exists():
#     table_id = 'moon_phases.moon_phases'
#     assert pygbq.table_exists(table_id) is True
#
#
# def test_table_not_exists():
#     table_id = 'not_existing_dataset.not_existing_table'
#     assert pygbq.table_exists(table_id) is False


# def test_not_existing_table():
#     query = 'not_existing_table'
#     with pytest.raises(pygbq.MyError):  # f"Your query '{query}' is incorrect"
#         pygbq.table(query)


# def test_fail():
#     how = 'fail'
#     table_id = 'moon_phases.moon_phases'
#
#     with pytest.raises(pygbq.MyError):
#         @pygbq.gbq(table_id=table_id, how=how)
#         def table():
#             return []




def test_test():
    """
    * test correct, when test returns only one True bool value
    * not one row
    * not one column
    * not bool value
    * bool False value
    """
    pass


# def test_bad_return_type():
#     with pytest.raises(pygbq.MyError):  # f"Your query '{query}' is incorrect"
#         @client.gbq()
#         def data():
#             return 1, 2, 3
#
#
# def test_bad_callback():
#     """
#     * good after
#     * bad after
#     """
#     callback = 'SELECT * FROM'
#
#     with pytest.raises(pygbq.MyError):  # f"Your query '{query}' is incorrect"
#         @client.gbq(callback=callback)
#         def data():
#             return []

# def test_schema():
#     data = [{"one_column": 1}, {"one_column": 2}]
#     correct_schema = [{"mode": "NULLABLE", "name": "one_column", "type": "INTEGER"}]
#     assert pygbq.Schema.gen_schema_from_data(data) == correct_schema


def test_read_jsonl_bad():
    bad_filename = "test_bad_filename"
    with pytest.raises(pygbq.PyGBQError):
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
