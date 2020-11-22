import pandas as pd
import pysql
from pysql import gbq, table, print_dataset


DATASET = 'moon_phases'
pysql.set_dataset(DATASET)


# @gbq(how=['phase', 'peak_datetime'])
@gbq(how='test')
# merge could be an id field, an iterable of id fields or a sql condition
# add service, and add cron optional
# might add params to the function and get it via Flask request
# cron better be manually specified in cron.yaml along with app.yaml
# and I might replace cron and dispatch to BigQuery repo as "readonly"
# and set the logic in apps
def my_table():
    # df = table("moon_phases.moon_phases")
    # df["my_column"] = "my_column"
    data = [{"phase": "New Moon", "phase_emoji": "🌑", "peak_datetime": "2097-09-06 00:35:00 UTC", "my_column": "my_column_merge"}]
    return data
    # return df, table_schema
    # return df with set dtypes, write about it in the documentation
    # return jsn, table_schema
    # could return a list and the logic in gbq could be different
    # if list is returned without schema, try to infer it using gbq schema
    # might be useful for some combination of parameters and not for others


@gbq(how='insert', after="SELECT * FROM")
def reddit(date="2020-01-01"):
    # do some parsing
    print(date)
    data = [{'one_column': 1}]
    schema = [{"mode": "NULLABLE", "name": "one_column", "type": "INTEGER"}]
    return data, schema


if __name__ == "__main__":
    # table("SELECT * FROM")
    # print_dataset()
    # my_table()
    reddit()
