from google.auth import default as default_creds
import pandas_gbq
from pandas import read_gbq, DataFrame
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import datetime
import pytz
import json
import string
import os
import random
from subprocess import check_output
from flask import Flask, request
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union
import logging
# from google.api_core.exceptions import BadRequest

app = Flask(__name__)

DATASET = None
SAVE_DIR = None


def set_dataset(new_dataset):
    global DATASET
    DATASET = new_dataset


def set_savedir(new_savedir):
    global SAVE_DIR
    SAVE_DIR = new_savedir


def print_dataset():
    print(DATASET)


# CREDENTIALS, PROJECT_ID = service_account.Credentials.from_service_account_file(
#     './pysql-294514-3e987336562c.json',
# ), "pysql-294514"

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""
CREDENTIALS, PROJECT_ID = default_creds(scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/bigquery",
    ])  # TODO: maybe add GOOGLE_APPLICATION_CREDENTIALS to configs


client = bigquery.Client(credentials=CREDENTIALS, project=PROJECT_ID)

pandas_gbq.context.credentials = CREDENTIALS
pandas_gbq.context.project = PROJECT_ID  # might put it to a config file


class MyError(Exception):
    """Basic exception for errors raised by pysql"""
    def __init__(self, msg="An error occurred in pysql"):
        super(MyError, self).__init__(msg)

# class CarCrashError(CarError):
#     """When you drive too fast"""
#     def __init__(self, car, other_car, speed):
#         super(CarCrashError, self).__init__(
#             car, msg="Car crashed into %s at speed %d" % (other_car, speed))
#         self.speed = speed
#         self.other_car = other_car


def table(table_name, **kwargs):
    """
    :arg table_name: "dataset_id.table_name" (no backticks) or an SQL query
    """
    # if 'query' in kwargs: kwargs['query']
    # maybe add a parameter rows not to return df
    if " " not in table_name:
        query = f"SELECT * FROM `{PROJECT_ID}.{table_name}`"
    else:
        query = table_name
    try:
        return read_gbq(query=query, **kwargs)
    except pandas_gbq.gbq.GenericGBQException as E:
        # logging.getLogger().error("Something bad happened", exc_info=True)
        raise MyError(f"Your query '{query}' is incorrect") from E


def parametrized(dec):
    """Helper decorator that allows the use of function and parameters in gbq decorator"""

    def layer(*args, **kwargs):
        def repl(function):
            return dec(function, *args, **kwargs)

        return repl

    return layer


def _id_generator(size=10, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def _gen_schema(schema_dict):
    schema = []
    for field in schema_dict:
        if field['type'] != 'RECORD':
            schema.append(bigquery.schema.SchemaField(field['name'],
                                                      field['type'],
                                                      mode=field['mode']))
        else:
            schema.append(bigquery.schema.SchemaField(field['name'],
                                                      field['type'],
                                                      mode=field['mode'],
                                                      fields=tuple(_gen_schema(field['fields']))))
    return schema


# TODO: dynamically set max_insert_num_rows
def update_table_using_temp(data, table_id, how, schema, after: str = None,
                            expiration=1, max_insert_num_rows=4000):
    """
    Creates a temp table, inserts rows there, then merges the table with the main table.
    Destroys the temp after expiration time.
    :param data: list of dicts
    :param table_name: name of the table
    :param dataset: dataset name, i.e. 'Ikentoo' or 'Stripe'
    :param schema: path to the schema of temp table or the schema itself
    :param query_template: query_template to merge temp to the main table. replace fields by table and table tmp names
    :arg after: query that needs to be executed in the end
    :param expiration: how many hours temporary tables live before expiration
    :param max_insert_num_rows: we will split data into batches of this size
    """
    dataset, table_name = table_id.split('.')  # TODO: check that works fine if project is specified

    # client = bigquery.Client()
    data_batches = [data[i * max_insert_num_rows:(i + 1) * max_insert_num_rows]
                    for i in range((len(data) + max_insert_num_rows - 1) // max_insert_num_rows)]

    if isinstance(schema, str):
        fp = open(schema, 'r')
        schema = json.load(fp)
        fp.close()

    query_template = prepare_query(how, schema) if isinstance(how, list) else None
    schema = _gen_schema(schema)

    for data_batch in data_batches:
        if how == 'insert':
            table = client.get_table(table_id)
            errors = client.insert_rows(table, data_batch)
            print("New rows have been added to", table.table_id) if not errors else print("ERROR", errors)
        elif how == 'replace':
            table = bigquery.Table(PROJECT_ID+'.'+table_id, schema=schema)
            table = client.create_table(table, exists_ok=True)  # exists_ok = True
            errors = client.insert_rows(table, data_batch)
            print("New rows have been added to", table.table_id) if not errors else print("ERROR", errors)
            how = 'insert'
        elif isinstance(how, list):
            tmp_id = _id_generator()
            table_tmp_id = PROJECT_ID + '.' + dataset + '.' + table_name + '_tmp_' + tmp_id

            table = bigquery.Table(table_tmp_id, schema=schema)
            table = client.create_table(table)
            print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

            # set table to expire expiration hours from now
            table.expires = datetime.datetime.now(pytz.utc) + datetime.timedelta(hours=expiration)
            table = client.update_table(table, ["expires"])  # API request

            print("Updated expiration date of table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

            print(f"Going to insert {len(data_batch)} rows to {table.table_id}")
            errors = client.insert_rows(table, data_batch)
            # TODO: add handle errors and show the row that causes the problems
            print("New rows have been added to", table.table_id) if not errors else print("ERROR", table_tmp_id, errors)

            # insert without duplicates
            query = query_template.format(table=table_name, table_tmp=table_name + '_tmp_' + tmp_id, DATASET=dataset)
            query_job = client.query(query)
            query_job.result()  # Waits for job to complete.
    return {
        "message": f"Status when pushing {table_name}",
    }


def prepare_query(how, schema=None):
    def _parse_schema():
        len_schema = len(schema)
        update_row = ""
        for j, f in enumerate(schema):
            update_row = update_row + f['name'] + ' = S.' + f['name']
            if j < len_schema - 1:
                update_row = update_row + ',\n'
        return update_row
    if how == 'fail':
        pass
    elif how == 'insert':
        q = """INSERT {DATASET}.{table} T
               SELECT * FROM {DATASET}.{table_tmp} S"""
        return q
    # should be directly a statement before query
    elif how == 'replace':
        pass
    # list of ids. one id should be put in the list, can't make it an option
    elif isinstance(how, list):  # possibly change to any iterable
        update_set = _parse_schema()
        condition = ""
        num_ids = len(how)
        for i, field in enumerate(how):
            # update_row = update_row + 'T.' + field['name'] + ' = S.' + field['name']
            condition = condition + f"T.{field} = S.{field}"
            if i < num_ids - 1:
                condition = condition + " AND "
        q = """MERGE {DATASET}.{table} T 
USING {DATASET}.{table_tmp} S
  ON """ + condition + \
            """
WHEN NOT MATCHED THEN
  INSERT ROW
WHEN MATCHED THEN
  UPDATE SET
""" + update_set
        return q
    # if it's a query / query template
    else:
        raise ValueError('Error in prepare_query')


class TableCreationError(ValueError):
    """
    Raised when the create table method fails
    """

    pass


def table_exists(table_id):
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False


def save_data(data: list, name: str = 'data.jsonl'):
    """Saves data in newline delimited json format so that BigQuery accepts this"""
    with open(name, 'w') as outfile:
        for d in data:
            if d:
                json.dump(d, outfile)
                outfile.write('\n')


def read_jsonl(name: str = 'data.jsonl'):
    data = []
    try:
        with open(name) as f:
            for line in f:
                data.append(json.loads(line))
    except Exception as E:
        raise MyError(f"Couldn't read data from {name}, check if it is a newline delimited json") from E
    return data


def generate_schema(data):
    data_string = ""
    for d in data:
        if d:
            data_string = data_string + json.dumps(d) + '\n'
    data_bytes = data_string.encode('utf-8')
    # TODO: test --keep_nulls, maybe need to put the flag in a different parameter
    s = check_output(['generate-schema --keep_nulls'], input=data_bytes)
    schema = json.loads(s)
    return schema


# TODO: Exceptions: https://julien.danjou.info/python-exceptions-guide/
# in my case all errors are just for logging because it's not supposed to catch my errors (only a decorator)
# as a straight solution might be the code with no try except since important is to understand what's wrong from logs
# BadFilenameError, TableNotFoundError, SchemaError, AfterError, FailError, NotSameSchemaError, CredentialsError
# TODO: rename module to pygbq or pygcp
# TODO: rename json to jsonl for newline delimeted json
# TODO: maybe add "test" that executes a query and evalutes a condition to True/False.
# use cases: no duplicates, count >=< than a certain number.
# so I can make it a query that returns [True/False]
# TODO: add access to external tables
# use case: to test that the query was successful (i.e. no duplicates) or to trigger view -> table
@parametrized
def gbq(function, table_id: str = None, how: str = None, save: Union[bool, str] = False, after: str = None, service: bool = False):
    """
    Prints the full table id.
    https://pandas-gbq.readthedocs.io/en/latest/reading.html
    :arg function: a function that returns a pandas DataFrame
    :arg save: save as newline delimeted json. use string to specify the folder where to save
    :arg how: None (return the data); test (sets expiration to 1h ?); fail, insert (==append), replace for df; fail, merge on id(s) or query, insert, replace for list
    :arg table_id: name of the destination table in the format "dataset.table_name". table_name defaults to function.__name__ and
    I might fix dataset in the beginning of the file
    :arg after: query that needs to be executed in the end
    :arg service: dict, creates an App Engine service and sets a cron job with the parameters specified in the dict.
    Service name defaults to "pysql" and endpoint is function name, could be rewritten.
    https://cloud.google.com/appengine/docs/admin-api/getting-started
    Bool parameter start_now (defaults to True) indicates whether to create a table now or wait until cron job does it
    :arg expiration: default None
    """

    # test correctness of table and dataset #
    if table_id:
        if '.' in table_id:
            dataset, table_name = table_id.split('.')
        else:
            dataset = DATASET
    else:
        dataset = DATASET
        if dataset:
            table_id = dataset + '.' + function.__name__
        else:
            raise MyError(f"Either provide full name of the table or set dataset with pysql.set_dataset()")
    try:
        client.get_dataset(dataset)
    except Exception as E:
        raise MyError(f"Dataset {dataset} does not exist") from E

    if how == 'test':
        table_id = table_id + '_test'
        how = 'replace'

    # test correctness of `how` #
    table_exists_bool = table_exists(table_id)
    if how == 'fail' and table_exists_bool:
        # TODO: this is a copy paste from pandas-gbq
        raise MyError(  #
            "Could not create the table because it "
            "already exists. "
            "Change the if_exists parameter to "
            "'insert' or 'replace' data."
        )
    if (isinstance(how, list) or how == 'insert') and not table_exists_bool:
        raise MyError(f"You are trying to merge with or insert in {table_id} which does not exist")

    def inner(**kwargs):
        def _save_file_name(extension: str = ''):
            """:arg extension: 'json' or 'csv'"""
            # TODO: maybe change only to string with None not save and '.' save to current directory (not obvious)
            # TODO: use the right library to work with folder names and handle / correctly
            if isinstance(save, str):
                file_name = './' + save + '/'
            else:
                if SAVE_DIR:
                    file_name = SAVE_DIR
                else:
                    file_name = ''
            # file_name = './' + save + '/' if isinstance(save, str) else ''
            file_name = file_name + table_name
            # TODO: drop chars not suitable for file name
            if kwargs:
                len_kwargs = len(kwargs)
                file_name = file_name + '?'
                for i, (arg, value) in enumerate(kwargs.items()):
                    file_name = file_name + arg + '=' + str(value)
                    if i < len_kwargs - 1:
                        file_name = file_name + '&'
            file_name = file_name + '.' + extension
            return file_name
        nonlocal how
        returned = function(**kwargs)
        # print(f"Going to update {PROJECT_ID}.{table_id}...")
        if isinstance(returned, tuple) and len(returned) == 2:
            data, schema = returned
        elif isinstance(returned, DataFrame) or isinstance(returned, list):
            data, schema = returned, None
        else:
            raise MyError('Return either list; df; (list, schema); (df, schema)')
        if isinstance(returned, DataFrame):
            try:
                if save:
                    name = _save_file_name('csv')
                    try:
                        data.to_csv(name, index=False)
                    except Exception:
                        print(f"Couldn't save file {name}")
                if how == 'insert':
                    how = 'append'
                data.to_gbq(table_id, project_id=PROJECT_ID, if_exists=how, table_schema=schema)
                print(f"Created")
            except Exception as E:
                raise MyError('Something bad happend') from E
        elif isinstance(data, list):
            if save:
                name = _save_file_name('json')
                try:
                    save_data(data=data, name=name)
                except Exception:
                    print(f"Couldn't save file {name}")
            if how:
                if not schema:
                    if isinstance(how, list) or how == 'insert':
                        try:
                            schema = [field.to_api_repr() for field in client.get_table(table_id).schema]
                            # or just schema as it's the right format
                        except Exception as E:
                            raise E
                    elif how in {'fail', 'replace', 'test'}:
                        try:
                            how = 'replace'
                            schema = generate_schema(data)
                        except Exception:
                            raise Exception('Generating schema failed, provide schema')
                update_table_using_temp(data, table_id, how, schema, after)
            if after:
                query_job = client.query(after)
                try:
                    query_job.result()
                except Exception as E:
                    raise MyError(f"Your query '{after}' is incorrect") from E
                # should add to result if fails
        else:
            print("Error")
        # raise Exceptions or set status=False and message=Exception ?
        return {'status': "fail/success", "data_len": len(data), **kwargs}

    if service:
        print("/" + table_id.split(".")[1])

        @app.route("/" + table_id.split(".")[1])
        # @hug.get("/" + table_id.split(".")[1])
        def service_inner():
            inner()
            return {"status": 200}

        return service_inner
    else:
        return inner
