import pandas_gbq
from pandas import read_gbq, DataFrame
from google.auth import default as default_creds, load_credentials_from_file
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import google.cloud.logging
from google.cloud import secretmanager_v1 as secretmanager
import logging
import datetime
import pytz
import json
import string
from pathlib import Path
import random
from subprocess import check_output

from typing import List, Union  # Optional, Any, Dict,

logging_client = google.cloud.logging.Client()
logging_client.get_default_handler()
logging_client.setup_logging()

SERVICE_KEY_PATH = None
DATASET = None


def set_dataset(new_dataset):
    global DATASET
    DATASET = new_dataset


def set_service_key_path(service_key_path):
    global SERVICE_KEY_PATH
    SERVICE_KEY_PATH = service_key_path


if SERVICE_KEY_PATH:
    CREDENTIALS, PROJECT_ID = load_credentials_from_file(
        SERVICE_KEY_PATH,
        scopes=[
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ],
    )
else:
    CREDENTIALS, PROJECT_ID = default_creds(
        scopes=[
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ]
    )

client = bigquery.Client(credentials=CREDENTIALS, project=PROJECT_ID)

pandas_gbq.context.credentials = CREDENTIALS
pandas_gbq.context.project = PROJECT_ID


class MyError(Exception):
    """Basic exception for errors raised by pygbq"""

    def __init__(self, msg="An error occurred in pygbq"):
        super(MyError, self).__init__(msg)


class MyNameError(MyError):
    """When test fails"""

    def __init__(self, msg="Bad table/dataset name"):
        super(MyNameError, self).__init__(msg=msg)


class MyTestError(MyError):
    """When test fails"""

    def __init__(self, msg="Bad test query"):
        super(MyTestError, self).__init__(msg=msg)


class MyDataError(MyError):
    """When test fails"""

    def __init__(self, row, error_info):
        super(MyDataError, self).__init__(
            msg=f"Row {row} caused the following errors:\n{error_info}"
        )


class TableCreationError(ValueError):
    """
    Raised when the create table method fails
    """

    pass


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


# TODO: dynamically set max_insert_num_rows
def update_table_using_temp(
    data, table_id, how, schema, expiration=1, max_insert_num_rows=4000
):
    """
    Creates a temp table, inserts rows there, then merges the table with the main table.
    Destroys the temp table after expiration time.
    :param data: list of dicts
    :param table_id: table_id
    :param how: table_id
    :param schema: path to the schema of temp table or the schema itself
    :param expiration: how many hours temporary tables live before expiration
    :param max_insert_num_rows: we will split data into batches of this size
    """

    def _id_generator(size=10, chars=string.ascii_uppercase + string.digits):
        return "".join(random.choice(chars) for _ in range(size))

    def _gen_schema(schema_dict):
        schema_list = []
        for field in schema_dict:
            if field["type"] != "RECORD":
                schema_list.append(
                    bigquery.schema.SchemaField(
                        field["name"], field["type"], mode=field["mode"]
                    )
                )
            else:
                schema_list.append(
                    bigquery.schema.SchemaField(
                        field["name"],
                        field["type"],
                        mode=field["mode"],
                        fields=tuple(_gen_schema(field["fields"])),
                    )
                )
        return schema_list

    def prepare_query():
        """Prepares query template"""

        def _parse_schema():
            len_schema = len(schema)
            update_row = ""
            for j, f in enumerate(schema):
                update_row = update_row + f["name"] + " = S." + f["name"]
                if j < len_schema - 1:
                    update_row = update_row + ",\n"
            return update_row

        # list of ids. one id should be put in the list, can't make it optional
        update_set = _parse_schema()
        condition = ""
        num_ids = len(how)
        for i, field in enumerate(how):
            condition = condition + f"T.{field} = S.{field}"
            if i < num_ids - 1:
                condition = condition + " AND "
        q = (
            """MERGE `{table_id}` T 
    USING `{table_tmp_id}` S
      ON """
            + condition
            + """
    WHEN NOT MATCHED THEN
      INSERT ROW
    WHEN MATCHED THEN
      UPDATE SET
    """
            + update_set
        )
        return q

    def handle_errors():
        """Print the first error and the row that caused the error"""
        if not errors:
            print("New rows have been added to", table.table_id)
        else:
            # print("ERROR", errors)
            index, error_info = errors[0]["index"], errors[0]["errors"]
            raise MyDataError(row=data_batch[index], error_info=error_info)

    data_batches = [
        data[i * max_insert_num_rows : (i + 1) * max_insert_num_rows]
        for i in range((len(data) + max_insert_num_rows - 1) // max_insert_num_rows)
    ]

    if isinstance(schema, str):
        fp = open(schema, "r")
        schema = json.load(fp)
        fp.close()

    query_template = prepare_query() if isinstance(how, list) else None
    schema_api = _gen_schema(schema)

    table = None
    for data_batch in data_batches:
        if how == "insert":
            # if replace then we already have table
            if not table:
                table = client.get_table(table_id)
            errors = client.insert_rows(table, data_batch)
            handle_errors()
        elif how == "replace":
            table = bigquery.Table(table_id, schema=schema_api)
            table = client.create_table(table, exists_ok=True)  # exists_ok = True
            errors = client.insert_rows(table, data_batch)
            handle_errors()
            how = "insert"
        elif isinstance(how, list):
            tmp_id = _id_generator()
            table_tmp_id = f"{table_id}_tmp_{tmp_id}"
            # table_tmp_id = PROJECT_ID + '.' + dataset + '.' + table_name + '_tmp_' + tmp_id

            table = bigquery.Table(table_tmp_id, schema=schema_api)
            table = client.create_table(table)
            print(f"Created table {table_tmp_id}")

            # set table to expire expiration hours from now
            table.expires = datetime.datetime.now(pytz.utc) + datetime.timedelta(
                hours=expiration
            )
            table = client.update_table(table, ["expires"])  # API request

            print(f"Updated expiration date of table {table_tmp_id}")

            print(f"Going to insert {len(data_batch)} rows to {table.table_id}")
            errors = client.insert_rows(table, data_batch)
            handle_errors()

            # insert without duplicates
            query = query_template.format(table_id=table_id, table_tmp_id=table_tmp_id)
            query_job = client.query(query)
            try:
                query_job.result()  # Waits for job to complete.
            except google.api_core.exceptions.BadRequest as E:
                raise MyError("Error with MERGE") from E
    return {"status": 200}


def save_data(data: list, name: str = "data.jsonl"):
    """Saves data in newline delimited json format so that BigQuery accepts this"""
    with open(name, "w") as outfile:
        for d in data:
            if d:
                json.dump(d, outfile)
                outfile.write("\n")


def read_jsonl(name: str = "data.jsonl"):
    data = []
    try:
        with open(name) as f:
            for line in f:
                data.append(json.loads(line))
    except Exception as E:
        raise MyError(
            f"Couldn't read data from {name}, check if it is a newline delimited json"
        ) from E
    return data


def generate_schema(data):
    data_string = ""
    for d in data:
        if d:
            data_string = data_string + json.dumps(d) + "\n"
    data_bytes = data_string.encode("utf-8")
    s = check_output(["generate-schema", "--keep_nulls"], input=data_bytes)
    schema = json.loads(s)
    return schema


# Exceptions: https://julien.danjou.info/python-exceptions-guide/
# in my case all errors are just for logging because it's not supposed to catch my errors (only a decorator)
# as a straight solution might be the code with no try except since important is to understand what's wrong from logs
# BadFilenameError, TableNotFoundError, SchemaError, AfterError, FailError, NotSameSchemaError, CredentialsError
# since I have many logical parts of the code (how, after, test) and it's important to know which has finished correctly
# and which hasn't,
# the program must return something like {'how_status': 200, 'test_status': 400, 'after_status': 'not run'}
# and maybe debug info for each how, test and after
# look at right statuses
# if bad table_id, raise Error
# if bad how, raise Error
# if good how, bad test, skip after and return {'how_status': 200, 'test_status': 400, 'after_status': 'not run'}
# if good how, good test and bad after return {'how_status': 200, 'test_status': 200, 'after_status': '400'}
# if good how, good test and good after return {'how_status': 200, 'test_status': 200, 'after_status': '200'}

# TODO: move schema from inside function to the decorator, or not, because sometimes schema is dynamical within function
# or make a tuple (sql, result), where sql must return a df == result, and raise error TestFailError
# possible cases for test:
# * check there is no duplicates in the final table:
# * check that there are 4 rows for a certain date:
# SELECT (SELECT COUNT(*) FROM `pygbq-294514.moon_phases.my_table_test` WHERE date = {date}) = 4
# SELECT (SELECT COUNT(*) FROM `pygbq-294514.moon_phases.my_table_test`) =
# (SELECT COUNT(DISTINCT my_column) FROM `pygbq-294514.moon_phases.my_table_test`)
# where "date" must be an argument from the function
@parametrized
def gbq(
    function,
    table_id: str = None,
    how: Union[str, List[str]] = None,
    save_dir: str = None,
    after: str = None,
    test: str = None,
):
    """
    https://pandas-gbq.readthedocs.io/en/latest/reading.html
    :arg function: a function that returns a pandas DataFrame
    :arg save_dir: save as newline delimited json. use string to specify the folder where to save
    :arg how: None (return the data);
    test (sets expiration to 1h?); fail, insert (==append), replace, create (==replace) for df;
    fail, merge on id(s) or query, insert, replace, create (==replace) for list
    TODO: rename fail to create, maybe keep create, replace, create or replace
    :arg table_id: name of the destination table in the format "dataset.table_name".
    table_name defaults to function.__name__
    :arg after: query that needs to be executed in the end. accepts format with function arguments
    :arg test: query that tests something are return True or False
    :arg service: dict, creates an App Engine service and sets a cron job with the parameters specified in the dict.
    Service name defaults to "pygbq" and endpoint is function name, could be rewritten.
    https://cloud.google.com/appengine/docs/admin-api/getting-started
    Bool parameter start_now (defaults to True) indicates whether to create a table now or wait until cron job does it
    """

    # Test correctness of table_id and set dataset and table_name
    if table_id:
        if not table_id.replace("_", "").replace(".", "").isalnum():
            raise MyNameError(
                "Bad table name: only letters, numbers and underscores allowed"
            )
        num_dots = table_id.count(".")
        if num_dots == 0:
            dataset, table_name = DATASET, table_id
        elif num_dots == 1:
            dataset, table_name = table_id.split(".")
        elif num_dots == 2:
            _, dataset, table_name = table_id.split(
                "."
            )  # don't need to store project as we use the default
        else:
            raise MyNameError("Bad table name")
    else:
        dataset = DATASET
        if dataset:
            table_name = function.__name__
        else:
            raise MyNameError(
                f"Either provide full name of the table or set dataset with pygbq.set_dataset()"
            )
    table_id = f"{PROJECT_ID}.{dataset}.{table_name}"
    try:
        # TODO: as this part of code (before def inner) is called at **start time** of Python code,
        # it might be expensive to check it for each function
        # think how to maybe move it in inner
        client.get_dataset(dataset)
    except Exception as E:
        raise MyNameError(f"Dataset {dataset} does not exist") from E

    if how == "test":
        table_id = table_id + "_test"
        how = "replace"
    if how == "create":
        how = "replace"

    try:
        client.get_table(table_id)
        table_exists = True
    except NotFound:
        table_exists = False
    if how == "fail" and table_exists:
        raise MyError(
            "Could not create the table because it already exists. "
            "Change the how parameter to 'insert' or 'replace' data."
        )

    if how and (isinstance(how, list) or how == "insert") and not table_exists:
        raise MyError(
            f"You are trying to merge with or insert in {table_id} which does not exist"
        )

    def inner(**kwargs):
        def _save_file_name(extension: str = ""):
            """:arg extension: 'jsonl' or 'csv'"""
            # file_name = './' + save + '/' if isinstance(save, str) else ''
            file_name = table_name
            # TODO: drop chars not suitable for file name
            if kwargs:
                len_kwargs = len(kwargs)
                file_name = file_name + "?"
                for i, (arg, value) in enumerate(kwargs.items()):
                    file_name = file_name + arg + "=" + str(value)
                    if i < len_kwargs - 1:
                        file_name = file_name + "&"
            file_name = f"{file_name}.{extension}"
            return Path(".") / save_dir / file_name

        nonlocal how
        returned = function(**kwargs)
        # print(f"Going to update {PROJECT_ID}.{table_id}...")
        if isinstance(returned, tuple) and len(returned) == 2:
            data, schema = returned
        elif isinstance(returned, DataFrame) or isinstance(returned, list):
            data, schema = returned, None
        else:
            raise MyError("Return either list; df; (list, schema); (df, schema)")
        if isinstance(returned, DataFrame):
            try:
                if save_dir:
                    name = _save_file_name("csv")
                    try:
                        data.to_csv(name, index=False)
                    except Exception:
                        print(f"Couldn't save file {name}")
                if how == "insert":
                    how = "append"
                data.to_gbq(
                    table_id, project_id=PROJECT_ID, if_exists=how, table_schema=schema
                )
                print(f"Created")
            except Exception as E:
                raise MyError("Something bad happend") from E
        elif isinstance(data, list):
            if save_dir:
                name = _save_file_name("jsonl")
                try:
                    save_data(data=data, name=name)
                except Exception:
                    print(f"Couldn't save file {name}")
            if how:
                if not schema:
                    if isinstance(how, list) or how == "insert":
                        try:
                            schema = [
                                field.to_api_repr()
                                for field in client.get_table(table_id).schema
                            ]
                            # or just schema as it's the right format
                        except Exception as E:
                            raise E
                    elif how in {"fail", "replace", "test"}:
                        try:
                            how = "replace"
                            schema = generate_schema(data)
                        except Exception:
                            raise Exception("Generating schema failed, provide schema")
                update_table_using_temp(data, table_id, how, schema)
            if test:
                try:
                    result = client.query(test.format(**kwargs)).result()
                except Exception as E:
                    logging.exception(f"Bad test query: {test}")
                    return {"status": "fail", "data_len": len(data), **kwargs}
                    # raise MyError(f"Bad test query: {test}") from E
                if result.total_rows != 1:
                    logging.exception(
                        f"'{test}' must return exactly one boolean value, but returned {result.total_rows} rows"
                    )
                    # raise MyTestError(
                    #     msg=f"'{test}' must return exactly one boolean value, but returned {result.total_rows} rows")
                else:
                    row = [row for row in result][0]
                    if len(row) != 1:
                        logging.exception(
                            f"'{test}' must return exactly one boolean value, but returned {len(row)} columns"
                        )
                        # raise MyTestError(
                        #     msg=f"'{test}' must return exactly one boolean value, but returned {len(row)} columns")
                    value = row[0]
                    if isinstance(value, bool):
                        # probably write to warning but still don't raise an error
                        # since the previous part of the function finished correctly
                        if not value:
                            logging.exception(f"'{test}' did not pass")
                            # raise MyTestError(msg=f"'{test}' did not pass")
                        else:
                            test_status = value
                    else:
                        logging.exception(
                            f"'{test}' must return exactly one boolean value, but returned type {type(value)}"
                        )
                        # raise MyTestError(
                        #     msg=f"'{test}' must return exactly one boolean value, but returned type {type(value)}")
            if after:
                query_job = client.query(after.format(**kwargs))
                try:
                    query_job.result()
                    after_status = True
                except Exception as E:
                    after_status = False
                    logging.exception(
                        f"Your query '{after}' is incorrect"
                    )  # this way we get E but not MyError
                    # well, maybe this is not too bad since I send the info I want to send
                    # raise MyError(f"Your query '{after}' is incorrect") from E
                # should add to result if fails
        else:  # shouldn't happen because I check return type before
            raise MyError("Return either list; df; (list, schema); (df, schema)")
        return {"status": "success", "data_len": len(data), **kwargs}

    return inner


def get_secret(secret_id, version="latest"):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """
    secretmanager_client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version}"
    response = secretmanager_client.access_secret_version(name=secret_path)
    return response.payload.data.decode("UTF-8")


def add_secret(secret_id, data):
    """
    Add a new secret version to the given secret with the provided data.
    """
    secretmanager_client = secretmanager.SecretManagerServiceClient()
    parent = f"projects/{PROJECT_ID}/secrets/{secret_id}"
    payload = secretmanager.types.SecretPayload(data=data.encode("UTF-8"))
    response = secretmanager_client.add_secret_version(parent=parent, payload=payload)
    print("Added secret version: {}".format(response.name))
