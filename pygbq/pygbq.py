import pandas_gbq
from pandas import DataFrame  # read_gbq
from google.auth import default as default_creds, load_credentials_from_file
from google.cloud import bigquery
# from google.cloud.exceptions import NotFound
import google.cloud.logging
from google.cloud import secretmanager_v1 as secretmanager
import logging
import datetime
import pytz
import json
import string
from pathlib import Path
import random
from bigquery_schema_generator.generate_schema import SchemaGenerator
import time

from typing import List, Union, Any, Callable, OrderedDict  # Optional, Dict

logging_client = google.cloud.logging.Client()
logging_client.get_default_handler()
logging_client.setup_logging()


class PyGBQError(Exception):
    """Basic exception for errors raised by pygbq"""

    def __init__(self, msg="An error occurred in pygbq"):
        super(PyGBQError, self).__init__(msg)


class PyGBQNameError(PyGBQError):
    """When test fails"""

    def __init__(self, msg="Bad table/dataset name"):
        super(PyGBQNameError, self).__init__(msg=msg)


class PyGBQDataError(PyGBQError):
    """When test fails"""

    def __init__(self, row, error_info):
        super(PyGBQDataError, self).__init__(
            msg=f"Error {error_info} has been cause by the row:\n{row}"
        )


# def table(table_name, **kwargs):
#     """
#     :arg table_name: "dataset_id.table_name" (no backticks) or an SQL query
#     """
#     # if 'query' in kwargs: kwargs['query']
#     # maybe add a parameter rows not to return df
#     if " " not in table_name:
#         query = f"SELECT * FROM `{PROJECT_ID}.{table_name}`"
#     else:
#         query = table_name
#     try:
#         return read_gbq(query=query, **kwargs)
#     except pandas_gbq.gbq.GenericGBQException as E:
#         # logging.getLogger().error("Something bad happened", exc_info=True)
#         raise PyGBQError(f"Your query '{query}' is incorrect") from E


def parametrized(dec):
    """Helper decorator that allows the use of function and parameters in gbq decorator"""

    def layer(*args, **kwargs):
        def repl(function):
            return dec(function, *args, **kwargs)

        return repl

    return layer


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
        raise PyGBQError(
            f"Couldn't read data from {name}, check if it is a newline delimited json"
        ) from E
    return data


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

# or make a tuple (sql, result), where sql must return a df == result, and raise error TestFailError
# possible cases for test:
# * check there is no duplicates in the final table:
# * check that there are 4 rows for a certain date:
# SELECT (SELECT COUNT(*) FROM `pygbq-294514.moon_phases.my_table_test` WHERE date = {date}) = 4
# SELECT (SELECT COUNT(*) FROM `pygbq-294514.moon_phases.my_table_test`) =
# (SELECT COUNT(DISTINCT my_column) FROM `pygbq-294514.moon_phases.my_table_test`)
# where "date" must be an argument from the function

def _id_generator(size=10, chars=string.ascii_uppercase + string.digits):
    return "".join(random.choice(chars) for _ in range(size))


class Schema:
    def __init__(self, data=None, schema_list: List[Union[dict, OrderedDict]] = None, schema_api=None, schema_path=None):
        """probably should be set only one of arguments"""
        if data:
            self.schema_list = self.gen_schema_from_data(data)
            self.schema_api = self.gen_schema_api(self.schema_list)
        elif schema_list:
            self.schema_list = schema_list
            self.schema_api = self.gen_schema_api(self.schema_list)
        elif schema_api:
            self.schema_api = schema_api
            self.schema_list = self.gen_schema_list(self.schema_api)
        elif schema_path:
            with open(schema_path, "r") as fp:
                self.schema_list = json.load(fp)
            self.schema_api = self.gen_schema_api(self.schema_list)
        else:
            self.schema_list = []
            self.schema_api = []

    @staticmethod
    def gen_schema_list(schema_api):
        return [field.to_api_repr() for field in schema_api]

    @staticmethod
    def gen_schema_api(schema_list):
        schema_api = []
        for field in schema_list:
            if field["type"] != "RECORD":
                schema_api.append(
                    bigquery.schema.SchemaField(
                        field["name"], field["type"], mode=field["mode"]
                    )
                )
            else:
                schema_api.append(
                    bigquery.schema.SchemaField(
                        field["name"],
                        field["type"],
                        mode=field["mode"],
                        fields=tuple(Schema.gen_schema_api(field["fields"])),
                    )
                )
        return schema_api

    @staticmethod
    def gen_schema_from_data(data):
        generator = SchemaGenerator('dict', keep_nulls=True)
        schema_map, error_logs = generator.deduce_schema(data)
        if error_logs:
            raise PyGBQError('Could not generate schema, please provide a schema')
        schema = generator.flatten_schema(schema_map)
        return schema


class Update:
    """Initialize client.
    Usage:
        Used as a helper class to store schema, how,  in update_table_using_temp
    """

    def __init__(self, client, how=None, table_id=None, schema=Schema(), expiration=None):
        self.client = client
        self.how = how
        self.table_id = table_id
        self.schema = schema
        if isinstance(how, list):
            self.query_template = self.prepare_query()
        self.expiration = expiration
        self.errors = []

    def merge(self, data_batch):
        # TODO: read https://cloud.google.com/bigquery/streaming-data-into-bigquery#template-tables
        tmp_id = _id_generator()
        table_tmp_id = f"{self.table_id}_tmp_{tmp_id}"

        table = self.client.create_table(table=bigquery.Table(table_tmp_id, schema=self.schema.schema_api))
        print(f"Created table {table_tmp_id}")

        # set table to expire expiration hours from now
        table.expires = datetime.datetime.now(pytz.utc) + datetime.timedelta(hours=self.expiration)
        table = self.client.update_table(table, ["expires"])  # API request

        print(f"Updated expiration date of table {table_tmp_id}")

        print(f"Going to insert {len(data_batch)} rows to {table.table_id}")
        self.errors = self.client.insert_rows(table, data_batch)
        self.handle_errors(data_batch)

        # insert without duplicates
        query = self.query_template.format(table_id=self.table_id, table_tmp_id=table_tmp_id)
        query_job = self.client.query(query)
        try:
            query_job.result()  # Waits for job to complete.
        except google.api_core.exceptions.BadRequest as E:
            # catch streaming buffer error because it will be fixed in at max 90 minutes
            if 'affect rows in the streaming buffer, which is not supported' in repr(E):
                logging.exception(E)
            elif 'UPDATE/MERGE must match at most one source row for each target row' in repr(E):
                backtick_fields = ', '.join(map(lambda s: f"`{s}`", self.how))
                query = f"""SELECT T.* FROM `{self.table_id}` T
JOIN (SELECT {backtick_fields}, COUNT(*)
    FROM `{self.table_id}`
    GROUP BY {backtick_fields}
    HAVING COUNT(*) > 1) USING ({backtick_fields})"""
                query_job = self.client.query(query)
                print(query_job.result().to_dataframe().to_dict(orient='records')[:3])  # top three rows
            else:
                raise E

    def replace(self, data_batch):
        try:
            self.client.get_table(self.table_id)
            table_exists = True
        except google.cloud.exceptions.NotFound:
            table_exists = False
        if table_exists:
            self.client.delete_table(table=self.table_id)  # doesn't work without delete
        table = bigquery.Table(self.table_id, schema=self.schema.schema_api)
        table = self.client.create_table(table=table, exists_ok=True)
        self.errors = self.client.insert_rows(table, data_batch)
        self.handle_errors(data_batch)

    def insert(self, data_batch):
        table = self.client.get_table(self.table_id)
        self.errors = self.client.insert_rows(table, data_batch)
        self.handle_errors(data_batch)

    def prepare_query(self):
        """
        Prepares query template.
        Example:
            MERGE `{table_id}` T
            USING `{table_tmp_id}` S
                ON T.phase = S.phase AND T.phase_emoji = S.phase_emoji
            WHEN NOT MATCHED THEN
              INSERT ROW
            WHEN MATCHED THEN
              UPDATE SET
            `phase` = S.phase,
            `phase_emoji` = S.phase_emoji,
            `peak_datetime` = S.peak_datetime
        """
        update_set = ',\n'.join(f'`{field["name"]}` = S.{field["name"]}' for field in self.schema.schema_list)
        condition = ' AND '.join(f'T.{field} = S.{field}' for field in self.how)
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

    def handle_errors(self, data_batch):
        """Print the first error and the row that caused the error"""
        if self.errors:
            index, error_info = self.errors[0]["index"], self.errors[0]["errors"]
            raise PyGBQDataError(row=data_batch[index], error_info=error_info)


# TODO: might add support of many scopes, i.e. storage
class Client:
    """Initialize client.
    Usage:
        client = Client(default_dataset, path_to_key)
        data = [{...}, {...}]
        client.update_table(table_name, how)
    """

    def __init__(self, default_dataset: str = None, path_to_key: str = None):
        self.default_dataset = default_dataset

        credentials, self.project_id = load_credentials_from_file(
            path_to_key,
            scopes=[
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery",
            ],
        ) if path_to_key else default_creds(
            scopes=[
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery",
            ]
        )

        pandas_gbq.context.credentials = credentials
        pandas_gbq.context.project = self.project_id

        self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        self.secretmanager_client = None

    # TODO: dynamically set max_insert_num_rows
    # TODO: move all table_id, how, schema logic here to be able to call this function separately
    def update_table_using_temp(
            self, data, table_id, how, schema: Union[str, List[dict]] = None,
            expiration=1, max_insert_num_rows=4000
    ):
        """
        Creates a temp table, inserts rows there, then merges the table with the main table.
        Destroys the temp table after expiration time.
        :param data: list of dicts
        :param table_id: either table_name if default dataset is set else table_id (possibly with project_id)
        :param how: table_id
        :param schema: path to the schema of temp table or the schema itself or None (then generate schema)
        :param expiration: how many hours temporary tables live before expiration
        :param max_insert_num_rows: we will split data into batches of this size
        """

        def set_schema():
            nonlocal schema, how
            if schema is None:
                if isinstance(how, list) or how == "insert":
                    return Schema(schema_api=self.client.get_table(table_id).schema)
                elif how in {"fail", "replace"}:
                    how = "replace"
                    try:
                        return Schema(data=data)
                    except Exception:
                        raise Exception("Generating schema failed, provide schema")
            elif isinstance(schema, str):
                return Schema(schema_path=schema)

        if not isinstance(data, list):
            raise PyGBQError(f"Returned `{type(data)}`. Need `list`.")

        table_id = self._set_table_id(table_id)

        data_batches = [
            data[i * max_insert_num_rows: (i + 1) * max_insert_num_rows]
            for i in range((len(data) + max_insert_num_rows - 1) // max_insert_num_rows)
        ]

        schema = set_schema()

        # TODO: table = client.get_table(table_id);
        # if is_subset(table.schema, schema_api):
        # save old table as table_name_old_schema_index, where index comes from try except
        # recreate table with broader schema and then insert
        if isinstance(how, list):
            update = Update(self.client, how, table_id, schema, expiration)
            for data_batch in data_batches:
                update.merge(data_batch)
        elif how == 'insert':
            update = Update(client=self.client, table_id=table_id)
            for data_batch in data_batches:
                update.insert(data_batch=data_batch)
        elif how == 'replace':
            update_replace = Update(client=self.client, table_id=table_id, schema=schema)
            update_insert = Update(client=self.client, table_id=table_id)
            for i, data_batch in enumerate(data_batches):
                # replace the table with the first data_batch and then insert
                if i == 0:
                    update_replace.replace(data_batch=data_batch)
                else:
                    update_insert.insert(data_batch=data_batch)
        return {"status": 200}

    def _set_table_id(self, table_id):
        if not table_id:
            raise PyGBQNameError("'table_id' must be set")
        if not table_id.replace("_", "").replace(".", "").isalnum():
            raise PyGBQNameError(
                "Bad table name: only letters, numbers and underscores allowed"
            )
        num_dots = table_id.count(".")
        # if table_id == table_name
        if num_dots == 0:
            return f"{self.project_id}.{self.default_dataset}.{table_id}"
        # if table_id == dataset.table_name
        elif num_dots == 1:
            return f"{self.project_id}.{table_id}"
        # if table_id == project_id.dataset.table_name
        elif num_dots == 2:
            return table_id
        raise PyGBQNameError("Bad table name")

    def test(self, test):
        try:
            result = self.client.query(test.format()).result()
        except Exception as E:
            message = f"Bad test query: {test}"
            logging.exception(message)
            return message
        if result.total_rows != 1:
            message = f"'{test}' must return exactly one boolean value, but returned {result.total_rows} rows"
            logging.exception(message)
        else:
            row = [row for row in result][0]
            if len(row) != 1:
                message = f"'{test}' must return exactly one boolean value, but returned {len(row)} columns"
                logging.exception(message)
            value = row[0]
            if isinstance(value, bool):
                if not value:
                    message = f"'{test}' did not pass"
                    logging.exception(message)
                else:
                    message = value
            else:
                message = f"'{test}' must return exactly one boolean value, but returned type {type(value)}"
                logging.exception(message)
        return message

    def query(self, query):
        query_job = self.client.query(query.format())
        try:
            query_job.result()
            message = 'Query executed correctly'
        except Exception as E:
            message = f"Your query '{query}' is incorrect"
            logging.exception(f"Your query '{query}' is incorrect")
        return message

    # def file_name_for_save(self, table_id, parameters, extension: str = ""):
    #     """:arg extension: 'jsonl' or 'csv'"""
    #     file_name = table_id
    #     # TODO: drop chars not suitable for file name
    #     if parameters:
    #         len_kwargs = len(parameters)
    #         file_name = file_name + "?"
    #         for i, (arg, value) in enumerate(parameters.items()):
    #             file_name = file_name + f"{arg}={value}"
    #             if i < len_kwargs - 1:
    #                 file_name = file_name + "&"
    #     file_name = f"{file_name}.{extension}"
    #     return Path(".") / self.save_dir / file_name

    def get_secret(self, secret_id, version="latest"):
        """
        Access the payload for the given secret version if one exists. The version
        can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
        """
        if self.secretmanager_client is None:
            self.secretmanager_client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version}"
        response = self.secretmanager_client.access_secret_version(name=secret_path)
        return response.payload.data.decode("UTF-8")

    def add_secret(self, secret_id, data):
        """
        Add a new secret version to the given secret with the provided data.
        """
        if self.secretmanager_client is None:
            self.secretmanager_client = secretmanager.SecretManagerServiceClient()
        parent = f"projects/{self.project_id}/secrets/{secret_id}"
        payload = secretmanager.types.SecretPayload(data=data.encode("UTF-8"))
        response = self.secretmanager_client.add_secret_version(parent=parent, payload=payload)
        print("Added secret version: {}".format(response.name))

    # @parametrized
    # def gbq(
    #         self,
    #         function: Callable[[Any], List[dict]],  # any arguments and returns a list of dicts
    #         table: str = None,
    #         how: Union[str, List[str]] = None,
    #         schema: Union[str, List[dict]] = None,
    #         query: str = None,
    #         test: str = None
    # ):
    #     if not table:
    #         table = function.__name__
    #         print(table)
    #     table_id = self._set_table_id(table)
    #
    #     def inner(**kwargs):
    #         nonlocal how, schema
    #         data = function(**kwargs)
    #         results = {"data_len": len(data), **kwargs}
    #         if isinstance(data, DataFrame):
    #             if self.save_dir:
    #                 name = self.file_name_for_save(table_id, kwargs, "csv")
    #                 data.to_csv(name, index=False)
    #             if how == "insert":
    #                 how = "append"  # rename to to_gbq syntax
    #             if data:
    #                 data.to_gbq(table_id, project_id=self.project_id, if_exists=how, table_schema=schema)
    #             else:
    #                 logging.warning('Data is empty')
    #                 results["message"] = "data is empty"
    #         elif isinstance(data, list):
    #             if self.save_dir:
    #                 name = self.file_name_for_save(table_id, kwargs, "jsonl")
    #                 save_data(data=data, name=name)
    #             if how:
    #                 if data:
    #                     self.update_table_using_temp(data, table_id, how, schema)
    #                     results["message"] = "success"
    #                 else:
    #                     logging.warning('Data is empty')
    #                     results["message"] = "data is empty"
    #         else:
    #             raise PyGBQError(f"Returned {type(data)}. Need either list or df.")
    #         if test:
    #             self.test(test=test, arguments=kwargs)
    #         if query:
    #             self.query(query, kwargs)
    #         return results
    #
    #     return inner
