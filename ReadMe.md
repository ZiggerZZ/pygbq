# PyGBQ

> Easily integrate data in BigQuery

## Example

The following snippet

```python
from pygbq import Client
import requests
client = Client(default_dataset='Finance')

def transactions(start_date):
    data = requests.get(url='some/api/endpoint', headers={'start_date': start_date}).json()
    client.update_table_using_temp(data=data, table_id='transactions', how=['id'])
    return {'status': 200}

if __name__ == "__main__":
    transactions("2020-11-25")
```

will upsert data to table `Finance.transactions` by `id`.

## Install and set up

`pip install pygbq`

Set up the [authentication](https://cloud.google.com/docs/authentication/getting-started).

## Documentation

* Client - you can set `default_dataset`, `save_dir`, `path_to_key`
* Client.update_table_using_temp - the main update function, use `how='insert'` to insert data 
and `how=['field1', 'field2']` to upsert (merge) by `field1` and `field2`  
* read_jsonl - read newline delimited json  
* Client.get_secret - get a secret version from Secret Manager  
* Client.add_secret - add a secret version to Secret Manager