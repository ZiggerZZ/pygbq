# PyGBQ

> Easily integrate data in BigQuery

## Example

The following snippet

```python
from pygbq import gbq, set_dataset
set_dataset('Finance')


@gbq(how='replace')
def transactions(start_date):
    data = ...
    return data

if __name__ == "__main__":
    transactions("2020-11-25")
```

will (re)create table `Finance.transactions` in your default project.

## Install and set up

`pip install pygbq`

Set up the [authentication](https://cloud.google.com/docs/authentication/getting-started).

## Documentation

* gbq - main decorator  
* update_table_using_temp - if you want to use the decorator as a function  
* table - friendly interface to get a table from BigQuery  
* set_dataset - set default dataset in the project  
* MyError - if you need to return a value with Flask  
* read_jsonl - read newline delimited json  
* generate_schema - might be useful for the first integration when you want to see schema  
* get_secret - get a secret version from Secret Manager  
* add_secret - add a secret version to Secret Manager