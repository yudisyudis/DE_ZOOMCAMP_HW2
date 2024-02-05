# DE_ZOOMCAMP_HW2
## THIS IS A REPOSITORY OF MODULE 2 HOMEWORK IN DATA ENGINEERING ZOOMCAMP

Module 2 is about learning to create ETL Pipeline using Mage. We create an end to end ETL from loading data from one source to exporting that data to another database. I would like to use this repo to save my progress on doing my Homework.

First, in Mage, I create a new pipeline named green_taxi_etl:
![image](https://github.com/yudisyudis/DE_ZOOMCAMP_HW2/assets/91902011/615c58d9-af16-4fff-a3dc-4e9dc5ab0917)

After that, we load data from API, utilize FOR LOOP and IF ELSE statement:

```
import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    for i in range(10, 13):
        
        taxi_dtypes = {
                        'VendorID': pd.Int64Dtype(),
                        'passenger_count': pd.Int64Dtype(),
                        'trip_distance': float,
                        'RatecodeID': pd.Int64Dtype(),
                        'store_and_fwd_flag': str,
                        'PULocationID': pd.Int64Dtype(),
                        'DOLocationID': pd.Int64Dtype(),
                        'payment_type': pd.Int64Dtype(),
                        'fare_amount': float,
                        'extra': float,
                        'mta_tax': float,
                        'tip_amount': float,
                        'tolls_amount': float,
                        'ehail_fee': float,
                        'improvement_surcharge': float,
                        'total_amount': float,
                        'trip_type': float,
                        'congestion_surcharge': float
                    }

        parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

        if(i == 10):
            url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{i}.csv.gz'
            df_1 = pd.read_csv(url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
        elif(i == 11):
            url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{i}.csv.gz'
            df_2 = pd.read_csv(url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
        elif(i == 12):
            url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{i}.csv.gz'
            df_3 = pd.read_csv(url , sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)

    return pd.concat([df_1, df_2, df_3], ignore_index=True)
     	 	 	 	 	 	 	 	 	 	


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
```

Then, we transform the data as instructed, which include remove columns with zero trip distance and zero passenger count, create new date column, rename teh camel case column.

```
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    
    passenger_null = data['passenger_count'].isin([0]).sum()
    trip_null = data['trip_distance'].isin([0]).sum()

    print(f"Row with zero passenger: {passenger_null}")
    print(f"Row with zero distance: {trip_null}")

    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    data.columns = (data.columns
                    .str.replace(' ', '_')
                    .str.lower()
                    )

    return data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]




@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are ride with zero passenger'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are ride with zero distance'
    assert output['vendorid'].isin(output['vendorid']).all(), 'vendorid is not valid'

```

Finally we export the data in two ways, to PostgreSQL database, and to GCP bucket.

to PostgreSQL:
```
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    schema_name = 'mage'  # Specify the name of the schema to export data to
    table_name = 'green_taxi'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'dev'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='replace',  # Specify resolution policy if table name already exists
        )

```

to GCS:
```
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/mage-zoomcamp-yudisyudis-b277ef4c4f23.json"

bucket_name = 'mage-bucket-yudisyudis'
project_id = 'mage-zoomcamp-yudisyudis'

table_name = 'green_taxi_data'

root_path = f'{bucket_name}/{table_name}' 

@data_exporter
def export_data(data, *args, **kwargs):
    
    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )

```

The final pipeline scheme look like this:
![image](https://github.com/yudisyudis/DE_ZOOMCAMP_HW2/assets/91902011/ec0e56df-b53a-4e86-96d7-3223cdab3a96)

To answer homework question, we look directly from all of the step above.

### question 1:
![image](https://github.com/yudisyudis/DE_ZOOMCAMP_HW2/assets/91902011/e4e2905b-6538-427d-9c85-6a04f3d17f67)

### question 2:
![image](https://github.com/yudisyudis/DE_ZOOMCAMP_HW2/assets/91902011/c82e44f5-d7e9-4d50-8603-78d34916c78f)

### question 3:
```
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
```

### question 4:
We use SQL loader to make sure all of the value of vendorid column:
```
SELECT DISTINCT vendorid FROM {{ df_1 }}
```
and the answer is:
![image](https://github.com/yudisyudis/DE_ZOOMCAMP_HW2/assets/91902011/ee0320ce-4156-4393-be91-50d8dbe8b3e9)

### question 5:
```
'VendorID': pd.Int64Dtype(),
'RatecodeID': pd.Int64Dtype(),
'PULocationID': pd.Int64Dtype(),
'DOLocationID': pd.Int64Dtype(),
```
4

### question 6:
Partition in GCS bucket:
![image](https://github.com/yudisyudis/DE_ZOOMCAMP_HW2/assets/91902011/fc31b18c-ddda-4adc-9284-3bbb431e9def)



