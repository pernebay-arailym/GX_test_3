import great_expectations as gx
import pandas as pd 
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/arailym.pernebay/Documents/GX_test_projects/GX_test_3/data/goibibo_flights_data.csv"

#gx context setup
context = gx.get_context(mode='file')

#define a datasource
data_source_name = "flights_data_source"
bucket_or_name = "flights-dataset-yt-tutorial"
gcs_options = {}
data_source = context.data_sources.add_pandas_gcs(
    name = "flights_data_source", bucket_or_name = "flights-dataset-yt-tutorial", gcs_options = {}
    )

#define a Data Asset
asset_name = "goibibo_flights_data"
gcs_prefix = "data/goibibo_flights_data.csv"
data_asset = data_source.add_csv_asset(name=asset_name, gcs_prefix=gcs_prefix)