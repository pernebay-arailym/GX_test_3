import great_expectations as gx
import pandas as pd 
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/your/credentials.json"

#gx context setup
context = gx.get_context(mode='file')

#define a datasource
data_source_name = "flights_data_source"
bucket_or_name = "flights-dataset-yt-tutorial"
gcs_options = {}
data_source = context.data_sources.add_pandas_gcs(name = "flights_data_source", bucket_or_name = "flights-dataset-yt-tutorial" gcs_options = {})