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

#define a "Batch Definition"- it determines which records in a Data Asset are retrieved for Validation
batch_definition_name = "goibibo_flights_data_whole"
batch_definition_path = "data/goibibo_flights_data.csv"
batch_definition = data_asset.add_batch_definiton(name=batch_definition_name)
batch = batch_definition.get_batch()
print(batch.head())

#build expectations and add to expectation suite
suite = context.suites.add(
    gx.ExpectationSuite(name="flight_expectation_suite")
)

expectation1 = gx.expectations.ExpectColumnValuesToNotBeNull(column = "airline")
expectation2 = gx.expectations.ExpectColumnDistinctValuesToBeInSet(
    column = "class",
    value_set=['economy', 'business']
)

suite.add_expectation(expectation=expectation1)
suite.add_expectation(expectation=expectation2)

