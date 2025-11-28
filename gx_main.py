import great_expectations as gx

print(gx.__version__)
import pandas as pd
from pathlib import Path
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/arailym.pernebay/Documents/GX_test_projects/GX_test_3/credentials/gcs_key.json"
)

# gx context setup
context = gx.get_context(mode="file")

# define a datasource
# data_source_name = "flights_data_source"
# bucket_or_name = "flights-dataset-yt-tutorial"
# gcs_options = {}
# data_source = context.data_sources.add_pandas_gcs(
#    name="flights_data_source", bucket_or_name="flights-dataset-yt-tutorial", gcs_options = gcs_options
#    )

data_source_name = "flights_data_source"
bucket_or_name = "flights-dataset-yt-tutorial"
gcs_options = {}
data_source = context.data_sources.add_pandas_gcs(
    name="flights_data_source",
    bucket_or_name="flights-dataset-yt-tutorial",
    gcs_options=gcs_options,
)

# define a Data Asset
asset_name = "goibibo_flights_data"
gcs_prefix = "data/goibibo_flights_data.csv"
data_asset = data_source.add_csv_asset(name=asset_name, gcs_prefix=gcs_prefix)

# define a "Batch Definition"- it determines which records in a Data Asset are retrieved for Validation
batch_definition_name = "goibibo_flights_data_whole"
batch_definition_path = "data/goibibo_flights_data.csv"
batch_definition = data_asset.add_batch_definiton(name=batch_definition_name)
batch = batch_definition.get_batch()
print(batch.head())

# build expectations and add to expectation suite
suite = context.suites.add(gx.ExpectationSuite(name="flight_expectation_suite"))

expectation1 = gx.expectations.ExpectColumnValuesToNotBeNull(column="airline")
expectation2 = gx.expectations.ExpectColumnDistinctValuesToBeInSet(
    column="class", value_set=["economy", "business"]
)

suite.add_expectation(expectation=expectation1)
suite.add_expectation(expectation=expectation2)

# define 'Validation Definition' : a Validation Definition is a fixed reference that links a Batch of data to an Expectation Suite
validation_definition = gx.ValidationDefinition(
    data=batch_definition, suite=suite, name="flight_batch_definition"
)

validation_definition = context.validation_definitions.add(validation_definition)
validation_results = validation_definition.run()
print(validation_results)

# create a Checkpoint with Actions for multiple validation_definition
validation_definitions = [validation_definition]  # can be multiple definitions

# create a list of Actions for the Checkpoint to perform
action_list = [
    # this Action sends a Slack Notification if a Expectation fails
    gx.checkpoint.SlackNotificationAction(
        name="send_slack_notification_on_failed_expectations",
        slack_token="${validation_notification_slack_webhook}",
        slack_channel="${validation_notification_slack_channel}",
        notify_on="failure",
        show_failed_expectations=True,
    ),
    # This Action updates the Data Docs static website with the Validation
    # results after the Checkpoint is run
    gx.checkpoint.UpdateDataDocsAction(
        name="update_all_data_docs",
    ),
]

checkpoint = gx.Checkpoint(
    name="flight_checkpoint",
    validation_definitions=validation_definitions,
    actions=action_list,
    result_format={"result_format": "COMPLET"},
)

context.checkpoints.add(checkpoint)

# run checkpoint
validation_results = checkpoint.run()
# print(validation_results)

# save results in the folder
Path("results").mkdir(exist_ok=True)

with open("results/validation_results.txt", "w") as f:
    f.write(str(validation_results))
