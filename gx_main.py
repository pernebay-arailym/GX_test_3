import great_expectations as gx

print(gx.__version__)
import pandas as pd
from pathlib import Path
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/pernebayarailym/Documents/Portfolio_Projects_AP/Python Projects/GX_test_3/credentials/gcs_key.json"
)

df = pd.read_csv(
    "/Users/pernebayarailym/Documents/Portfolio_Projects_AP/Python Projects/GX_test_3/data/goibibo_flights_data.csv"
)
df["price"] = df["price"].str.replace(",", "").astype(int)

# optional: re-save cleaned file for GE to read
df.to_csv("data/goibibo_flights_data_clean.csv", index=False)
# gx context setup
context = gx.get_context(mode="file")

# define a datasource
# data_source_name = "flights_data_source"
# bucket_or_name = "flights-dataset-yt-tutorial"
# gcs_options = {}
# data_source = context.data_sources.add_pandas_gcs(
#    name="flights_data_source", bucket_or_name="flights-dataset-yt-tutorial", gcs_options = gcs_options
#    )

data_source_name = "flights_data_source_23"
bucket_or_name = "flights-dataset-tutorial"
gcs_options = {}
data_source = context.data_sources.add_pandas_gcs(
    name=data_source_name,
    bucket_or_name=bucket_or_name,
    gcs_options=gcs_options,
)

# define a Data Asset
asset_name = "goibibo_flights_data"
gcs_prefix = "data/"
data_asset = data_source.add_csv_asset(name=asset_name, gcs_prefix=gcs_prefix)

# define a "Batch Definition"- it determines which records in a Data Asset are retrieved for Validation
batch_definition_name = "goibibo_flights_data_whole"
batch_definition_path = "data/goibibo_flights_data.csv"
batch_definition = data_asset.add_batch_definition(name=batch_definition_name)
batch = batch_definition.get_batch()
print(batch.head())

# build expectations and add to expectation suite
suite_name = "flight_expectation_suite_18"

try:
    suite = context.suites.get(suite_name)
except gx.exceptions.DataContextError:
    suite = gx.ExpectationSuite(name=suite_name)
    context.suites.add_or_update(suite)


expectation1 = gx.expectations.ExpectColumnValuesToNotBeNull(column="airline")
expectation2 = gx.expectations.ExpectColumnDistinctValuesToBeInSet(
    column="class", value_set=["economy", "business"]
)
# 3. "stops" column must contain only defined stop categories
expectation3 = gx.expectations.ExpectColumnDistinctValuesToBeInSet(
    column="stops", value_set=["non-stop", "1-stop"]
)

# 4. "price" should fall within expected range (string comparison but still meaningful)
expectation4 = gx.expectations.ExpectColumnValuesToBeBetween(
    column="price", min_value=5000, max_value=7000
)

# 5. "duration" must follow pattern like '02h 10m'
expectation5 = gx.expectations.ExpectColumnValuesToMatchRegex(
    column="duration", regex=r"^\d{2}h \d{2}m$"
)


suite.add_expectation(expectation=expectation1)
suite.add_expectation(expectation=expectation2)
suite.add_expectation(expectation=expectation3)
suite.add_expectation(expectation=expectation4)
suite.add_expectation(expectation=expectation5)

context.suites.add_or_update(suite)

# define 'Validation Definition' : a Validation Definition is a fixed reference that links a Batch of data to an Expectation Suite
vd_name = "flight_batch_definition_17"
try:
    validation_definition = context.validation_definitions.get(vd_name)
except gx.exceptions.DataContextError:
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(data=batch_definition, suite=suite, name=vd_name)
    )


# create a Checkpoint with Actions for multiple validation_definition
validation_definitions = [validation_definition]  # can be multiple definitions

# create a list of Actions for the Checkpoint to perform
# action_list = [
#    # this Action sends a Slack Notification if a Expectation fails
#    gx.checkpoint.SlackNotificationAction(
#        name="send_slack_notification_on_failed_expectations",
#        slack_token="${validation_notification_slack_webhook}",
#        slack_channel="${validation_notification_slack_channel}",
#        notify_on="failure",
#        show_failed_expectations=True,
#    ),
#    # This Action updates the Data Docs static website with the Validation
#    # results after the Checkpoint is run
#    gx.checkpoint.UpdateDataDocsAction(
#        name="update_all_data_docs",
#    ),
# ]

action_list = [
    gx.checkpoint.UpdateDataDocsAction(
        name="update_all_data_docs",
    ),
]
cp_name = "flight_checkpoint_14"
try:
    checkpoint = context.checkpoints.get(cp_name)
except gx.exceptions.DataContextError:
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name=cp_name,
            validation_definitions=validation_definitions,
            actions=action_list,
            result_format={"result_format": "COMPLETE"},
        )
    )


# run checkpoint
validation_results = checkpoint.run()
context.checkpoints.save(checkpoint)
results = context.run_checkpoint("flight_checkpoint_14")
results

# print(validation_results)

# save results in the folder
Path("results").mkdir(exist_ok=True)

with open("results/validation_results.txt", "w") as f:
    f.write(str(validation_results))
