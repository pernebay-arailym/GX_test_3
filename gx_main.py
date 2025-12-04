import great_expectations as gx

print(gx.__version__)
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

df = pd.read_csv(
    "/Users/pernebayarailym/Documents/Portfolio_Projects_AP/Python Projects/GX_test_3/data/goibibo_flights_data.csv"
)
# df["price"] = df["price"].str.replace(",", "").astype(int)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/pernebayarailym/Documents/Portfolio_Projects_AP/Python Projects/GX_test_3/credentials/gcs_key.json"
)

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

data_source_name = "flights_data_source_37"
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
suite_name = "flight_expectation_suite_33"

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
    column="stops", value_set=["non-stop", "1-stop", "1-stop Via Raipur", "2+-stops"]
)

# 4. "price" should fall within expected range
expectation4 = gx.expectations.ExpectColumnDistinctValuesToBeInSet(
    column="from",
    value_set=["Delhi", "Bangalore", "Chennai", "Hyderabad", "Kolkata", "Mumbai"],
)

# 5. airline companies
expectation5 = gx.expectations.ExpectColumnDistinctValuesToBeInSet(
    column="airline",
    value_set=[
        "Air India",
        "Indigo",
        "SpiceJet",
        "Vistara",
        "AirAsia",
        "GO FIRST",
        "Trujet",
        "StarAir",
    ],
)

suite.add_expectation(expectation=expectation1)
suite.add_expectation(expectation=expectation2)
suite.add_expectation(expectation=expectation3)
suite.add_expectation(expectation=expectation4)
suite.add_expectation(expectation=expectation5)

context.suites.add_or_update(suite)

# define 'Validation Definition' : a Validation Definition is a fixed reference that links a Batch of data to an Expectation Suite
vd_name = "flight_batch_definition_31"
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
cp_name = "flight_checkpoint_28"
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
# context.checkpoints.save(checkpoint)
# results = context.run_checkpoint("flight_checkpoint_17")
# results

# print(validation_results)

# save results in the folder
Path("results").mkdir(exist_ok=True)

with open("results/validation_results.txt", "w") as f:
    f.write(str(validation_results))


# EMAIL NOTIFICATION

import smtplib
from email.mime.text import MIMEText
from datetime import datetime
import pytz


# 1. Extract success/failure
all_success = validation_results.success
failed_expectations = []

for run_result in validation_results.run_results.values():
    for res in run_result.results:  # use .results in v1.9.1
        if not res.success:
            # Access expectation type safely for v1.9.1
            failed_expectations.append(res.expectation_config["expectation_context"])


# 2. Build email content

paris_tz = pytz.timezone("Europe/Paris")
timestamp = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")

if all_success:
    subject = "GE Validation PASSED ✓"
    body = f"""
Great Expectations Validation Report
Time: {timestamp}

All expectations passed successfully.

Total expectations: 5
Passed: 5
Failed: 0
"""
else:
    subject = "GE Validation FAILED ✗"
    body = f"""
Great Expectations Validation Report
Time: {timestamp}

Some expectations FAILED.

Total expectations: 5
Passed: {5 - len(failed_expectations)}
Failed: {len(failed_expectations)}

Failed expectation types:
{failed_expectations}

See full details in: results/validation_results.txt
"""


# 3. Send email via Gmail SMTP

sender_email = "pernebayarailym@gmail.com"
receiver_email = "pernebayarailym@gmail.com"
app_password = os.getenv("GMAIL_APP_PASSWORD")

msg = MIMEText(body)
msg["Subject"] = subject
msg["From"] = sender_email
msg["To"] = receiver_email

try:
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender_email, app_password)
        server.send_message(msg)
    print("Email notification sent.")
except Exception as e:
    print("Error sending email:", e)
