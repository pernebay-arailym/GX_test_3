# Flight Data Validation Project using Great Expectations and GCS

## Overview
This project demonstrates how to use **Great Expectations** (GX) for data quality validation on flight data stored in a **Google Cloud Storage (GCS) bucket**. The dataset was obtained from **Kaggle** and contains flight details such as airline, class, departure, and arrival times.  

The main goal is to **validate the dataset** using expectations and checkpoints, ensuring critical columns are not null and values follow expected categories.

---

## Key Features
- Upload and manage flight dataset in GCS.
- Configure Great Expectations to connect to GCS.
- Define **Data Sources, Data Assets, Batch Definitions, Expectation Suites**, and **Validation Definitions**.
- Implement **Checkpoints** to automate validation.
- Save validation results locally for analysis.
- Overcome common permission and configuration issues when connecting GX to GCS.

---
## Challenges Faced

- Encountered 403 Forbidden errors due to missing GCS permissions (storage.buckets.get).
- Required adding both: Storage Object Viewer | Storage Legacy Bucket Reader
- Checkpoint Slack actions caused missing env-variable errors; fixed by disabling Slack during development.

---
## Project Setup

### 1. Prepare Google Cloud
1. Created a **Google Cloud Project**.
2. Created a **GCS bucket**: `flights-dataset-tutorial`.
3. Uploaded Kaggle flight dataset to the bucket under `data/goibibo_flights_data.csv`.
4. Created a **service account** and downloaded the JSON key.
5. Assigned the following roles to the service account:
   - `Storage Object Viewer` → to read files.
   - `Storage Legacy Bucket Reader` → to access bucket metadata (required by GX).

### 2. Configure Environment
- Added the JSON key path in Python using environment variables:

```python
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/gcs_key.json"
```

### 3. Set Up Great Expectations

- Initialized a **GE Data Context**.
- Added a **pandas GCS datasource** pointing to the bucket.
- Defined a **Data Asset** (CSV file) and **Batch Definition** to specify which records to validate.
- Built an **Expectation Suite**:
  - `airline` column must not be null.
  - `class` column must have values from `["economy", "business"]`.

### 4. Validation Definition and Checkpoints

- Created a Validation Definition linking the batch and expectation suite.
- Created a Checkpoint to run validations and optionally update Data Docs.
- Disabled Slack actions during development to avoid config errors.

### 5. Run Validation / Save results locally
```
from pathlib import Path
Path("results").mkdir(exist_ok=True)
with open("results/validation_results.txt", "w") as f:
    f.write(str(validation_results))
```

---

## Outcome

- Successfully validated 300k+ flight records.
- Ensured that key data quality expectations passed.
- Saved validation results locally for review.
- Developed a repeatable workflow combining GCS and Great Expectations.
---
## Tools & Libraries
- Python 3.12
- Great Expectations 1.9.1
- pandas
- Google Cloud Storage (GCS)
