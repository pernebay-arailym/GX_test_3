# 🛫 Great Expectations – Flight Data Quality Pipeline

A fully automated **data validation pipeline** built with **Great Expectations (GX)** to ensure data quality for Goibibo flight datasets.
This project loads flight data, validates it against custom expectations, generates Data Docs, and sends **email notifications** when validations pass or fail.

---

## 🚀 Features

* **Great Expectations Data Context** (file-based)
* **Pandas + Google Cloud Storage (GCS)** datasource
* Custom **Expectation Suite** for flight data:

  * `airline` cannot be null
  * `class` limited to economy/business
  * `stops` limited to known categories
  * `from` column restricted to major Indian cities
  * Valid airline names only
* Automated **Checkpoint** execution
* Generates **Data Docs** for validation results
* Saves validation output to `/results`
* Automated **Email notifications** using Gmail SMTP

---

## 📁 Project Structure

```
.
├── data/
│   ├── goibibo_flights_data.csv
│   └── goibibo_flights_data_clean.csv
├── credentials/
│   └── gcs_key.json
├── results/
│   └── validation_results.txt
├── gx/                      # Auto-generated Great Expectations directory
├── gx_main.py                  # Main pipeline script
└── README.md
```

---

## 🔧 Technologies Used

* **Python**
* **Great Expectations**
* **Pandas**
* **Google Cloud Storage (GCS)**
* **dotenv**
* **SMTP (email notifications)**

---

## ⚙️ How It Works

1. Load and clean flight data
2. Configure GX context and GCS datasource
3. Build Data Asset → Batch Definition
4. Create custom Expectation Suite
5. Create Checkpoint and run validation
6. Export results + update Data Docs
7. Send email with pass/fail summary

---

## ▶️ Running the Pipeline

1. Install dependencies:

   ```bash
   pip install great_expectations pandas python-dotenv pytz
   ```
2. Set Gmail App Password in `.env`:

   ```
   GMAIL_APP_PASSWORD=your_app_password_here
   ```
3. Place your GCS key file in `credentials/`
4. Run:

   ```bash
   python gx_main.py
   ```

---

## 📨 Email Notification Example

* **PASSED:** All expectations met
* **FAILED:** Includes a detailed list of failed expectations
* Automatically includes timestamp in **Paris timezone**

---

## 📘 Data Docs

After running the pipeline, open your automatically generated Data Docs at:

```
gx/uncommitted/data_docs/local_site/index.html
```

---

## 👤 Author

**Arailym Pernebay**
Data Engineering & Analytics Projects
📧 [pernebayarailym@gmail.com](mailto:pernebayarailym@gmail.com)

## License
This project is licensed under the MIT License.

---
