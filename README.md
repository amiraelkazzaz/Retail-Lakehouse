# Retail-Lakehouse
Architecture Overview
<img width="1536" height="1024" alt="ChatGPT Image Feb 14, 2026, 05_18_06 PM" src="https://github.com/user-attachments/assets/ce04eec8-17c0-4a17-8871-6a29c834c70d" />

---
A modern end-to-end **data lakehouse** built to ingest retail data from spreadsheets, process it at scale, and expose it for analytics, BI, and reporting.

This project demonstrates how raw business files become production-ready datasets using industry-standard data engineering tools.

---

## 📌 Project Overview

Retail companies often store sales data in Excel files scattered across departments.
This creates problems like:

* inconsistent formats
* duplicated data
* slow reporting
* no single source of truth

The Retail Lakehouse centralizes data, validates it, and makes it queryable in real time.

---

## 🎯 Business Problem

Decision makers need answers for questions like:

* What are the best selling products?
* Which countries generate the highest revenue?
* How do sales change over time?
* Who are the most valuable customers?

But Excel files are not designed for scalable analytics.

---

## 💡 Solution

We transform raw spreadsheets into structured, reliable analytical tables using a lakehouse architecture.

**Flow:**

Excel → CSV → Object Storage → Data Lake Tables → SQL Engine → Dashboards

---

## 🧱 Architecture

```
Data Source (Excel with 2 sheets)
        ↓
      CSV
        ↓
 Object Storage (S3 / MinIO)
        ↓
   Apache Iceberg Tables
        ↓
     SQL Query Engine
        ↓
   BI & Visualization
```

---

## ⚙️ Technologies Used

* **Storage:** MinIO (S3 compatible)
* **Table Format:** Apache Iceberg
* **Processing:** Apache Spark / PySpark
* **Catalog:** Project Nessie / Polaris
* **SQL Engine:** Trino
* **Orchestration:** Airflow
* **Visualization:** Superset
* **Containers:** Docker & Docker Compose

---

## 🔄 Data Pipeline

### 1️⃣ Ingestion

* Source Excel file contains two sheets.
* Converted into CSV files.
* Uploaded to object storage.

### 2️⃣ Processing

* Spark reads CSV data.
* Cleans & transforms.
* Writes Iceberg analytical tables.

### 3️⃣ Serving

* Trino queries the tables.
* Superset builds dashboards and reports.

---

## 📊 What the Customer Gains

✅ Faster analytics
✅ Reliable & consistent data
✅ Historical tracking
✅ Scalable architecture
✅ SQL access for analysts
✅ Ready for machine learning

---

## 🚀 How to Run

```bash
docker compose up -d
```

Services will start including Spark, Trino, Superset, catalog, and storage.

---

## 📁 Project Structure

```
airflow/        → DAGs & orchestration  
spark/          → processing jobs  
trino/          → SQL engine  
superset/       → dashboards  
warehouse/      → table storage  
data/           → raw files  
```

---

## 🧠 Skills Demonstrated

* Data Engineering
* ETL / ELT design
* Lakehouse architecture
* Distributed processing
* Query optimization
* Containerized environments

---

## 👤 Author

**Amira Elkazzaz**
Data Engineer | Lakehouse Enthusiast

---
