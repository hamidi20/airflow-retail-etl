# 🚀 Airflow Retail ETL Pipeline

## 📌 Project Overview

This project demonstrates an end-to-end ETL pipeline built using **Apache Airflow** and **PostgreSQL**.
The architecture separates the **Master (source) database** and **Warehouse (analytics) database** using **PostgreSQL Foreign Data Wrapper (FDW)**.
The pipeline extracts retail transaction data from a CSV file, loads it into a master database, and transforms it into a structured warehouse table ready for analytics.

---

## 🏗️ Architecture

CSV File  
⬇  
Master Database (`retail_master`)  
⬇ (PostgreSQL FDW)  
Warehouse Database  
⬇  
`retail_sales` (Fact Table)

---

## 🔄 ETL Workflow

### 1️⃣ Extract & Load (PythonOperator)

- 📂 Read CSV file (`retail-dataset.csv`)
- 🗑️ Truncate `retail_master`
- ⚡ Bulk load using PostgreSQL `COPY`
- 🕒 Handle date format with:
  
  ```sql
  SET datestyle TO 'ISO, DMY';
  ```
---

### 2️⃣ Transform (PostgresOperator)

- 🔗 Connect to Warehouse DB
- 📊 Query `retail_master` via FDW
- 🧹 Apply data transformation:
  - Handle NULL values using `COALESCE`
  - Filter by date range
  - Create calculated column:
  ```sql
  totalamount = quantity * unitprice
  ```
- 💾 Insert results into `retail_sales`

---

## 🛠️ Technologies Used

- 🐍 Python 3
- 🌪️ Apache Airflow 2.x
- 🐘 PostgreSQL
- 🔌 PostgreSQL FDW
- 🐳 Docker & Docker Compose
- 🗂️ Git & GitHub

---

## 👨‍💻 Author

**Hamidi**  
Data Engineer  
Specializing in ETL, Data Warehousing, and Data Orchestration
