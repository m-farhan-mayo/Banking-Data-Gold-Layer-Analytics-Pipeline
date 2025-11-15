# Banking Data Gold-Layer Analytics Pipeline

**Author:** Farhab Mayo  
**Project:** Banking Data Gold-Layer Analytics Pipeline  
**Tech Stack:** PySpark, Delta Lake, Python, Databricks/Spark  
**Status:** Completed  

---

## **Project Overview**

This project demonstrates the end-to-end development of a **Gold-layered banking analytics pipeline**. The pipeline ingests raw data from multiple banking sources, cleans and transforms it, and generates **dimension and fact tables** for reporting and analytics.  

The primary goal was to build a **scalable, production-ready Gold layer** for Customers, Accounts, Transactions, and Branches datasets.

---

## **Data Sources**

1. **Customers:** Customer_ID, Full_Name, Email, Phone, Address, City, Country, Created_At  
2. **Accounts:** Account_ID, Customer_ID, Account_Type, Balance, Currency, Created_At, Status  
3. **Transactions:** Transaction_ID, Account_ID, Transaction_Type, Amount, Currency, Transaction_Date, Description  
4. **Branches:** Branch_ID, Branch_Name, Address, City, Country, Opened_On  

---

## **Pipeline Phases**

### **Phase 1 — Data Ingestion**
- Raw datasets were ingested in CSV and JSON formats.
- Loaded into PySpark DataFrames for further processing.
- Data types were inferred, and initial validations were applied.

### **Phase 2 — Bronze Layer**
- Stored raw ingested data in Delta format for persistence.
- No transformations were applied; data kept **as-is** for traceability.

### **Phase 3 — Silver Layer (Cleansed Data)**
- Parsed JSON columns (Accounts, Transactions) into structured columns.  
- Handled invalid timestamps using `try_to_date` / `to_timestamp` to avoid pipeline failures.  
- Cleaned strings: trimmed whitespace, standardized phone numbers and emails.  
- Deduplicated datasets by primary keys (Customer_ID, Account_ID, Transaction_ID, Branch_ID).  
- Stored cleaned datasets in Delta format for **Silver layer**.

### **Phase 4 — Gold Layer (Final Analytics Tables)**

#### **1. Dimension Tables**
- **dim_customer:** Customer_ID, Full_Name, Email, Phone, Address, City, Country, Created_At  
- **dim_account:** Account_ID, Customer_ID, Account_Type, Balance, Currency, Created_At, Status  
- **dim_branch:** Branch_ID, Branch_Name, Address, City, Country, Opened_On  
- **dim_transaction:** Transaction_ID, Account_ID, Transaction_Type, Currency, Description, Transaction_Timestamp  

#### **2. Fact Tables**
- **fact_account_metrics:** total balance, average balance, number of accounts by type/status/currency.  
- **fact_branch_metrics:** total branches, average branch age by country/city.  
- **fact_transaction_account:** total/average transaction amounts, transaction counts per account.  
- **fact_transaction_type:** total/average transaction amounts, transaction counts per type.  
- **fact_transaction_currency:** total/average transaction amounts, transaction counts per currency.  
- **fact_transaction_daily:** total amount and transaction counts per day.

**Notes:**  
- Invalid or missing timestamps in branches/transactions handled as `NULL`.  
- Aggregations ignore `NULL` values to prevent incorrect metrics.  
- Dimension tables contain descriptive attributes; Fact tables contain measurable KPIs.  

---

### **Phase 5 — Aggregations & KPIs**
- Calculated total, average, and counts for balances and transaction amounts.  
- Derived branch age in days using `datediff(current_date(), opened_on)` for branch KPIs.  
- Daily transaction counts and totals generated for reporting purposes.

---

### **Phase 6 — Error Handling & Data Quality**
- Null and invalid data flagged or transformed.  
- Deduplication applied to all datasets.  
- Logging applied at ingestion and transformation steps (optional in production).  

---

### **Phase 7 — Delta Lake Storage**
- All cleaned and transformed data stored in **Delta Lake** format.  
- Enables incremental updates, ACID compliance, and time-travel queries.  

---

### **Phase 8 — Data Modeling**
- Separated dimension and fact tables for a **Star Schema design**.  
- Fact tables join with dimension tables using primary keys (Customer_ID, Account_ID, Branch_ID, Transaction_ID).

---

### **Phase 9 — Documentation & Diagrams**
- Architecture diagram, sequence diagram, and data flow diagram created.  
- Showed flow from raw ingestion → Silver → Gold layer.  
- Highlights transformations, joins, and aggregations.

---

### **Phase 10 — Final Documentation & Case Study**
- Prepared LinkedIn post to showcase project.  
- README document summarizes pipeline steps, datasets, transformations, and Gold layer design.  

---

## **Project Outputs**
- **Dimension Tables:**  
  - `/gold/dim_customer`  
  - `/gold/dim_account`  
  - `/gold/dim_branch`  
  - `/gold/dim_transaction`  

- **Fact Tables:**  
  - `/gold/fact_account_metrics`  
  - `/gold/fact_branch_metrics`  
  - `/gold/fact_transaction_account`  
  - `/gold/fact_transaction_type`  
  - `/gold/fact_transaction_currency`  
  - `/gold/fact_transaction_daily`  

- **Diagrams:** Stored under `/docs/` folder.  

---

## **How to Run**
```bash
git clone <repo-url>
cd Banking-Gold-Layer-Pipeline
python run_pipeline.py   # Or run in Databricks notebook
