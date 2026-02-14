# Retailer Sales Data Pipeline (Medallion Lakehouse)

A complete **Medallion Architecture (Bronze → Silver → Gold)** based Retail Sales Analytics pipeline.  
This project demonstrates how to build an end-to-end **Lakehouse ETL pipeline** using raw Sales, Customer, and Product datasets, including **SCD Type 2 historical tracking**, data cleansing, and Gold-level KPI reporting.

---

## 🚀 Project Highlights

- ✅ Built a **Medallion-based Sales Analytics Lakehouse**
- ✅ Loaded raw Sales, Customer, and Product data into **Bronze layer**
- ✅ Used **append-only ingestion** for raw data landing
- ✅ Implemented **Silver layer cleansing + standardization**
- ✅ Implemented **SCD Type 2** for Customer/Product history tracking
- ✅ Built **Gold reporting tables** using joins and derived KPIs
- ✅ Designed pipeline suitable for **Databricks / Spark / Delta Lake**

---

## 🏗️ Architecture Overview

### **Bronze Layer (Raw)**
- Stores raw data as-is
- Append-only ingestion
- No business rules applied

### **Silver Layer (Cleansed + Conformed)**
- Cleans invalid records
- Standardizes formats (dates, strings, null handling)
- Applies business logic
- Maintains history using **SCD Type 2**

### **Gold Layer (Analytics + KPIs)**
- Reporting-ready tables
- Aggregations and KPI calculations
- Optimized for BI dashboards

---

## 📂 Dataset Used

This pipeline uses 3 core datasets:

### 1. Sales Dataset
Contains transaction-level sales details.

Example fields:
- `transaction_id`
- `customer_id`
- `product_id`
- `quantity`
- `price`
- `date`

### 2. Customer Dataset
Contains customer master data.

Example fields:
- `customer_id`
- `name`
- `region`

### 3. Product Dataset
Contains product master data.

Example fields:
- `product_id`
- `product_name`
- `category`

---

## 🧱 Medallion Data Model

### Bronze Tables
| Table Name | Description |
|----------|-------------|
| `bronze_sales` | Raw sales transactions (append-only) |
| `bronze_customers` | Raw customer master data |
| `bronze_products` | Raw product master data |

---

### Silver Tables
| Table Name | Description |
|----------|-------------|
| `silver_sales_clean` | Cleaned sales transactions |
| `silver_customers_scd2` | Customer SCD Type 2 table |
| `silver_products_scd2` | Product SCD Type 2 table |

---

### Gold Tables
| Table Name | Description |
|----------|-------------|
| `gold_sales_fact` | Final sales fact table (joined with dimensions) |
| `gold_daily_sales_kpi` | Daily sales KPIs |
| `gold_region_sales_kpi` | Region-wise performance KPIs |
| `gold_top_products` | Top-selling products |

---

## 🔄 Pipeline Flow

```text
Raw CSV Files
   |
   v
Bronze Layer (Append Only Delta Tables)
   |
   v
Silver Layer (Cleansing + SCD2)
   |
   v
Gold Layer (Joins + KPIs + Aggregations)
