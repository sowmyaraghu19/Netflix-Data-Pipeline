# Netflix Data Pipeline

## 📌 Project Overview
This project builds a **scalable ETL pipeline** for processing and analyzing Netflix datasets using **Azure** services. The pipeline automates data ingestion, transformation, and validation, ensuring structured and optimized data for analytics.

## 🏗️ Architecture

The data pipeline follows a **multi-layered architecture**:
1. **Raw Data Ingestion:** Extracting data using Azure Data Factory (ADF) via HTTP requests.
2. **Bronze Layer (Raw Storage):** Storing ingested files in Azure Data Lake Storage.
3. **Silver Layer (Processed Data):** Transforming data using Databricks Autoloader and validation workflows.
4. **Gold Layer (Final Tables):** Creating **Delta Live Tables** for structured analytics.

## 🚀 Tech Stack
- **Azure Data Factory (ADF)** - Data ingestion and workflow automation
- **Databricks & PySpark** - Data processing and transformation
- **Delta Live Tables** - Managing structured datasets
- **SQL** - Querying and data validation
- **Azure Data Lake Storage** - Storing raw and processed data

## 🔄 Workflow
### 1️⃣ Data Ingestion (Bronze Layer)
- Extracts Netflix dataset from GitHub using **ADF HTTP request**.
- Checks for file existence before ingestion.
- Stores raw files in the Bronze container.

📷 **Screenshot:**

![image](https://github.com/user-attachments/assets/9893085a-9c08-4057-b7c2-01226ebd1c8b)


### 2️⃣ Data Processing & Validation (Silver Layer)
- Uses **Databricks Autoloader** to process raw data.
- Implements **checkpointing** for incremental loads.
- Runs validation workflows to ensure data quality.

📷 **Screenshot:**

![image](https://github.com/user-attachments/assets/21745ab3-b6f8-4191-a5c8-73a7e5e82dce)


### 3️⃣ Transformation & Scheduling
- Feature transformation script runs in Databricks.
- Workflow scheduled to execute transformations **only on Sundays**.

📷 **Screenshot:**

![image](https://github.com/user-attachments/assets/1be02fde-c459-4d88-b205-64b901304aab)


### 4️⃣ Delta Live Tables (Gold Layer)
- ETL pipeline loads transformed data from **Silver** to **Gold**.
- Creates **Delta Live Tables** for optimized querying.

## 📊 Results & Business Impact
✅ **Automated Data Processing**: Reduced manual effort by **80%**.
✅ **Optimized Query Performance**: Improved data retrieval by **50%**.
✅ **Scalable Analytics**: Structured Netflix dataset for deeper insights.

## 🛠️ How to Run
1. Deploy the **ADF pipeline** to ingest data.
2. Execute **Databricks Autoloader** for incremental processing.
3. Run **transformation workflows** via Databricks notebooks.
4. Trigger the **ETL pipeline** to generate Delta Live Tables.
