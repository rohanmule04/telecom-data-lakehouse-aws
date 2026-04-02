# telecom-data-lakehouse-aws
This project demonstrates the design and implementation of a Telecom Data Lakehouse built on AWS using AWS Glue and PySpark. It follows the Medallion Architecture (Bronze, Silver, Gold layers) to ensure high data quality, consistency, and efficient analytics.

The pipeline extracts data from RDBMS source systems, processes it through multiple transformation layers, and delivers curated datasets for analytics and reporting.

Architecture
🔹 Bronze Layer (Raw Data)
Acts as the landing zone
Stores raw data ingested directly from source systems
No transformations applied
Data stored in Amazon S3

🔹Silver Layer (Cleaned Data)
Performs data cleansing, deduplication, and standardization
Resolves data quality issues
Applies transformation logic using PySpark (AWS Glue)

🔹 Gold Layer (Business Layer)
Contains aggregated and enriched data
Implements business logic and derived metrics
Optimized for analytics, reporting, and dashboa

🔄 Data Flow
Extract data from RDBMS systems
Load raw data into Bronze layer (S3)
Transform and clean data into Silver layer
Aggregate and enrich data into Gold layer
Query data using Amazon Athena / Amazon Redshift

⚙️Technologies Used
Amazon S3 → Data Lake storage
AWS Glue (PySpark) → ETL processing
AWS Glue Crawlers → Metadata management (Glue Data Catalog)
Amazon Athena → Querying data
Amazon Redshift → Data warehousing
AWS CloudWatch → Monitoring and logging

🔑 Key Features
Scalable ETL pipeline for telecom data
Implementation of Medallion Architecture
Automated metadata updates using Glue Crawlers
Efficient data transformations using PySpark
End-to-end pipeline monitoring with CloudWatch
Supports analytics and reporting use cases

📊 Use Cases
Customer analytics
Call Detail Record (CDR) analysis
Revenue and billing insights
Network usage analysis

🧠 Key Learnings
Building scalable data pipelines using AWS Glue
Implementing Medallion Architecture in real-world scenarios
Handling large-scale telecom datasets
Data modeling and performance optimization

This project showcases a robust, scalable, and production-ready Telecom Data Lakehouse solution using AWS services, enabling efficient data processing and advanced analytics.
