Overview
========

End-to-End ETL Pipeline with Airflow, dbt, and Google Cloud's BigQuery integration with Metabase for analytics

Project Contents
================

This project demonstrates an end-to-end ETL pipeline using Apache Airflow, dbt (Data Build Tool), and Google BigQuery, leveraging the online retail dataset from Kaggle. The pipeline is designed to perform the following tasks:

- Extract: Read data from a local file.
- Load: Transfer the extracted data to Google Cloud Storage (GCS) and load it into BigQuery.
- Data Quality Checks: Ensure data integrity and quality using Python-based checks.
- Transform: Utilize dbt to build and run SQL models that transform the data within the BigQuery dataset named "retail".
- Report: Implement dbt models to enable report generation in Metabase for business analytics and insights.

Project Structure
===========================
The project consists of an Apache Airflow Directed Acyclic Graph (DAG) with the following key components:

1. Local File to GCS: Extract data from a local file and load it into a Google Cloud Storage bucket.
2. GCS to BigQuery: Transfer data from Google Cloud Storage to a BigQuery dataset.
3. Data Quality Checks: Utilize SODA to perform quality checks on the data in BigQuery.


Installation
=================================
To get started with this project, you'll need to set up your environment with the following tools:

- Apache Airflow
- Apache Airflow is used to orchestrate the ETL pipeline. For installation instructions, refer to the [Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html).
- dbt (Data Build Tool)
dbt is used for transforming the data within BigQuery. You can find installation instructions in the [dbt documentation](https://docs.getdbt.com/docs/build/documentation).
- Google Cloud Platform (GCP) You'll need to have a Google Cloud project set up with the following components:
- Google Cloud Storage (GCS): For storing data files.
BigQuery: For querying and analyzing the data.
To get started with GCP, follow the [GCP Getting Started guide] (https://cloud.google.com/gcp/?hl=de&utm_source=google&utm_medium=cpc&utm_campaign=emea-de-all-de-bkws-all-all-trial-e-gcp-1707574&utm_content=text-ad-none-any-DEV_c-CRE_529379242747-ADGP_Hybrid+%7C+BKWS+-+EXA+%7C+Txt+-+GCP+-+General+-+v3-KWID_43700060393213364-kwd-6458750523-userloc_9043130&utm_term=KW_google%20cloud-NET_g-PLAC_&&gad_source=1&gclid=CjwKCAjwufq2BhAmEiwAnZqw8iAtk2q3eyQdrytn-Z14REzRsSd1fHfMZUBM71_Jh_GF2vzkOL74LxoCqV4QAvD_BwE&gclsrc=aw.ds).


- Python Programming Language for quality checks when transfering data from GCS bucket to Google big query studio [Python] (https://www.python.org/).

Usage
-----------------------

- Dataset
  [data](https://www.kaggle.com/datasets/tunguz/online-retail)
1. Start the Airflow web server and scheduler.
`docker-compose up`
2. Trigger the DAG
- Access the Airflow web interface at http://localhost:8080, and manually trigger the DAG to start the ETL process.
3. Monitor and Check
- Monitor the progress of the ETL pipeline in the Airflow web interface. Check the BigQuery dataset and table to ensure data has been loaded correctly, and review the quality check results.

dbt (Data Build Tool)
-----------------------
SQL models are created and run to clean, transform, and aggregate the data for downstream reporting and analysis.
Metabase: Metabase is used for visualizing the transformed data and generating reports that provide insights into the retail business's performance and trends.

The final, transformed datasets from dbt are connected to Metabase.
Dashboards and reports are built in Metabase, providing a user-friendly interface for analyzing the retail business's key metrics.

Some of the key reports include:
1. Top 10 products by quantity sold
2. Total Revenue per month
3. Primary Markets




