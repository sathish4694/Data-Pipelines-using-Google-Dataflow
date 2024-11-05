# Data-Pipelines-using-Google-Dataflow
This project demonstrates the creation of a data pipeline using Google Cloud Dataflow to extract, transform, and load data from Google Cloud Storage (GCS) into BigQuery. Additionally, it generates a CSV file with selected fields and stores it back into GCS. The code is written in Python and utilizes Apache Beam.

# Prerequisites
Before running the project, make sure you have the following set up:
A Google Cloud Platform (GCP) account
A GCP project with the following APIs enabled:
Dataflow API
BigQuery API
Cloud Storage API
# The Google Cloud SDK installed on your local system
Python and Apache Beam library installed locally or accessible through Cloud Shell
Setup Steps
Step 1: Create a Google Cloud Storage (GCS) Bucket
Open the Google Cloud Console.
Use the following gcloud command to create a GCS bucket:
gsutil mb -l <region> gs://<your-bucket-name>/
Upload your data files to the GCS bucket using the gsutil command:
gsutil cp <local-file-path> gs://<your-bucket-name>/
Step 2: Write and Configure the Dataflow Code
You can write your Dataflow code either in Cloud Shell or locally. The code performs the following operations:
The operations canbe perofrmed through vscode also
Extract and Load Data:
Reads data from JSON and CSV files in the GCS bucket.
Transformations:
Uppercase Transformation:
Convert company_name and country to uppercase.
Format industry to have capitalized words (e.g., "Management Consulting").
Data Cleaning:
Drop description and locationUrl fields.
Extract the maximum size from the size field.
Extract city and state from headquarters and drop headquarters.
If city and state are missing, attempt to extract from the location field.
Fill latest_news with "Not available" if it's empty.
Add Timestamp:
Add a load_date column with the current date in yyyy-mm-dd format.
Map CEO Information:
Join the main data with the CEO information from the CSV file using company_name.
Write Outputs:
Save the transformed data to BigQuery.
Write a CSV file with selected fields back to GCS.
Project Structure
bash

Running the Pipeline
Running in Vscode
Running Locally
Make sure you have Python and the Apache Beam library installed:
pip install apache-beam[gcp]
Configuration Details
Google Cloud Project Settings:
Update your project ID, region, and bucket name in the PipelineOptions in main.py.
BigQuery Table:
The transformed data will be loaded into a BigQuery table with a predefined schema.
Outputs
BigQuery Table:
The transformed data is loaded into a BigQuery table named suppliers_data.
<img width="887" alt="image" src="https://github.com/user-attachments/assets/1cf89dd7-5090-45fb-998a-d77162667792">

CSV File in GCS:
The CSV file containing company_name, ceo, country, state, city, and founded is saved in the GCS bucket.

Additional Notes
Ensure you have permissions to create and write to BigQuery tables and GCS buckets.
If running locally, configure your environment to authenticate with GCP using:
