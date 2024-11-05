import os
from google.cloud import storage

# Set the path to your service account key
service_account_key_path = r'D:\Miraclesoft\ETL\gcpprofessionaldataengineer-f37028d44b80.json'

# Set the environment variable for the Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_key_path

def upload_to_gcs(bucket_name, files_to_upload):
    """
    Uploads multiple files to a Google Cloud Storage bucket.

    Parameters:
        bucket_name (str): The name of the GCS bucket.
        files_to_upload (list): A list of dictionaries containing 'source_file_name' and 'destination_blob_name'.
    """
    # Initialize a Google Cloud Storage client
    storage_client = storage.Client()

    # Get the bucket object
    bucket = storage_client.bucket(bucket_name)

    # Loop through each file in the list and upload it
    for file in files_to_upload:
        source_file_name = file['source_file_name']
        destination_blob_name = file['destination_blob_name']
        
        # Get a blob object for the destination in GCS
        blob = bucket.blob(destination_blob_name)
        
        # Upload the file to GCS
        blob.upload_from_filename(source_file_name)
        print(f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}.")

# Define the GCS bucket and file paths
bucket_name = 'supplier_data_miracle'

# Define the files and their respective GCS destination names
files_to_upload = [
    {'source_file_name': 'sample_suppliers_data.json', 'destination_blob_name': 'sample_suppliers_data.json'},
    {'source_file_name': 'company_ceo_data.csv', 'destination_blob_name': 'company_ceo_data.csv'}
]

# Upload each file to GCS
upload_to_gcs(bucket_name, files_to_upload)
