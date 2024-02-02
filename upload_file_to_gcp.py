from google.cloud import storage
from google.oauth2 import service_account

# The name of bucket from GCP
bucket_name = 'compras-bucket'

# Specify the path to your service account key file
key_path = 'gcp_key_compras_bucket.json'


def upload_to_gcp_bucket(data, destination_blob_name):

    # Initialize a client with explicit credentials
    storage_client = storage.Client.from_service_account_json(key_path)

    # Get the bucket
    bucket = storage_client.get_bucket(bucket_name)

    # Create a blob (file) in the bucket
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(data, 'text/csv')

    print(f"Data uploaded to {bucket_name}/{destination_blob_name}")


def check_if_file_exists(blob_name):
    # Initialize a client with explicit credentials
    storage_client = storage.Client.from_service_account_json(key_path)

    # Get the bucket
    bucket = storage_client.get_bucket(bucket_name)

    blob = bucket.blob(blob_name)

    return blob.exists()