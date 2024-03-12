from google.cloud import storage
from google.oauth2 import service_account
import io
import utils.config as config


def upload_to_gcp_bucket(data, destination_blob_name, file_type):

    # Initialize a client with explicit credentials
    storage_client = storage.Client.from_service_account_json(config.key_path)

    # Get the bucket
    bucket = storage_client.get_bucket(config.gcs_bucket)

    # Create a blob (file) in the bucket
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(data, file_type)

    print(f"Data uploaded to {config.gcs_bucket}/{destination_blob_name}")


def check_if_file_exists(blob_name):
    # Initialize a client with explicit credentials
    storage_client = storage.Client.from_service_account_json(config.key_path)

    # Get the bucket
    bucket = storage_client.get_bucket(config.gcs_bucket)

    blob = bucket.blob(blob_name)

    return blob.exists()


def get_file(blob_name):
    storage_client = storage.Client.from_service_account_json(config.key_path)

    # Get the bucket
    bucket = storage_client.get_bucket(config.gcs_bucket)

    blob = bucket.blob(blob_name)

    with blob.open("r", encoding="ISO-8859-1") as f:
        return io.StringIO(f.read())


def list_files(path):
    storage_client = storage.Client.from_service_account_json(config.key_path)

    # Get the bucket
    bucket = storage_client.get_bucket(config.gcs_bucket)

    # List all blobs (files) in the bucket
    return list(bucket.list_blobs(prefix=path))


def delete_delta_data(prefix):
    # Instantiate a client
    storage_client = storage.Client.from_service_account_json(config.key_path)

    # Get the bucket
    bucket = storage_client.get_bucket(config.gcs_bucket)

    # List the objects in the bucket with the specified prefix
    blobs = bucket.list_blobs(prefix=prefix)

    # Delete each blob in the bucket
    for blob in blobs:
        blob.delete()
        print(f"Deleted: {blob.name}")
