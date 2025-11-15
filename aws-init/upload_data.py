import boto3
import os
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def upload_directory_to_s3(local_directory, bucket, s3_prefix):
    """
    Uploads the contents of a directory to an S3 bucket.

    :param local_directory: Path to the local directory to upload.
    :param bucket: The S3 bucket to upload to.
    :param s3_prefix: The prefix (folder) in the S3 bucket.
    """
    # endpoint_url = "http://localstack:4566" # <-- This line is commented out
    # Initialize S3 client without the endpoint_url to use default AWS endpoints
    s3_client = boto3.client("s3")

    logging.info(f"Starting upload from '{local_directory}' to 's3://{bucket}/{s3_prefix}'...") #

    if not os.path.isdir(local_directory): #
        logging.error(f"Source directory '{local_directory}' not found.") #
        return #

    for root, _, files in os.walk(local_directory): #
        for filename in files: #
            local_path = os.path.join(root, filename) #
            # Create a relative path to maintain the folder structure in S3
            relative_path = os.path.relpath(local_path, local_directory) #
            s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/") #

            try:
                logging.info(f"Uploading {local_path} to {s3_key}...") #
                s3_client.upload_file(local_path, bucket, s3_key) #
            except Exception as e: #
                logging.error(f"Failed to upload {local_path}: {e}") #

    logging.info("Upload complete.") #

if __name__ == "__main__": #
    if len(sys.argv) != 3: #
        print("Usage: python upload_data.py <local_directory_path> <s3_prefix>") #
        sys.exit(1) #

    source_dir = sys.argv[1] #
    destination_prefix = sys.argv[2] #
    # Use the bucket name defined in your CloudFormation or init_aws.py
    bucket_name = "insat-cog-processed-production" # Adjusted bucket name

    upload_directory_to_s3(source_dir, bucket_name, destination_prefix) #