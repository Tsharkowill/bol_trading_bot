import os
import pandas as pd
import json
import boto3
from datetime import datetime
from decouple import config

def upload_to_s3(local_file, bucket_name, s3_key):
    """Upload a file to S3."""
    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(local_file, bucket_name, s3_key)
        print(f"File uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def process_order_responses(json_file_path, parquet_file_path, bucket_name, s3_key_parquet):
    """Process order responses from a JSON file and upload to S3."""
    try:
        if not os.path.exists(json_file_path):
            raise FileNotFoundError(f"{json_file_path} does not exist.")

        with open(json_file_path, "r") as f:
            responses = json.load(f)

        df = pd.DataFrame([resp['data'] for resp in responses])
        df['strategy'] = 'mean_reversion'

        df.to_parquet(parquet_file_path, index=False)
        print(f"Parquet file saved to: {parquet_file_path}")

        upload_to_s3(parquet_file_path, bucket_name, s3_key_parquet)
        os.remove(json_file_path)
        print(f"JSON file deleted: {json_file_path}")
    except FileNotFoundError as fnf_error:
        print(f"Error: {fnf_error}")
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    # S3 bucket name from environment variables
    bucket_name = config('s3_bucket_name')

    # Parquet file paths
    parquet_file_path = f"/tmp/response_momentum_{datetime.now().strftime('%Y%m%d')}.parquet"
    s3_key_parquet = f"response/daily/response_momentum_{datetime.now().strftime('%Y%m%d')}.parquet"
    json_file_path = "order_responses.json"

    # Process and upload responses
    process_order_responses(json_file_path, parquet_file_path, bucket_name, s3_key_parquet)

if __name__ == "__main__":
    main()








