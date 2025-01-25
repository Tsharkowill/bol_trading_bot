import os
import pandas as pd
import json
import boto3
from datetime import datetime

def upload_to_s3(local_file, bucket_name, s3_key):
    """Upload a file to S3."""
    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(local_file, bucket_name, s3_key)
        print(f"File uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def process_order_responses(json_file_paths, parquet_file_path, bucket_name, s3_key_parquet):
    """Process order responses from multiple JSON files and upload to S3."""
    try:
        all_dataframes = []

        for json_file_path, strategy_name in json_file_paths.items():
            if not os.path.exists(json_file_path):
                print(f"Warning: {json_file_path} does not exist. Skipping...")
                continue

            with open(json_file_path, "r") as f:
                responses = json.load(f)

            df = pd.DataFrame([resp['data'] for resp in responses])
            df['strategy'] = strategy_name
            all_dataframes.append(df)

            os.remove(json_file_path)
            print(f"JSON file deleted: {json_file_path}")

        if not all_dataframes:
            print("No data to process. Exiting...")
            return

        combined_df = pd.concat(all_dataframes, ignore_index=True)

        combined_df.to_parquet(parquet_file_path, index=False)
        print(f"Parquet file saved to: {parquet_file_path}")

        upload_to_s3(parquet_file_path, bucket_name, s3_key_parquet)
    except FileNotFoundError as fnf_error:
        print(f"Error: {fnf_error}")
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    # S3 bucket name from environment variables
    bucket_name = os.getenv('S3_BUCKET_NAME')

    # Parquet file paths
    parquet_file_path = f"/tmp/response_momentum_{datetime.now().strftime('%Y%m%d')}.parquet"
    s3_key_parquet = f"response/daily/response_momentum_{datetime.now().strftime('%Y%m%d')}.parquet"

    # JSON files and their corresponding strategies
    json_file_paths = {
        "momentum_order_responses_normalized.json": "normalized_slope_momentum",
        "momentum_order_responses_percentage.json": "percentage_slope_momentum"
    }

    # Process and upload responses
    process_order_responses(json_file_paths, parquet_file_path, bucket_name, s3_key_parquet)

if __name__ == "__main__":
    main()








