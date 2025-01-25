import bitget.v1.mix.order_api as maxOrderApi
from bitget.bitget_api import BitgetApi
from bitget.exceptions import BitgetAPIException
from decouple import config
#from constants import SCALP_MARKETS

import time
import pandas as pd
import boto3
import os
import json
from datetime import datetime


def get_unix_times():
    """Get the Unix time for the last 24 hours."""
    current_unix_time = int(time.time() * 1000)  # Current time in milliseconds
    unix_time_minus_24h = current_unix_time - (24 * 60 * 60 * 1000)  # 24 hours back
    return current_unix_time, unix_time_minus_24h


def fetch_historical_trades(order_api, markets, start_time, end_time):
    """Fetch historical trades for each market."""
    orders_df = pd.DataFrame()
    for market in markets:
        params = {
            "symbol": f"{market}_UMCBL",
            "productType": "USDT-FUTURES",
            "startTime": start_time,
            "endTime": end_time,
            "pageSize": 20
        }
        try:
            response = order_api.ordersHistory(params)
            if 'data' in response and 'orderList' in response['data']:
                orders = response['data']['orderList']
                current_orders = pd.DataFrame(orders)
                orders_df = pd.concat([orders_df, current_orders], ignore_index=True)
            else:
                print(f"No data returned for market: {market}")
        except BitgetAPIException as e:
            print(f"Error fetching orders for market {market}: {e.message}")
    return orders_df


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
    # Get Unix times
   # current_unix_time, unix_time_minus_24h = get_unix_times()

    # credentials
    #secret_key = config('secretKey')
    #passphrase = config('passphrase')
    bucket_name = config('s3_bucket_name')

    # Initialize the API
    #order_api = maxOrderApi.OrderApi(api_key, secret_key, passphrase)


    # Process order responses
    parquet_file_path = f"/tmp/response_momentum_{datetime.now().strftime('%Y%m%d')}.parquet"
    s3_key_parquet = f"response/daily/response_{datetime.now().strftime('%Y%m%d')}.parquet"
    json_file_paths = {
        "momentum_order_responses_normalized.json": "normalized_slope_momentum",
        "momentum_order_responses_percentage.json": "percentage_slope_momentum"
    }
    process_order_responses(json_file_paths, "order_responses.parquet", bucket_name, "s3-key/order_responses.parquet")



if __name__ == "__main__":
    main()








