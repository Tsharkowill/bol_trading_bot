from datetime import datetime, timedelta
import pandas as pd
import time

from bitget.bitget_api import BitgetApi
from bitget.exceptions import BitgetAPIException
from decouple import config

# API credentials
apiKey = config('apiKey')
secretKey = config('secretKey')
passphrase = config('passphrase')

# Create an instance of the BitgetApi class
baseApi = BitgetApi(apiKey, secretKey, passphrase)


def to_unix_milliseconds_rounded(dt):
    """
    Helper function to convert datetime to Unix timestamp in milliseconds 
    and round down to the nearest 15 minutes
    """
    # Calculate the number of minutes to subtract to round down to the nearest 15 minutes
    minutes_to_subtract = dt.minute % 15
    # Round down to the nearest 15 minutes
    dt_rounded = dt.replace(minute=dt.minute - minutes_to_subtract, second=0, microsecond=0)
    return int(dt_rounded.timestamp() * 1000)


def get_unix_times(steps):
    """
    Generate time ranges for data fetching.
    
    Args:
        steps (int): Number of time ranges to generate
        
    Returns:
        dict: Dictionary with time ranges, each covering 200 intervals of 15 minutes
    """
    # Get current datetime and round down to the nearest 15 minutes
    current_time = datetime.now()
    current_time_rounded = current_time - timedelta(
        minutes=current_time.minute % 15, 
        seconds=current_time.second, 
        microseconds=current_time.microsecond
    )
    
    # Initialize the dictionary to store time ranges
    times_dict = {}
    intervals_step = 200  # 200 intervals of 15 minutes each = 50 hours per range
    
    # Generate sequential time ranges
    for i in range(1, steps):
        end_time = current_time_rounded - timedelta(minutes=(i-1) * intervals_step * 15)
        start_time = current_time_rounded - timedelta(minutes=i * intervals_step * 15)
        
        times_dict[f"range_{i}"] = {
            "from_unix": to_unix_milliseconds_rounded(start_time),
            "to_unix": to_unix_milliseconds_rounded(end_time),
        }
    
    return times_dict


def fetch_and_compile_candle_data(times_dict, markets, granularity):
    """
    Fetch historical candle data for multiple markets and compile into a single CSV.
    
    Args:
        times_dict (dict): Time ranges for data fetching
        markets (list): List of market symbols to fetch
        granularity (str): Time granularity (e.g., '15m', '1h')
    """
    try:
        final_df = pd.DataFrame()
        
        for market in markets:
            interim_df = pd.DataFrame()  # Reset interim_df for each market
            
            for times_key, times_value in times_dict.items():
                params = {
                    "symbol": market,
                    "productType": "USDT-FUTURES",
                    "granularity": granularity,
                    "endTime": times_value["to_unix"],
                    "limit": "200"
                }
                response = baseApi.get("/api/v2/mix/market/history-candles", params)
            
                # Temporary DataFrame from the response
                temp_df = pd.DataFrame(response)
                
                # Process the 'time' column
                temp_df['time'] = temp_df['data'].apply(lambda x: x[0])
                temp_df['time'] = pd.to_numeric(temp_df['time'])
                temp_df['time'] = pd.to_datetime(temp_df['time'], unit='ms')

                # Append the data for the current market
                interim_df = pd.concat([interim_df, temp_df], ignore_index=True, axis=0)
                
                # Create a new column for the market using the exit price (index 4)
            final_df[market] = interim_df['data'].apply(lambda x: x[4])

            # Sleep to avoid hitting the rate limit
            time.sleep(0.2)  
            
            # Ensure the 'time' column is synchronized across all market columns
            if 'time' not in final_df.columns:
                final_df['time'] = interim_df['time']

        # Sort the times in the DataFrame in ascending order
        final_df.sort_values(by='time', inplace=True)

        # Reorder the DataFrame columns to have 'time' as the first column
        cols = ['time'] + [col for col in final_df.columns if col != 'time']
        df_market_prices = final_df[cols]
        
        # Export the compiled data to a CSV file
        output_filename = f"data_{granularity}.csv"
        df_market_prices.to_csv(output_filename, index=False)
        print(f"Data saved to {output_filename}")

    except BitgetAPIException as e:
        print(f"API error: {e.message}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
