import pandas as pd

from market_data import get_unix_times, fetch_and_compile_candle_data
from bollinger import manage_trade
from constants import TRADING_STRATEGIES


from bitget.bitget_api import BitgetApi
from decouple import config

'''Create instance of Api'''


apiKey = config('apiKey')
secretKey = config('secretKey')
passphrase = config('passphrase')


baseApi = BitgetApi(apiKey, secretKey, passphrase)




# Create dictionary for requesting market data
times_dict = get_unix_times(3)

# Get market prices and create a .csv for selected markets
try:
    # Get markets from TRADING_STRATEGIES keys
    markets = list(TRADING_STRATEGIES.keys())
    fetch_and_compile_candle_data(times_dict, markets, '15m')
    print(f"Market data fetched for: {markets}")
except Exception as e:
    print(f"Error fetching market data: {e}")

# Execute the Bollinger Bands trading strategy
try:
    manage_trade('data_15m.csv')
    print("Trading strategy executed successfully")
except Exception as e:
    print(f"Error executing trading strategy: {e}")