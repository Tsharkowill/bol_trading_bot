import pandas as pd

from get_time import get_unix_times
from get_markets import fetch_and_compile_candle_data
from momentum import manage_trade
from constants import MARKETS, EMA_ENTRY_THRESHOLD_HIGH, EMA_ENTRY_THRESHOLD_MEDIUM, WINDOW_HIGH, WINDOW_MEDIUM


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
    fetch_and_compile_candle_data(times_dict, MARKETS, '15m')
except Exception as e:
    print(f"Error fetching market data: {e}")

try:
    manage_trade('data_15m.csv', MARKETS, "high", EMA_ENTRY_THRESHOLD_HIGH, WINDOW_HIGH)
except Exception as e:
    print(f"Error managing scalps: {e}")

try:
    manage_trade('data_15m.csv', MARKETS, "medium", EMA_ENTRY_THRESHOLD_MEDIUM, WINDOW_MEDIUM)
except Exception as e:
    print(f"Error managing scalps: {e}")