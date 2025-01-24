import pandas as pd

from get_time import get_unix_times
from get_markets import fetch_and_compile_candle_data
from momentum import manage_normalized_slope_trades, manage_percentage_slope_trades
from constants import MOMENTUM_MARKETS, NORMALIZED_ENTRY_THRESHOLD, NORMALIZED_EXIT_THRESHOLD, NORMALIZED_WINDOW, PERCENTAGE_ENTRY_THRESHOLD, PERCENTAGE_EXIT_THRESHOLD, PERCENTAGE_WINDOW


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
    fetch_and_compile_candle_data(times_dict, MOMENTUM_MARKETS, '15m')
except Exception as e:
    print(f"Error fetching market data: {e}")

try:
    manage_normalized_slope_trades('data_15m.csv', MOMENTUM_MARKETS, '15m', NORMALIZED_ENTRY_THRESHOLD, NORMALIZED_EXIT_THRESHOLD, NORMALIZED_WINDOW)
except Exception as e:
    print(f"Error managing scalps: {e}")

try:
    manage_percentage_slope_trades('data_15m.csv', MOMENTUM_MARKETS, '15m', PERCENTAGE_ENTRY_THRESHOLD, PERCENTAGE_EXIT_THRESHOLD, PERCENTAGE_WINDOW)
except Exception as e:
    print(f"Error managing scalps: {e}")