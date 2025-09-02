import pandas as pd
import json

from constants import TRADE_SIZE, WINDOW, NUM_STD, TRADING_STRATEGIES
import bitget.v1.mix.order_api as maxOrderApi
from bitget.bitget_api import BitgetApi
from bitget.exceptions import BitgetAPIException
from decouple import config

apiKey = config('apiKey')
secretKey = config('secretKey')
passphrase = config('passphrase')

baseApi = BitgetApi(apiKey, secretKey, passphrase)

# Function to log the order response
def log_order_response(response, file_path):

    try:
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
        except FileNotFoundError:
            data = []

        data.append(response)

        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)
        print(f"Order logged successfully: {response['data']['orderId']}")
    except Exception as e:
        print(f"Failed to log order: {e}")


def calculate_bollinger_bands(price_series, window, num_std):
    """
    Calculate Bollinger Bands for a given price series.
    
    Args:
        price_series: pandas Series of prices
        window: rolling window size for the moving average
        num_std: number of standard deviations for the bands
    
    Returns:
        tuple: (upper_band, middle_band, lower_band)
    """
    middle_band = price_series.rolling(window=window).mean()
    std = price_series.rolling(window=window).std()
    upper_band = middle_band + (std * num_std)
    lower_band = middle_band - (std * num_std)
    
    return upper_band, middle_band, lower_band


def manage_trade(price_data_file):

    price_data = pd.read_csv(price_data_file)

    try:
        with open('open_trades.json', 'r') as json_file:
            open_trades = json.load(json_file)
        print(f'Open positions loaded: {open_trades}')
    except FileNotFoundError:
        open_trades = {}
        print('No open positions found, starting fresh')

    keys_to_remove = []

    for market in TRADING_STRATEGIES.keys():

        price_series = price_data[market]
        upper_band, middle_band, lower_band = calculate_bollinger_bands(price_series, WINDOW, NUM_STD)
        
        current_price = price_series.iloc[-1]
        current_upper = upper_band.iloc[-1]
        current_middle = middle_band.iloc[-1]
        current_lower = lower_band.iloc[-1]

        key_to_remove = None
        strategy = TRADING_STRATEGIES.get(market, "both")

        if market not in open_trades:
            # "Long" strategy: Close short at market when price touches lower band (oversold)
            # Then open short limit at SMA band (middle band)
            if strategy in ["long", "both"]:
                if current_price <= current_lower:
                    # Close short position at market (this is our "long" entry)
                    enter_market_trade(market, "close_short", price_data, open_trades)
                    # Open short limit at SMA band (middle band)
                    enter_limit_trade(market, "open_short", price_data, current_middle)
            
            # "Short" strategy: Open short at market when price touches upper band (overbought)
            # Then close short limit at SMA band (middle band)
            if strategy in ["short", "both"]:
                if current_price >= current_upper:
                    # Open short position at market
                    enter_market_trade(market, "open_short", price_data, open_trades)
                    # Close short limit at SMA band (middle band)
                    enter_limit_trade(market, "close_short", price_data, current_middle)

        elif market in open_trades:
            position_type = open_trades[market]['position_type']
            
            # Exit conditions based on position type
            if position_type == "open_short":
                # Close short position when price reaches middle band (SMA)
                if current_price <= current_middle:
                    key_to_remove = market
            elif position_type == "close_short":
                # This represents a "long" position that was entered by closing a short
                # Exit when price reaches middle band or upper band
                if current_price >= current_middle or current_price >= current_upper:
                    key_to_remove = market

            if key_to_remove is not None:
                keys_to_remove.append(key_to_remove)

    for key in keys_to_remove:
        del open_trades[key]

    with open('open_trades.json', 'w') as json_file:
        json.dump(open_trades, json_file, indent=4)


def enter_market_trade(market, position_type, price_data, open_trades):
    
    asset_latest_price = price_data[market].iloc[-1]

    asset_position_size = round(TRADE_SIZE / asset_latest_price, 2)
    
    if position_type == "close_short":
        print(f"Closing short position (long entry) on: {market}")
        market_params = {
            "symbol": f"{market}_UMCBL",
            "marginCoin": "USDT",
            "side": "close_short",
            "orderType": "market",
            "size": round(asset_position_size, 2),
            "timeInForceValue": "normal"
        }
    elif position_type == "open_short":
        print(f"Opening short position on: {market}")
        market_params = {
            "symbol": f"{market}_UMCBL",
            "marginCoin": "USDT",
            "side": "open_short",
            "orderType": "market",
            "size": round(asset_position_size, 2),
            "timeInForceValue": "normal"
        }
    
    # Initialize the API and define the order logging file.
    order_api = maxOrderApi.OrderApi(apiKey, secretKey, passphrase)
    ORDER_FILE = "order_responses.json"
    
    # Place the market order.
    try:
        response_market = order_api.placeOrder(market_params)
        print("Market order response:", response_market)
        if response_market['code'] == '00000':
            log_order_response(response_market, ORDER_FILE)
        else:
            print(f"Market order failed: {response_market['msg']}")
    except BitgetAPIException as e:
        print("Market order API exception:", e.message)

    open_trades[f"{market}"] = {
        "position_type": position_type,
        "base_position_size": asset_position_size
    }


def enter_limit_trade(market, position_type, price_data, limit_price):

    asset_latest_price = price_data[market].iloc[-1]

    asset_position_size = round(TRADE_SIZE / asset_latest_price, 2)

    if position_type == "open_short":
        print(f"Opening short limit trade on: {market} at {limit_price}")
        params = {
            "symbol": f"{market}_UMCBL",
            "marginCoin": "USDT",
            "side": "open_short",
            "orderType": "limit",
            "size": round(asset_position_size, 2),
            "price": limit_price,
            "timeInForceValue": "normal"
        }

    elif position_type == "close_short":
        print(f"Closing short limit trade on: {market} at {limit_price}")
        params = {
            "symbol": f"{market}_UMCBL",
            "marginCoin": "USDT",
            "side": "close_short",
            "orderType": "limit",
            "size": round(asset_position_size, 2),
            "price": limit_price,
            "timeInForceValue": "normal"
        }

    # Execute the trades
    order_api = maxOrderApi.OrderApi(apiKey, secretKey, passphrase)
    # File to store order responses
    ORDER_FILE = "order_responses.json"
    try:
        response_base = order_api.placeOrder(params)
        print(response_base)
        # Check if the response is successful
        if response_base['code'] == '00000':
            log_order_response(response_base, ORDER_FILE)
        else:
            error_message = f"Order failed: {response_base['msg']}"
            print(error_message)
    except BitgetAPIException as e:
        print("error:" + e.message)