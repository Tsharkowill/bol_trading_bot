import pandas as pd
import json

from constants import MOMENTUM_TRADE_SIZE
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


def calculate_normalized_slope(price_series, window):
    """
    Calculate the normalized slope of the price series.

    Parameters:
    - price_series (Series): Asset price series.
    - window (int): Window size for slope calculation.

    Returns:
    - Series: Normalized slope series.
    """
    moving_avg = price_series.rolling(window=window).mean()
    slope = (moving_avg.diff(window) / moving_avg.shift(1)) * 100
    min_slope = slope.rolling(window=window, min_periods=1).min()
    max_slope = slope.rolling(window=window, min_periods=1).max()

    denom = max_slope - min_slope
    denom = denom.replace(0, 1)

    normalized_slope = 2 * (slope - min_slope) / denom - 1
    return normalized_slope.fillna(0)


def calculate_percentage_slope(price_series, window):
    """
    Calculate the percentage slope of the price series.

    Parameters:
    - price_series (Series): Asset price series.
    - window (int): Window size for slope calculation.

    Returns:
    - Series: Percentage slope series.
    """
    moving_avg = price_series.rolling(window=window).mean()
    slope = (moving_avg.diff(1) / moving_avg.shift(1)) * 100
    return slope.fillna(0)


def manage_normalized_slope_trades(price_data_file, MARKETS, cadence, NORMALIZED_ENTRY_THRESHOLD, NORMALIZED_EXIT_THRESHOLD, NORMALIZED_WINDOW):

    price_data = pd.read_csv(price_data_file)

    try:
        with open(f'open_normalized_slope_trades_{cadence}.json', 'r') as json_file:
            open_trades = json.load(json_file)
        print(f'Open positions loaded: {open_trades}')
    except FileNotFoundError:
        open_trades = {}
        print('No open positions found, starting fresh')

    keys_to_remove = []

    for market in MARKETS:

        price_series = price_data[market]
        normalized_slope = calculate_normalized_slope(price_series, NORMALIZED_WINDOW)
        price_data[f'normalized_slope_{market}'] = normalized_slope

        current_normalized_slope = normalized_slope.iloc[-1]
        print(f"Normalized Slope: {current_normalized_slope}")

        key_to_remove = None

        if market not in open_trades:
            if current_normalized_slope >= NORMALIZED_ENTRY_THRESHOLD:
                enter_momentum_trade(market, "long", price_data, open_trades, "normalized")
            elif current_normalized_slope <= -NORMALIZED_ENTRY_THRESHOLD:
                enter_momentum_trade(market, "short", price_data, open_trades, "normalized")

        elif market in open_trades:
            position_type = open_trades[market]['position_type']
            if position_type == "long" and current_normalized_slope <= NORMALIZED_EXIT_THRESHOLD:
                exit_momentum_trade(market, "long", open_trades, "normalized")
                key_to_remove = market

            elif position_type == "short" and current_normalized_slope >= -NORMALIZED_EXIT_THRESHOLD:
                exit_momentum_trade(market, "short", open_trades, "normalized")
                key_to_remove = market

            if key_to_remove is not None:
                keys_to_remove.append(key_to_remove)

    for key in keys_to_remove:
        del open_trades[key]

    with open(f'open_normalized_slope_trades_{cadence}.json', 'w') as json_file:
        json.dump(open_trades, json_file, indent=4)


def manage_percentage_slope_trades(price_data_file, MARKETS, cadence, PERCENTAGE_ENTRY_THRESHOLD, PERCENTAGE_EXIT_THRESHOLD, PERCENTAGE_WINDOW):

    price_data = pd.read_csv(price_data_file)

    try:
        with open(f'open_percentage_slope_trades_{cadence}.json', 'r') as json_file:
            open_trades = json.load(json_file)
        print(f'Open positions loaded: {open_trades}')
    except FileNotFoundError:
        open_trades = {}
        print('No open positions found, starting fresh')

    keys_to_remove = []

    for market in MARKETS:

        price_series = price_data[market]
        percentage_slope = calculate_percentage_slope(price_series, PERCENTAGE_WINDOW)
        price_data[f'percentage_slope_{market}'] = percentage_slope

        current_percentage_slope = percentage_slope.iloc[-1]
        print(f"Percentage Slope: {current_percentage_slope}")

        key_to_remove = None

        if market not in open_trades:
            if current_percentage_slope >= PERCENTAGE_ENTRY_THRESHOLD:
                enter_momentum_trade(market, "long", price_data, open_trades, "percentage")
            elif current_percentage_slope <= -PERCENTAGE_ENTRY_THRESHOLD:
                enter_momentum_trade(market, "short", price_data, open_trades, "percentage")

        elif market in open_trades:
            position_type = open_trades[market]['position_type']
            if position_type == "long" and current_percentage_slope <= PERCENTAGE_EXIT_THRESHOLD:
                exit_momentum_trade(market, "long", open_trades, "percentage")
                key_to_remove = market

            elif position_type == "short" and current_percentage_slope >= -PERCENTAGE_EXIT_THRESHOLD:
                exit_momentum_trade(market, "short", open_trades, "percentage")
                key_to_remove = market

            if key_to_remove is not None:
                keys_to_remove.append(key_to_remove)

    for key in keys_to_remove:
        del open_trades[key]

    with open(f'open_percentage_slope_trades_{cadence}.json', 'w') as json_file:
        json.dump(open_trades, json_file, indent=4)


def enter_momentum_trade(market, position_type, price_data, open_trades, strategy):

    asset_latest_price = price_data[market].iloc[-1]

    asset_position_size = round(MOMENTUM_TRADE_SIZE / asset_latest_price, 2)

    if position_type == "long":
        print(f"Opening long momentum trade on: {market} using {strategy} slope")
        params = {
            "symbol": f"{market}_UMCBL",
            "marginCoin": "USDT",
            "side": "open_long",
            "orderType": "market",
            "size": asset_position_size,
            "timeInForceValue": "normal"
        }

    elif position_type == "short":
        print(f"Opening short momentum trade on: {market} using {strategy} slope")
        params = {
            "symbol": f"{market}_UMCBL",
            "marginCoin": "USDT",
            "side": "open_short",
            "orderType": "market",
            "size": asset_position_size,
            "timeInForceValue": "normal"
        }

    order_api = maxOrderApi.OrderApi(apiKey, secretKey, passphrase)
    ORDER_FILE = f"momentum_order_responses_{strategy}.json"
    try:
        response_base = order_api.placeOrder(params)
        print(response_base)
        if response_base['code'] == '00000':
            log_order_response(response_base, ORDER_FILE)
        else:
            error_message = f"Order failed: {response_base['msg']}"
            print(error_message)
    except BitgetAPIException as e:
        print("error:" + e.message)

    open_trades[f"{market}"] = {
        "position_type": position_type,
        "base_position_size": asset_position_size
    }


def exit_momentum_trade(market, position_type, open_trades, strategy):

    asset_position_size = open_trades[market]['base_position_size']

    if position_type == "long":
        print(f"Closing long momentum trade on: {market} using {strategy} slope")
        params = {
            "symbol": f"{market}_UMCBL",
            "marginCoin": "USDT",
            "side": "close_long",
            "orderType": "market",
            "size": asset_position_size,
            "timeInForceValue": "normal"
        }

    elif position_type == "short":
        print(f"Closing short momentum trade on: {market} using {strategy} slope")
        params = {
            "symbol": f"{market}_UMCBL",
            "marginCoin": "USDT",
            "side": "close_short",
            "orderType": "market",
            "size": asset_position_size,
            "timeInForceValue": "normal"
        }

    order_api = maxOrderApi.OrderApi(apiKey, secretKey, passphrase)
    ORDER_FILE = f"momentum_order_responses_{strategy}.json"
    try:
        response_base = order_api.placeOrder(params)
        print(response_base)
        if response_base['code'] == '00000':
            log_order_response(response_base, ORDER_FILE)
        else:
            error_message = f"Order failed: {response_base['msg']}"
            print(error_message)
    except BitgetAPIException as e:
        print("error:" + e.message)
