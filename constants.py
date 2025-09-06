TRADE_SIZE = 200
WINDOW = 200  # Rolling window for Bollinger Bands calculation
NUM_STD = 2  # Number of standard deviations for Bollinger Bands

# Strategy preferences for each asset: "long", "short", or "both"
TRADING_STRATEGIES = {
    "ETHUSDT": "long",
    "SOLUSDT": "long", 
    "INJUSDT": "short",
    "NEARUSDT": "short",
    "AAVEUSDT": "both",
    "LTCUSDT": "short",
    "LINKUSDT": "short",
    "ENSUSDT": "short",
    "JUPUSDT": "both",
    "LDOUSDT": "both",
    "INJUSDT": "both",
    "HYPEUSDT": "long",
    "BCHUSDT": "long",
    "BTCUSDT": "long",
    "UNIUSDT": "short",
    "SUIUSDT": "both",
    "DOGEUSDT": "short"
}