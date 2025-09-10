# -*- coding: utf-8 -*-
"""
Shared configuration
"""
import os

API_KEY = ""

MAX_THREADS = os.cpu_count()
ATM_STRIKE_PRICE_SETTING = 5
TARGETS_DAYS = [7, 30, 45, 75, 90]

DATA_LOCATE = {
    # Option 1
    'StockList': [],

    # Option 2: Only work if StockList is empty.
    'DATA_STOCKS_CSV': './SnP500List.csv',

}

OUTPUT_DIR = "expiry_data"
TICKER_DIR = "../../option_tickers"
ATM_TICKER_DIR = "../../option_chain_ATM_tickers"

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_PASSWORD = None
