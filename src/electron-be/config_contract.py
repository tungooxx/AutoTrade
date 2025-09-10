# -*- coding: utf-8 -*-
"""
Shared configuration
"""
import os

API_KEY = ""
MAX_THREADS = os.cpu_count()

OUTPUT_DIR = "expiry_data"
TICKER_DIR = "../../option_tickers"
ATM_TICKER_DIR = "../../option_chain_ATM_tickers"

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_PASSWORD = None
