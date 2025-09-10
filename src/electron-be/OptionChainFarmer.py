# -*- coding: utf-8 -*-
"""
Fetches and saves individual option contracts
"""
import os
import pandas as pd
from loguru import logger
from utils.api_client import init_pool_worker, fetch_reference_option, fetch_redis
from utils.storage import save_to_redis
from utils.data_processing import find_reference_target_expiries, get_ATM, get_top_bottom_strikes
from config_chain import MAX_THREADS, ATM_TICKER_DIR, DATA_LOCATE, ATM_STRIKE_PRICE_SETTING, TARGETS_DAYS, API_KEY
import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import re
import redis
from tqdm import tqdm
from multiprocessing import Pool, get_context
import multiprocessing as mp

import json

# Logger setup
os.makedirs("../../logs", exist_ok=True)
logger.remove()
logger.add(
    os.path.join("../../logs", "chain_contracts_warning.log"),
    level="WARNING",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    mode="a",
    enqueue=True
)
logger.add(
    os.path.join("../../logs", "chain_contracts_full.log"),
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    mode="a",
    enqueue=True
)


def process_option_contract_data(data, ticker, underlying_price):
    """
   Process raw option chain data to extract key information

   Args:
        data (dict): Raw option chain data from Polygon.io
        ticker (str): Stock symbol (passed to expiry_dates)
        option type (str): must be 'call' or 'put'
   Returns:
       tuple: (underlying_price, sorted_expiries, filtered_options) or (None, [], []) on error
   """
    try:
        expiries_days = find_reference_target_expiries(data, ticker, target_days=TARGETS_DAYS)
        utc_now = datetime.now(timezone.utc)
        edt_time = utc_now.astimezone(ZoneInfo("America/New_York"))
        filtered_options = []
        for expiry_date, closest_days in expiries_days:
            strike_prices = [
                contract['strike_price']
                for contract in data
                if contract['expiration_date'] == expiry_date
            ]
            closest_strikes = get_ATM(underlying_price, strike_prices, ATM_STRIKE_PRICE_SETTING)
            target_strikes = closest_strikes.keys()
            for contract in data:
                expiry = contract['expiration_date']
                strike_price = contract['strike_price']
                if contract['expiration_date'] == expiry_date and strike_price in target_strikes:
                    filtered_options.append({
                        'Date': edt_time.strftime('%Y-%m-%d'),
                        'Time': edt_time.strftime('%H:%M'),
                        'Ticker': contract['ticker'],
                        'Symbol': ticker,
                        'Type': contract["contract_type"].capitalize(),
                        'Expiry_X': closest_days,
                        'SP_Price_X': strike_price,
                        'ATM_X': closest_strikes[strike_price],
                        'Expiry_Date': expiry,
                    })
        unique_expiries = list(set(pair[0] for pair in expiries_days))
        return underlying_price, sorted(unique_expiries), filtered_options
    except Exception as e:
        logger.warning(f"Processing failed for {ticker} | Error: {e}")
        return None, [], []


def process_ticker(ticker):
    """Pipeline for Multi Threads - handles both calls and puts"""
    try:
        today = datetime.utcnow().date()
        start_date = today + timedelta(days=sorted(TARGETS_DAYS)[0] - 30)  # update this based on
        end_date = today + timedelta(days=sorted(TARGETS_DAYS)[-1] + 30)  # update this based on
        logger.debug(f"Starting processing for {ticker}")

        # Process both call and put options
        underlying_price = fetch_redis(ticker, attribute="last")
        if underlying_price is None:
            raise ValueError(f"Underlying price for {ticker} not found in Redis")
        
        top_sp, bottom_sp = get_top_bottom_strikes(underlying_price=underlying_price)

        data = fetch_reference_option(ticker,
                                      top_sp=top_sp,
                                      bottom_sp=bottom_sp,
                                      start_date=start_date,
                                      end_date=end_date)
        # if not data:
        #     logger.warning(f"No call data for {ticker}")
        #     return [], False, ticker
        # Process data
        if not data:
            raise ValueError(f"No data found for {ticker} in the specified date range")

        uprice, expiries, options = process_option_contract_data(data, ticker, underlying_price)

        return options, True, ticker

    except Exception as e:
        logger.warning(f"Processing failed for {ticker}: {str(e)}")
        return [], False, ticker


def run_optionchain():
    os.makedirs(ATM_TICKER_DIR, exist_ok=True)
    edt_time = ny_now()
    path_call = csv_path_for_today()
    if os.path.exists(path_call):
        os.remove(path_call)
        logger.success(f"Remove {path_call} tickers")
    try:
        if DATA_LOCATE.get('StockList'):
            tickers = list(set(DATA_LOCATE['StockList']))
            start_time = time.time()
            success_count = 0
            logger.success(f"Loaded {len(tickers)} tickers")
        else:
            df = pd.read_csv(DATA_LOCATE["DATA_STOCKS_CSV"])
            tickers = df['Symbol'].unique().tolist() 
            start_time = time.time()
            success_count = 0
            logger.success(f"Loaded {len(tickers)} tickers")
    except Exception as e:
        logger.warning(f"Error loading tickers: {str(e)}")

    all_options = []
    No_Stock = []
    ctx = get_context('spawn')
    # Multithreading setup
    with ctx.Pool(processes=MAX_THREADS, initializer=init_pool_worker, initargs=(API_KEY,), ) as pool:
        
        results_iterator = pool.imap_unordered(process_ticker, tickers)

        for option, result, ticker in tqdm(results_iterator, total=len(tickers), desc="Processing tickers"):

            if result:
                success_count += 1
                all_options.extend(option)
            else:
                No_Stock.append(ticker)

    # Single thread setup
    # for t in tqdm(tickers, total=len(tickers), desc="Processing tickers"):

    #     option, result, ticker = process_ticker(t)
    #     if result:
    #         success_count += 1
    #         all_options.extend(option)
    #     else:
    #         No_Stock.append(ticker)

    total_time = time.time() - start_time
    logger.debug(f"Processing complete: {success_count}/{len(tickers)} succeeded in {total_time / 60:.1f} minutes")
    logger.debug(f"Invalid Stock are {No_Stock}")

    if all_options:
        logger.info(f"Successfully farm {len(all_options)} option contracts.")

        final_df = pd.DataFrame(all_options)

        final_df.to_csv(f"{ATM_TICKER_DIR}/OptionChain_{edt_time.strftime('%Y%m%d')}.csv", index=False)
        logger.success(f"Data saved successfully to /OptionChain_{edt_time.strftime('%Y%m%d')}.csv")
        save_to_redis(final_df, edt_time)
        return final_df, path_call, success_count, No_Stock
    else:
        logger.warning("No data was collected to save.")

def ny_now():
    return datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))

def csv_path_for_today():
    dt = ny_now().strftime('%Y%m%d')
    os.makedirs(ATM_TICKER_DIR, exist_ok=True)
    return f"{ATM_TICKER_DIR}/OptionChain_{dt}.csv"

if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    run_optionchain()