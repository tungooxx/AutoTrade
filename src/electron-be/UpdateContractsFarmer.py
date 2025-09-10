# -*- coding: utf-8 -*-
"""
Update and saves exist individual option contracts
"""
import os
import pandas as pd
from loguru import logger
import time
from config_contract import TICKER_DIR, ATM_TICKER_DIR, API_KEY
from utils.api_client import init_pool_worker, fetch_contract_option, fetch_redis
from utils.storage import save_single_to_redis
from utils.data_processing import find_target_expiries, get_ATM, convert_atm_string_to_number
from utils.config import load_timeframe
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import sys
# import asyncio
# import aiohttp
# from tqdm.asyncio import tqdm_asyncio
from tqdm import tqdm
from multiprocessing import Pool, get_context
from OptionChainFarmer import csv_path_for_today
from concurrent.futures import ThreadPoolExecutor, as_completed
import glob

# Logger setup
os.makedirs("../../logs", exist_ok=True)
logger.remove()
logger.add(
    os.path.join("../../logs", "option_contract_update_warning.log"),
    level="WARNING",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    mode="a",
    enqueue=True
)
logger.add(
    os.path.join("../../logs", "option_contract_update_full.log"),
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    mode="a",
    enqueue=True
)


def process_update_contract_data(data, ticker, TARGETS_DAYS, ATM_STRIKE_PRICE_SETTING):
    """
   Process raw option chain data to extract key information

   Args:
       data (dict): Raw option chain data from Polygon.io
       ticker (str): Stock symbol (passed to expiry_dates)

   Returns:
       tuple: (underlying_price, sorted_expiries, filtered_options) or (None, [], []) on error
   """
    if 'results' not in data or not data['results']:
        return None, [], []

    try:
        expiries_days = find_target_expiries(data, ticker, target_days=TARGETS_DAYS)
        underlying_price = fetch_redis(ticker, attribute='last')
        utc_now = datetime.now(timezone.utc)
        edt_time = utc_now.astimezone(ZoneInfo("America/New_York"))
        filtered_options = []
        for expiry_date, closest_days in expiries_days:
            strike_prices = [
                contract['details']['strike_price']
                for contract in data['results']
                if contract['details']['expiration_date'] == expiry_date
            ]

            closest_strikes = get_ATM(underlying_price, strike_prices, ATM_STRIKE_PRICE_SETTING)
            target_strikes = closest_strikes.keys()

            for contract in data['results']:
                details = contract.get('details', {})
                greeks = contract.get('greeks', {})
                last_quote = contract.get('last_quote', {})
                day_data = contract.get('day', {})
                
                expiry = details['expiration_date']
                strike_price = details['strike_price']
                if details['expiration_date'] == expiry_date and strike_price in target_strikes:
                    try:
                        filtered_options.append({
                            'Date': edt_time.strftime('%Y-%m-%d'),
                            'Time': edt_time.strftime('%H:%M'),
                            'Option_Ticker': details.get('ticker'),
                            'Strike_Price': strike_price,
                            'Expiry': expiry,
                            'Contract_Type': str(details.get('contract_type', '')).capitalize(),
                            'Expiry_X': closest_days,
                            'ATM_X': closest_strikes.get(strike_price),
                            'IV': contract.get('implied_volatility'),
                            'Open_interest': contract.get("open_interest"),
                            'Delta': greeks.get('delta'),
                            'Gamma': greeks.get('gamma'),
                            'Theta': greeks.get('theta'),
                            'Vega': greeks.get('vega'),
                            'Bid': last_quote.get('bid'),
                            'Ask': last_quote.get('ask'),
                            "Open": day_data.get('open'),
                            'High': day_data.get('high'),
                            'Low': day_data.get('low'),
                            'Close': day_data.get('close'),
                            'Volume': day_data.get('volume'),
                            'VWAP': day_data.get('vwap'),
                        })
                    except (KeyError, TypeError) as e:
                        option_ticker_for_log = details.get('ticker', 'UNKNOWN')
                        logger.warning(
                            f"Data for {option_ticker_for_log} ({ticker}) is incomplete. Missing field: {e}. Skipping."
                        )
        return True, underlying_price, filtered_options
    except Exception as e:
        logger.warning(f"Processing failed for {ticker} | Error: {e}")
        return False, [], []

def update_metrics_for_ticker(ticker, TARGETS_DAYS, ATM_STRIKE_PRICE_SETTING):
    try:
        data = fetch_contract_option(ticker, contract_type=None)
        if not data or not data.get('results'):
            logger.warning(f"No data for {ticker}")
            return [], False, ticker
        result, underlying_price, options = process_update_contract_data(data, ticker, TARGETS_DAYS, ATM_STRIKE_PRICE_SETTING)
        
        # Save results to Redis
        if result:
            save_single_to_redis(options=options, ticker=ticker, DIR=TICKER_DIR)
            
        return options, result, ticker
    except Exception as e:
        logger.warning(f"Error Update contract for {ticker}: {str(e)}")
        return [], False, ticker
    
def process_ticker_wrapper(args):
    """Helper function to unpack arguments for use with imap_unordered."""
    return update_metrics_for_ticker(*args)

def run_updatecontract():
    
    os.makedirs(TICKER_DIR, exist_ok=True)
    edt_time = ny_now()
    filename = csv_path_for_today()
    output_dir = os.path.join(TICKER_DIR, "Updater")
    os.makedirs(output_dir, exist_ok=True)
    try:
        df = pd.read_csv(filename)
        tickers = set(df['Symbol'])
        tickers = list(tickers)
        
        TARGETS_DAYS = set(df['Expiry_X'])
        TARGETS_DAYS = sorted(list(TARGETS_DAYS))
        
        df['ATM_X_Numeric'] = df['ATM_X'].apply(convert_atm_string_to_number)
        ATM_STRIKE_PRICE_SETTING = df['ATM_X_Numeric'].abs().max()
        
        total = len(tickers)
        success_count = 0
    except FileNotFoundError:
        logger.error(f"Error: The file was not found at {filename}")
        logger.error("Please run OptionChainFarmer.py first to generate the input file.")
        sys.exit(1)
    success_count = 0
    counter = 1
    completed = 1
    args_list = [(ticker, TARGETS_DAYS, ATM_STRIKE_PRICE_SETTING) for ticker in tickers]

    frame = load_timeframe()  # in minutes, dynamic
    if counter >= frame:
        all_options = []
        start = time.time()
        logger.info(f"Starting update cycle (every {frame} min)")
        print("START UPDATE!!!")
        utc_now = datetime.now(timezone.utc)
        edt_time = utc_now.astimezone(ZoneInfo("America/New_York"))
        success_count = 0
        # Multiprocessing setup
        ctx = get_context('spawn')
        with ctx.Pool(processes=os.cpu_count(), initializer=init_pool_worker, initargs=(API_KEY,), ) as pool:
            results_iterator = pool.imap_unordered(process_ticker_wrapper, args_list)

            for option, result, ticker in tqdm(results_iterator, total=len(tickers), desc="Processing tickers"):

                if result:
                    success_count += 1
                    completed += 1
                    all_options.extend(option)


                if completed % 10 == 0:
                    logger.debug(f"Progress: {completed}/{total} tickers ({success_count} successful)")
        
        # Single Thread setup
        # for ticker in tqdm(tickers, total=total, desc="Processing tickers"):
        #     option, result, _ = update_metrics_for_ticker(ticker, TARGETS_DAYS, ATM_STRIKE_PRICE_SETTING)

        #     if result:
        #         success_count += 1
        #         completed += 1
        #         all_options.extend(option)

        #     if completed % 10 == 0:
        #         logger.debug(f"Progress: {completed}/{total} tickers ({success_count} successful)")
        total_time = time.time() - start
        logger.debug(f"Processing complete: {success_count}/{total} succeeded in {total_time / 60:.1f} minutes")
        
        counter = 0
        success_count = 0   
        completed = 0
        path = csv_updater_path_for_today()
        if all_options:
            logger.info(f"Successfully Update {len(all_options)} option contracts.")

            df = pd.DataFrame(all_options)
            df.to_csv(path,
                      index=False)
            logger.success(
                f"Data saved successfully to {TICKER_DIR}/Updater/OptionContracts_{edt_time.strftime('%Y%m%d')}_{edt_time.strftime('%H%M')}.csv")
        else:
            logger.warning("No data was collected to save.")
        return df, path
    else:
        time.sleep(60)
        counter += 1
def ny_now():
    return datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))

def csv_updater_path_for_today():
    edt_time = ny_now()
    return f"{TICKER_DIR}/Updater/OptionContracts_{edt_time.strftime('%Y%m%d')}_{edt_time.strftime('%H%M')}.csv"

if __name__ == "__main__":
    run_updatecontract()