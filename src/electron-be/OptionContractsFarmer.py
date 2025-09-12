# -*- coding: utf-8 -*-
"""
Fetches and saves individual option contracts
"""
import os
import pandas as pd
from loguru import logger
from config_contract import TICKER_DIR, ATM_TICKER_DIR, API_KEY, MAX_THREADS
from utils.api_client import init_pool_worker, fetch_contract_option, fetch_redis
from utils.storage import save_single_to_redis, save_contract_option_tickers
from utils.data_processing import  get_ATM, convert_atm_string_to_number, find_target_expiries
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import sys
import time
from tqdm import tqdm
from multiprocessing import get_context

# Logger setup
os.makedirs("../../logs", exist_ok=True)
logger.remove()
logger.add(
    os.path.join("../../logs", "option_contracts_warning.log"),
    level="WARNING",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    mode="a",
    enqueue=True
)

logger.add(
    os.path.join("../../logs", "option_contracts_full.log"),
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    mode="a",
    enqueue=True
)


def process_contract_data(data, ticker, TARGETS_DAYS, ATM_STRIKE_PRICE_SETTING, valid_option_tickers):
    """
   Process raw option chain data to extract key information

   Args:
       data (dict): Raw option chain data from Polygon.io
       ticker (str): Stock symbol (passed to expiry_dates)

   Returns:
       tuple: (underlying_price, sorted_expiries, filtered_options) or (None, [], []) on error
   """
    if 'results' not in data or not data['results']:
        return None, [], [], []

    try:
        contracts = [
            contract for contract in data['results']
            if contract.get('details', {}).get('ticker') in valid_option_tickers
        ]
        expiries_days = find_target_expiries({'results': contracts}, ticker, target_days=TARGETS_DAYS)
        underlying_price = fetch_redis(ticker, attribute="last")
        utc_now = datetime.now(timezone.utc)
        edt_time = utc_now.astimezone(ZoneInfo("America/New_York"))
        filtered_options = []
        for expiry_date, closest_days in expiries_days:
            strike_prices = [
                contract['details']['strike_price']
                for contract in contracts
                if contract['details']['expiration_date'] == expiry_date
            ]

            closest_strikes = get_ATM(underlying_price, strike_prices, ATM_STRIKE_PRICE_SETTING)
            target_strikes = closest_strikes.keys()

            for contract in contracts:
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
        processed_tickers = [opt['Option_Ticker'] for opt in filtered_options]
        return True, underlying_price, filtered_options, processed_tickers
    except Exception as e:
        logger.warning(f"Processing failed for {ticker} | Error: {e}")
        return False, [], [], []


def process_ticker(ticker, TARGETS_DAYS, ATM_STRIKE_PRICE_SETTING, valid_option_tickers):
    """Pipeline for Multi Threads"""
    try:
        logger.debug(f"Starting processing for {ticker}")
        data = fetch_contract_option(ticker, contract_type=None)
        if not data or not data.get('results'):
            logger.warning(f"No call data for {ticker}")
            return False, ticker, []

        # Process data
        result, underlying_price, options, processed_tickers = process_contract_data(data, ticker, TARGETS_DAYS,
                                                                            ATM_STRIKE_PRICE_SETTING,
                                                                            valid_option_tickers)
        # Save results
        if result:
            save_contract_option_tickers(ticker=ticker, options=options,DIR=TICKER_DIR)
            save_single_to_redis(options=options, ticker=ticker, DIR=TICKER_DIR)
            logger.debug(f"Saved call/put options for {ticker}")
        
        return result, ticker, processed_tickers

    except Exception as e:
        logger.error(f"Processing failed for {ticker}: {str(e)}", exc_info=True)
        return False, ticker, []
    
def process_ticker_wrapper(args):
    """Helper function to unpack arguments for use with imap_unordered."""
    return process_ticker(*args)

def run_optioncontract():
    os.makedirs(TICKER_DIR, exist_ok=True)
    start_time = time.time()
    edt_time = ny_now()
    source_filepath = csv_path_for_today()
    tickers = []

    try:
        df = pd.read_csv(source_filepath)
        tickers = set(df['Symbol'])
        tickers = list(tickers)

        TARGETS_DAYS = set(df['Expiry_X'])
        TARGETS_DAYS = list(TARGETS_DAYS)
        
        df['ATM_X_Numeric'] = df['ATM_X'].apply(convert_atm_string_to_number)
        ATM_STRIKE_PRICE_SETTING = df['ATM_X_Numeric'].abs().max()
            
        valid_option_tickers = set(df['Ticker'])
        success_count = 0
    except FileNotFoundError:
        logger.error(f"Error: The file was not found at {source_filepath}")
        logger.error("Please run OptionChainFarmer.py first to generate the input file.")
        sys.exit(1)

    args_list = [(ticker, TARGETS_DAYS, ATM_STRIKE_PRICE_SETTING, valid_option_tickers) for ticker in tickers]
    No_Stock = []
    found_option_tickers = set()
    # Multiprocessing setup
    ctx = get_context('spawn')
    with ctx.Pool(processes=MAX_THREADS, initializer=init_pool_worker, initargs=(API_KEY,), ) as pool:
        results_iterator = pool.imap_unordered(process_ticker_wrapper, args_list)

        for result, ticker, processed_tickers in tqdm(results_iterator, total=len(tickers), desc="Processing tickers"):
            if result:
                success_count += 1
                found_option_tickers.update(processed_tickers)
            else:
                No_Stock.append(ticker)

    # Single Thread setup  
    # for t in tqdm(tickers, total=len(tickers), desc="Processing tickers"):

    #     result, ticker, processed_tickers = process_ticker(t)
    #     if result:
    #         success_count += 1
    #         found_option_tickers.update(processed_tickers)
    #     else:
    #         No_Stock.append(ticker)
    missing_tickers = valid_option_tickers - found_option_tickers
    if missing_tickers:
        logger.warning(
            f"Discrepancy found: {len(valid_option_tickers)} tickers were expected, but only {len(found_option_tickers)} were found and processed.")
        logger.warning(
            f"{len(missing_tickers)} option tickers from the input file were not found in the API results and were skipped.")
        logger.info(f"{missing_tickers})}}")
    else:
        logger.success("All option tickers from the input file were successfully found and processed.")
    total_time = time.time() - start_time
    logger.debug(f"Processing complete: {success_count}/{len(tickers)} succeeded in {total_time / 60:.1f} minutes")
    return {
        "tickers_total": len(tickers),
        "success_count": success_count,
        "missing_count": len(missing_tickers),
        "duration_sec": total_time,
        "timestamp": str(edt_time),
    }
def ny_now():
    return datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))

def csv_path_for_today():
    dt = ny_now().strftime('%Y%m%d')
    os.makedirs(ATM_TICKER_DIR, exist_ok=True)
    return f"{ATM_TICKER_DIR}/OptionChain_{dt}.csv"

if __name__ == "__main__":
    utc_now = datetime.now(timezone.utc)
    edt_time = utc_now.astimezone(ZoneInfo("America/New_York"))
    main()