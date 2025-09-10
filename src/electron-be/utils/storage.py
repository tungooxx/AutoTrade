import os
import time

import pandas as pd
import redis
from loguru import logger
from datetime import datetime, timedelta, timezone

from utils.api_client import fetch_option_snapshot
from config_contract import TICKER_DIR
import config_chain
import config_contract

import re
def save_contract_option_tickers(ticker, options, DIR):
    """
    Save individual option contracts to separate CSV files

    Args:
        ticker (str): Stock symbol
        options (list): List of option contract dictionaries
    """
    if not options:
        logger.warning(f"No call contract options found for {ticker}")
        return
    os.makedirs(DIR, exist_ok=True)

    # Create DataFrame first
    df = pd.DataFrame(options)

    # Iterate through each option contract
    for _, row in df.iterrows():
        try:
            # Format filename components      
            option_ticker = row['Option_Ticker']
            match = re.search(r'O:([A-Z]+[0-9]?)(\d{6})', option_ticker)
            if match:
                unique_symbol = match.group(1)
            else:
                unique_symbol = ticker
            expiry = str(row['Expiry']).replace('-', '')
            strike = str(row['Strike_Price']).replace('.0', '')
            option_type = str(row['Contract_Type'])
            type = 'C' if option_type == 'Call' else 'P'
            # Create filename
            filename = f"{DIR}/{unique_symbol}_{type}_{expiry}_{strike}.csv"
            # Convert single row to DataFrame and save
            row.to_frame().T.to_csv(filename, index=False)
        except Exception as e:
            logger.warning(f"Error processing contract for {ticker}: {str(e)}")
            continue


def save_full_contract_option_tickers(ticker, options, DIR, name):
    """
    Save all option contracts to separate CSV files

    Args:
        ticker (str): Stock symbol
        options (list): List of option contract dictionaries
    """
    if not options:
        logger.warning(f"No call contract options found for {ticker}")
        return

    os.makedirs(DIR, exist_ok=True)

    # Create DataFrame first
    df = pd.DataFrame(options)
    utc_now = datetime.now(timezone.utc)
    try:
        filename = f"{DIR}{name}"
        if os.path.exists(filename):
            df.to_csv(filename, mode='a', header=False, index=False)
            logger.info(f"Appended {len(df)} options for {ticker} to {filename}")
        else:
            df.to_csv(filename, mode='w', header=True, index=False)
            logger.info(f"Created chain options csv")


    except Exception as e:
        logger.warning(f"Error processing contract for {ticker}: {str(e)}")


def update_chain(csv_path):
    """
    Load the per-contract CSV if it exists, update metrics,
    or create a new DataFrame if not.
    """
    utc_now = datetime.now(timezone.utc)
    utc_plus8 = utc_now + timedelta(hours=8)
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        for _, row in df.iterrows():
            ticker = row['Symbol']
            option_ticker = row['Ticker']

            contract = fetch_option_snapshot(ticker, option_ticker)
            if not contract or 'results' not in contract:
                logger.warning(f"Missing data for {option_ticker}")
                continue

            gamma = contract['results']['greeks']['gamma']
            oi = contract['results']['open_interest']
            last_trade = contract['results']['last_trade']['price']
            Contract_price = contract['results']['underlying_asset']['price']

            df.loc[_, ["Gamma", "Open_interest", "Contract_price", "Last_trade"]] = [gamma, oi, Contract_price,
                                                                                     last_trade]
            df.loc[_, ["Last_update"]] = [utc_plus8.strftime('%m/%d/%Y %H:%M UTC+8')]
        df.to_csv(csv_path, index=False)
        logger.info(f"Successful Updated {csv_path}")


def update_contract_full(csv_path):
    """
    Load the per-contract CSV if it exists, update metrics,
    or create a new DataFrame if not.
    """
    utc_now = datetime.now(timezone.utc)
    utc_plus8 = utc_now + timedelta(hours=8)
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        for _, row in df.iterrows():
            option_ticker = row['Ticker']
            ticker = row['Symbol']
            type = 'C' if row['Type'].lower() == 'call' else 'P'

            contract = fetch_option_snapshot(ticker, option_ticker)
            if not contract or 'results' not in contract:
                logger.warning(f"Missing data for {option_ticker}")
                continue

            expiry = str(contract['results']['details']['expiration_date']).replace('-', '')
            strike = str(contract['results']['details']['strike_price']).replace('.0', '')

            update_path = f"{ticker}_{type}_{expiry}_{strike}.csv"
            path_call = os.path.join(TICKER_DIR, update_path)

            df_update = pd.read_csv(path_call)
            gamma = contract['results']['greeks']['gamma']
            oi = contract['results']['open_interest']
            last_trade = contract['results']['last_trade']['price']

            df_update.loc[0, ["Gamma", "Open_interest", "Last_trade"]] = [gamma, oi, last_trade]
            df_update.loc[0, ["Last_update"]] = [utc_plus8.strftime('%m/%d/%Y %H:%M UTC+8')]
            df_update.to_csv(path_call, index=False)
            logger.info(f"Successful Updated {path_call}")
            
            
def save_to_redis(df, edt_time):
    """Converts an entire DataFrame to a single JSON string and saves it to Redis."""
    # Define a single key for the entire dataset
    r = redis.Redis(host=config_chain.REDIS_HOST, port=config_chain.REDIS_PORT, password=config_chain.REDIS_PASSWORD, db=0)

    key = f"OptionChain_{edt_time.strftime('%Y%m%d')}"

    logger.info("Converting entire DataFrame to a JSON string...")
    json_data = df.to_json(orient="records", indent=4)

    logger.info(f"Saving option chain Redis in key: '{key}'")

    r.set(key, json_data)

    logger.info("Save complete.")


def save_single_to_redis(options, ticker, DIR):
    """Converts an entire DataFrame to a single JSON string and saves it to Redis."""
    # Define a single key for the entire dataset

    os.makedirs(DIR, exist_ok=True)

    # Create DataFrame first
    df = pd.DataFrame(options)
    r = redis.Redis(host=config_contract.REDIS_HOST, port=config_contract.REDIS_PORT,
                    password=config_contract.REDIS_PASSWORD, db=0)
    # Iterate through each option contract
    for _, row in df.iterrows():
        key = row.get('Option_Ticker', f"{ticker}_UNKNOWN")
        try:
            # Format filename components
            option_ticker = row['Option_Ticker']
            match = re.search(r'O:([A-Z]+[0-9]?)(\d{6})', option_ticker)
            if match:
                unique_symbol = match.group(1)
            else:
                unique_symbol = ticker
                
            expiry = str(row['Expiry']).replace('-', '')
            strike = str(row['Strike_Price']).replace('.0', '')
            option_type = str(row['Contract_Type'])
            type = 'C' if option_type == 'Call' else 'P'

            df = pd.read_csv(f"{DIR}/{unique_symbol}_{type}_{expiry}_{strike}.csv")

            key = f"{unique_symbol}_{type}_{expiry}_{strike}"

            row_dict = df.iloc[0].to_dict()
            
            r.hset(key, mapping=row_dict)
        except Exception as e:
            logger.warning(f"Skipping contract for {key} due to an error. | Error: {e}")
            continue