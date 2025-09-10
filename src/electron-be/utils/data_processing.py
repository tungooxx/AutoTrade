from datetime import datetime
from loguru import logger
from config_chain import ATM_STRIKE_PRICE_SETTING
def get_ATM(underlying_price, strike_prices, settings):
    """
    Find ATM (At-The-Money) strike and surrounding strikes with dynamic range.

    Args:
        underlying_price (float): Current price of the underlying asset
        strike_prices (list): List of available strike prices
        settings (int): Number of strikes to return on each side of ATM
                      (e.g., 2 means 2 below + ATM + 2 above = 5 strikes total)

    Returns:
        list: Strikes centered around ATM (may contain duplicates at boundaries)
    """
    if not strike_prices:
        return []

    # Remove duplicates and sort
    unique_strikes = sorted(list(set(strike_prices)))

    atm_strike = min(unique_strikes, key=lambda x: abs(x - underlying_price))
    atm_index = unique_strikes.index(atm_strike)
    n = len(unique_strikes)

    atm_map = {atm_strike: "ATM"}

    # Get strikes below ATM
    for i in range(1, settings + 1):
        strike_index = max(atm_index - i, 0)
        strike_price = unique_strikes[strike_index]
        if strike_price not in atm_map:
            atm_map[strike_price] = f"ATM-{i}"

    # Get strikes above ATM
    for i in range(1, settings + 1):
        strike_index = min(atm_index + i, n - 1)
        strike_price = unique_strikes[strike_index]
        if strike_price not in atm_map:
            atm_map[strike_price] = f"ATM{i}"

    return atm_map

def find_target_expiries(data, ticker, target_days):
    """
    Find expiry dates closest to the target days (30, 60, 90 days from today)

    Args:
        data (dict): Option chain data from Polygon.io
        ticker (str): Stock symbol (for error messages)
        target_days (list): List of target days to find closest expiries for

    Returns:
        set: Unique expiration dates in YYYY-MM-DD format that are closest to targets
    """
    if not data or 'results' not in data:
        logger.warning(f"No valid data for {ticker}")
        return set()

    today = datetime.now().date()
    expiries = set()
    target_days = sorted(list(set(target_days)))
    # First collect all unique expiration dates
    for contract in data['results']:
        try:
            expiry_str = contract['details']['expiration_date']
            expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d').date()
            if expiry_date > today:  # Only future expiries
                expiries.add(expiry_date)
        except (KeyError, ValueError) as e:
            logger.warning(f"Skipping malformed contract for {ticker} | Error: {e}")
            continue

    if not expiries:
        return set()

    # Find closest expiries to each target
    expiry_to_target = []

    for target in target_days:
        closest = min(expiries, key=lambda x: abs((x - today).days - target))
        expiry_to_target.append((closest.strftime('%Y-%m-%d'), target))
    return expiry_to_target


def find_reference_target_expiries(data, ticker, target_days):
    """
    Find expiry dates closest to the target days (30, 60, 90 days from today)

    Args:
        data (dict): Option chain data from Polygon.io
        ticker (str): Stock symbol (for error messages)
        target_days (list): List of target days to find closest expiries for

    Returns:
        set: Unique expiration dates in YYYY-MM-DD format that are closest to targets
    """
    today = datetime.now().date()
    expiries = set()
    target_days = sorted(list(set(target_days)))
    # First collect all unique expiration dates
    for contract in data:
        try:
            expiry_str = contract['expiration_date']
            expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d').date()
            if expiry_date > today:  # Only future expiries
                expiries.add(expiry_date)
        except (KeyError, ValueError) as e:
            logger.warning(f"Skipping malformed contract for {ticker} | Error: {e}")
            continue

    if not expiries:
        return set()

    # Find closest expiries to each target
    expiry_to_target = []
    
    for target in target_days:
        closest = min(expiries, key=lambda x: abs((x - today).days - target))
        expiry_to_target.append((closest.strftime('%Y-%m-%d'), target))
    return expiry_to_target

def convert_atm_string_to_number(atm_string):
    if atm_string == 'ATM':
        return 0
    try:
        return int(atm_string.replace('ATM', ''))
    except (ValueError, TypeError):
        return 0

def get_top_bottom_strikes(underlying_price, buffer=10):
    gap = ATM_STRIKE_PRICE_SETTING * 10
    top_sp = underlying_price + gap + buffer
    bottom_sp = underlying_price - gap - buffer

    return top_sp, bottom_sp