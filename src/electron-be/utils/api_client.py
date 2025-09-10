import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import redis
from loguru import logger

# Import your config variables
from config_chain import API_KEY, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

_SESSION = None
_API_KEY = None


def build_session():
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.5,  # 0.5, 1.0, 2.0, ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({"Connection": "keep-alive"})
    return s


def init_pool_worker(api_key=API_KEY):
    global _SESSION, _API_KEY
    _API_KEY = api_key
    _SESSION = build_session()


def fetch_redis(ticker, attribute="last"):
    redisConn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, db=0, decode_responses=True)
    quotePrice = redisConn.hgetall("TRADE_US_" + ticker)
    try:
        if quotePrice != None:
            return float(quotePrice[attribute])
    except KeyError:
        logger.warning(f"Attribute {attribute} not found for symbols {ticker} in Redis")
        return None


def fetch_contract_option(ticker, contract_type, max_limit=250, apikey=API_KEY):
    """
    Fetch call option chain data from Polygon.io API for a given ticker

    Args:
        ticker (str): Stock symbol to fetch options for
        api_key (str): Polygon.io API key

    Returns:
        dict: JSON response containing option chain data or None if error occurs
    """
    global _SESSION
    if apikey is None or len(apikey) == 0:
        logger.warning(f"Missing API")
        return
    url = f"https://api.polygon.io/v3/snapshot/options/{ticker}"
    if contract_type == None:
        params = {"limit": max_limit, "apiKey": apikey}
    else:
        params = {'contract_type': contract_type, "limit": max_limit, "apiKey": apikey}

    all_results = []
    sess = _SESSION or build_session()
    try:
        if max_limit < 250:
            try:
                resp = sess.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()
                all_results.extend(data.get("results", []))
            except requests.HTTPError as e:
                if resp.status_code == 401:
                    logger.warning(f"Please check your API key: {apikey}")
                else:
                    logger.warning(f"Error fetching data: {resp.status_code}")
                return None

            return {'results': all_results} if all_results else None
        else:
            while url:
                try:
                    resp = sess.get(url, params=params)
                    resp.raise_for_status()
                    data = resp.json()
                    all_results.extend(data.get("results", []))
                    url = data.get("next_url")
                    params = {"apiKey": apikey} if url else {}
                except requests.HTTPError as e:
                    if resp.status_code == 401:
                        logger.warning(f"Please check your API key: {apikey}")
                    else:
                        logger.warning(f"Error fetching data: {resp.status_code}")
                    return None

            return {'results': all_results} if all_results else None

    except Exception as e:
        logger.warning(f"Fetch failed for {ticker} | Error: {e}")
        return None


def fetch_reference_option(ticker, top_sp, bottom_sp, start_date, end_date, max_limit=1000, apikey=API_KEY):
    """
    Fetch call option chain data from Polygon.io API for a given ticker.

    Args:
        ticker (str): Stock symbol to fetch options for.
        max_limit (int): Maximum number of results per page.
        start_date (datetime.date): Start date for option expiration filter.
        end_date (datetime.date): End date for option expiration filter.
        apikey (str): Polygon.io API key (optional, defaults to global API_KEY).

    Returns:
        dict: JSON response containing option chain data, or None if error occurs.
    """
    global _SESSION, _API_KEY

    if not apikey:
        logger.warning("Missing API key.")
        return None

    url = "https://api.polygon.io/v3/reference/options/contracts"
    params = {
        "underlying_ticker": ticker,
        "strike_price.gte": bottom_sp,
        "strike_price.lte": top_sp,
        "expiration_date.gte": start_date.isoformat(),
        "expiration_date.lte": end_date.isoformat(),
        "apiKey": apikey,
        "limit": max_limit
    }

    all_results = []
    sess = _SESSION or build_session()

    try:
        while url:
            try:
                resp = sess.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()
                all_results.extend(data.get("results", []))
                url = data.get("next_url")
                params = {"apiKey": apikey} if url else {}
            except requests.HTTPError:
                if resp.status_code == 401:
                    logger.warning(f"Invalid API key: {apikey}")
                else:
                    logger.warning(f"HTTP error {resp.status_code} while fetching data.")
                return None

        return all_results

    except Exception as e:
        logger.warning(f"Fetch failed for {ticker} | Error: {e}")
        return None


def fetch_option_snapshot(ticker, option_ticker):
    global _SESSION, _API_KEY

    url = f"https://api.polygon.io/v3/snapshot/options/{ticker}/{option_ticker}"
    params = {"apiKey": _API_KEY}

    sess = _SESSION or build_session()  # fallback for non-pool usage
    try:
        response = sess.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if not data or "results" not in data:
            logger.warning(f"No results for {ticker} {option_ticker}")
            return None
        return data
    except requests.exceptions.RequestException as e:
        logger.warning(f"Fetch failed for {ticker} {option_ticker} | {e}")
        return None