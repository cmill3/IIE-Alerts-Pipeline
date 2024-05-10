import requests
import json
import pandas as pd
from datetime import datetime, timedelta, time
import helpers.ta_formulas as ta
import statistics
import pytz
import warnings
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import logging

logger = logging.getLogger()

warnings.filterwarnings("ignore")

KEY = "XpqF6xBLLrj6WALk4SS1UlkgphXmHQec"

class CustomRetry(Retry):
    def is_retry(self, method, status_code, has_retry_after=False):
        """ Return True if we should retry the request, otherwise False. """
        if status_code != 200:
            return True
        return super().is_retry(method, status_code, has_retry_after)
    
def setup_session_retries(
    retries: int = 3,
    backoff_factor: float = 0.05,
    status_forcelist: tuple = (500, 502, 504),
):
    """
    Sets up a requests Session with retries.
    
    Parameters:
    - retries: Number of retries before giving up. Default is 3.
    - backoff_factor: A factor to use for exponential backoff. Default is 0.3.
    - status_forcelist: A tuple of HTTP status codes that should trigger a retry. Default is (500, 502, 504).

    Returns:
    - A requests Session object with retry configuration.
    """
    retry_strategy = CustomRetry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"]),
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def execute_polygon_call(url):
    session = setup_session_retries()
    response = session.request("GET", url, headers={}, data={})
    return response 

def call_polygon_features_historical(symbol_list, from_stamp, to_stamp, timespan, multiplier,hour,month,day,year):
    trading_hours = [9,10,11,12,13,14,15]
    error_list = []

    year, month, day = to_stamp.split("-")
    current_date = datetime(int(year), int(month), int(day), int(hour))

    data = []
    for symbol in symbol_list:
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={KEY}"
        response = execute_polygon_call(url)
        try:
            response_data = json.loads(response.text)
            results = response_data['results']
        except:
            error_list.append(symbol)
            continue
        results_df = pd.DataFrame(results)
        results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
        results_df['date'] = results_df['t'].apply(lambda x: convert_timestamp_est(x))
        results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
        results_df['minute'] = results_df['date'].apply(lambda x: x.minute)
        results_df['day'] = results_df['date'].apply(lambda x: x.day)
        results_df['month'] = results_df['date'].apply(lambda x: x.month)
        results_df['symbol'] = symbol
        trimmed_df = results_df.loc[results_df['hour'].isin(trading_hours)]
        filtered_df = trimmed_df.loc[~((trimmed_df['hour'] == 9) & (trimmed_df['minute'] < 30))]
        filtered_df = filtered_df.loc[filtered_df['date'] < current_date]
        data.append(filtered_df)

    return data, error_list

def convert_timestamp_est(timestamp):
    # Create a naive datetime object from the UNIX timestamp
    dt_naive = datetime.utcfromtimestamp(timestamp)
    # Convert the naive datetime object to a timezone-aware one (UTC)
    dt_utc = pytz.utc.localize(dt_naive)
    # Convert the UTC datetime to EST
    dt_est = dt_utc.astimezone(pytz.timezone('US/Eastern'))

    year,month,day,hour,minute = dt_est.year, dt_est.month, dt_est.day, dt_est.hour,dt_est.minute
    date = datetime(year,month,day,hour,minute)
    return date
