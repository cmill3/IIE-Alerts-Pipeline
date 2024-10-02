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
import pywt
import numpy as np
from statsmodels.tsa.seasonal import seasonal_decompose
from scipy import stats

logger = logging.getLogger()

warnings.filterwarnings("ignore")

KEY = "XpqF6xBLLrj6WALk4SS1UlkgphXmHQec"

class CustomRetry(Retry):
    def is_retry(self, method, status_code, has_retry_after=False):
        """ Return True if we should retry the request, otherwise False. """
        if status_code != 200:
            return True
        return super().is_retry(method, status_code, has_retry_after)

def format_pcr_dates(dates):
    date_str = dates.apply(lambda x: x.strftime("%Y-%m-%d"))
    # cut_date = date_str.apply(lambda x: x.split(" ")[0])
    formatted_dates = date_str.apply(lambda x: datetime.strptime(x, '%Y-%m-%d').strftime("%Y-%m-%dT%H:%M:%SZ"))
    return formatted_dates

def get_pcr(symbol, window, dates):
    symbol = symbol.iloc[0]
    pcr_dates = format_pcr_dates(dates)
    url = f"https://www.alphaquery.com/data/option-statistic-chart?ticker={symbol}\
        &perType={window}-Day&identifier=put-call-ratio-volume"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) \
            Chrome/70.0.3538.77 Safari/537.36"
    }
    
    r = requests.get(url, headers=headers)
    pcr = pd.DataFrame.from_dict(r.json())
    pcr.rename(columns={"x": "date", "value": "PCR"}, inplace=True)
    x = pcr.loc[pcr['date'].isin(pcr_dates)]
    raw_list = x['PCR'].values.tolist()
    raw_list.insert(0,0)
    return raw_list

def get_pcr_historic(symbol, window, dates):
    symbol = symbol.iloc[0]
    pcr_dates = format_pcr_dates(dates)
    url = f"https://www.alphaquery.com/data/option-statistic-chart?ticker={symbol}\
        &perType={window}-Day&identifier=put-call-ratio-volume"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) \
            Chrome/70.0.3538.77 Safari/537.36"
    }
    
    r = requests.get(url, headers=headers)
    pcr = pd.DataFrame.from_dict(r.json())
    pcr.rename(columns={"x": "date", "value": "PCR"}, inplace=True)
    x = pcr.loc[pcr['date'].isin(pcr_dates)]
    raw_list = x['PCR'].values.tolist()
    return raw_list

def calc_price_action(row):
    try:
        date = row['date']
        from_stamp = date.strftime("%Y-%m-%d")
        aggs = call_polygon_price(row['symbol'], from_stamp, "hour", 1, row['hour'])
        one_day, three_day, two_hour, four_hour = build_date_dfs(aggs, date)
        open = one_day.head(1)['o'].values[0]

        one_c = one_day.tail(1)['c'].values[0]
        one_h = one_day['h'].max()
        one_l = one_day['l'].min()

        three_c = three_day.tail(1)['c'].values[0]
        three_h = three_day['h'].max()
        three_l = three_day['l'].min()

        twoH_c = two_hour.tail(1)['c'].values[0]
        twoH_h = two_hour['h'].max()
        twoH_l = two_hour['l'].min()

        fourH_c = four_hour.tail(1)['c'].values[0]
        fourH_h = four_hour['h'].max()
        fourH_l = four_hour['l'].min()

        one_high = (one_h - open)/ open
        one_low = (one_l - open)/ open
        one_pct = (one_c - row['alert_price'])/row['alert_price']

        three_high = (three_h - open)/ open
        three_low = (three_l - open)/ open
        three_pct = (three_c - row['alert_price'])/row['alert_price']

        twoH_high = (twoH_h - open)/ open
        twoH_low = (twoH_l - open)/ open
        twoH_pct = (twoH_c - row['alert_price'])/row['alert_price']
        
        fourH_high = (fourH_h - open)/ open
        fourH_low = (fourH_l - open)/ open
        fourH_pct = (fourH_c - row['alert_price'])/row['alert_price']

        results_dict =  {
            "one_max": one_high, "one_min": one_low, "one_pct": one_pct, 
            "three_max": three_high, "three_min": three_low, "three_pct": three_pct,
            "twoH_max": twoH_high, "twoH_min": twoH_low, "twoH_pct": twoH_pct,
            "fourH_max": fourH_high, "fourH_min": fourH_low, "fourH_pct": fourH_pct,
            "symbol": row['symbol']}
        
        return results_dict
    except Exception as e:
        print(f"{e} for {row['symbol']} in calc price")
        results_dict =  {
            "one_max": 67, "one_min": 67, "one_pct": 67, 
            "three_max": 67, "three_min": 67, "three_pct": 67,
            "twoH_max": 67, "twoH_min": 67, "twoH_pct": 67,
            "fourH_max": 67, "fourH_min": 67, "fourH_pct": 67,
            "symbol": row['symbol']}
        return results_dict

def build_date_dfs(df, dt):
    sell_1d = calculate_sellby_date(dt, 2)
    sell_3d = calculate_sellby_date(dt, 4)
    year, month, day = sell_1d.strftime("%Y-%m-%d").split("-")
    year3, month3, day3 = sell_3d.strftime("%Y-%m-%d").split("-")
    dt_3d = datetime(int(year3), int(month3), int(day3),tzinfo=pytz.timezone('US/Eastern'))
    dt_1d = datetime(int(year), int(month), int(day),tzinfo=pytz.timezone('US/Eastern'))
    one_day_df = df.loc[df['date'] < dt_1d]
    three_day_df = df.loc[df['date'] < dt_3d]
    two_hour_df = df.iloc[:2]
    four_hour_df = df.iloc[:4]
    return one_day_df, three_day_df, two_hour_df, four_hour_df

    
def calculate_sellby_date(dt, trading_days_to_add): #End date, n days later for the data set built to include just trading days, but doesnt filter holiday
    # date_str = current_date.strftime("%Y-%m-%d %H:%M:%S")
    # date_str = date_str.split(" ")[0]
    # dt = datetime.strptime(date_str,"%Y-%m-%d")
    while trading_days_to_add > 0:
        dt += timedelta(days=1)
        weekday = dt.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1
    return dt

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

def call_polygon(symbol_list, from_stamp, to_stamp, timespan, multiplier):
    payload={}
    headers = {}
    dfs = []
    
    error_list = []

    if timespan == "minute":
        from_stamp = to_stamp
    for symbol in symbol_list:
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={KEY}"

        response = execute_polygon_call(url)

        response_data = json.loads(response.text)
        try:
            results = response_data['results']
        except:
            error_list.append(symbol)
            continue
        results_df = pd.DataFrame(results)
        results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
        results_df['date'] = results_df['t'].apply(lambda x: convert_timestamp_est(x))
        results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
        results_df['symbol'] = symbol
        dfs.append(results_df)

    return dfs, error_list


def call_polygon_features(symbol_list, from_stamp, to_stamp, timespan, multiplier,hour,month,day,year):
    payload={}
    headers = {}
    dfs = []
    trading_hours = [9,10,11,12,13,14,15]
    error_list = []

    year, month, day = to_stamp.split("-")
    current_date = datetime(int(year), int(month), int(day))

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
        print(filtered_df['date'].tail(1))
        filtered_df['date'] = filtered_df['date'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        filtered_df['date'] = filtered_df['date'].apply(lambda x: x.split(" ")[0])
        filtered_df['date'] = pd.to_datetime(filtered_df['date'])
        filtered_df = filtered_df.loc[filtered_df['date'] <= current_date]
        print(filtered_df['date'].tail(1))
        print(filtered_df['hour'].head(1))
        data.append(filtered_df)

    return data, error_list


def call_polygon_price(symbol, date_stamp, timespan, multiplier, hour):
    from_stamp = date_stamp.split(' ')[0]
    sell_by = calculate_sellby_date(datetime.strptime(from_stamp,"%Y-%m-%d"),4)
    to_stamp = sell_by.strftime("%Y-%m-%d %H:%M:%S").split(" ")[0]
    year, month, day = from_stamp.split("-")
    time_stamp = datetime(int(year),int(month),int(day),int(hour)).timestamp()
    trading_hours = [9,10,11,12,13,14,15]
    error_list = []
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={KEY}"
    response = execute_polygon_call(url)

    response_data = json.loads(response.text)
    results = response_data['results']
    results_df = pd.DataFrame(results)
    results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
    results_df['date'] = results_df['t'].apply(lambda x: convert_timestamp_est(x))
    results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
    results_df['day'] = results_df['date'].apply(lambda x: x.day)
    results_df['minute'] = results_df['date'].apply(lambda x: x.minute)
    results_df = results_df.loc[results_df['t'] >= time_stamp]
    results_df = results_df.loc[results_df['hour'].isin(trading_hours)]
    results_df = results_df.loc[~((results_df['hour'] == 9) & (results_df['minute'] < 30))]
    # results_df['symbol'] = row['symbol']
    # dfs.append(results_df)

    return results_df

def call_polygon_PCR_price(symbol, date_stamp, timespan, multiplier, hour):
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{date_stamp}/{date_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={KEY}"
    response = execute_polygon_call(url)
    trading_hours = [9,10,11,12,13,14,15]

    response_data = json.loads(response.text)
    results = response_data['results']
    results_df = pd.DataFrame(results)
    results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
    results_df['date'] = results_df['t'].apply(lambda x: convert_timestamp_est(x))
    results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
    results_df['day'] = results_df['date'].apply(lambda x: x.day)
    results_df['minute'] = results_df['date'].apply(lambda x: x.minute)
    results_df = results_df.loc[results_df['hour'].isin(trading_hours)]
    results_df = results_df.loc[results_df['hour'] == int(hour)]
    return results_df['o'].values[0]


def call_polygon_price_day(symbol, from_stamp, to_stamp, timespan, multiplier):
    payload={}
    headers = {}
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=false&sort=asc&limit=50000&apiKey={KEY}"
    response = execute_polygon_call(url)
    response_data = json.loads(response.text)
    results = response_data['results']
    results_df = pd.DataFrame(results)
    return results_df['c'].iloc[-1]


def call_polygon_PCR(symbols, from_stamp, to_stamp, timespan, multiplier, hour):
    values = []
    trading_hours = [9,10,11,12,13,14,15]
    for symbol in symbols:
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={KEY}"
        response = execute_polygon_call(url) 
        try:
            response_data = json.loads(response.text)
        except Exception as e:
            response = execute_polygon_call(url)
            response_data = json.loads(response.text)
        try:
            results = response_data['results']
            results_df = pd.DataFrame(results)
            results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
            results_df['date'] = results_df['t'].apply(lambda x: convert_timestamp_est(x))
            results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
            results_df = results_df.loc[results_df['hour'] <= int(hour)]
            results_df = results_df.loc[results_df['hour'].isin(trading_hours)]
        except Exception as e:
            # print(f"{e} for {symbol}")
            continue
        values.append({"high": results_df['h'].max(),"low": results_df['l'].min(),"volume": results_df['v'].sum(),"symbol": symbol})

    full_df = pd.DataFrame.from_dict(values)
    return full_df


def call_polygon_backtest(symbols, from_stamp, to_stamp, timespan, multiplier):
    values = []
    for symbol in symbols:
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={KEY}"
        response = execute_polygon_call(url)
        response_data = json.loads(response.text)
        try:
            results = response_data['results']
            results_df = pd.DataFrame(results)
            results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
            results_df['date'] = results_df['t'].apply(lambda x: convert_timestamp_est(x))
            # results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
        except Exception as e:
            # print(url)
            # print(f"{e} for {symbol}")
            continue
        values.append({"high": results_df['h'].max(),"low": results_df['l'].min(),"volume": results_df['v'].sum(),"symbol": symbol})

    full_df = pd.DataFrame.from_dict(values)
    return full_df

def feature_engineering(dfs,date,hour):
    features = []
    agg_dict = {
            'v': 'sum',
            'o': 'first',
            'c': 'last',
            'h': 'max',
            'l': 'min'
        }
    
    for thirty_aggs in dfs:
        try:
            if len(thirty_aggs) == 0:
                print("Empty")
                continue
            # min_aggs.reset_index(drop=True,inplace=True)
            thirty_aggs.set_index('date',inplace=True)
            # Perform resampling and aggregation
            hour_aggs = thirty_aggs.resample('H').agg(agg_dict)
            daily_aggs = thirty_aggs.resample('D').agg(agg_dict)
            hour_aggs.dropna(inplace=True)
            daily_aggs.dropna(inplace=True)

            thirty_aggs['price_change_absolute'] = abs(thirty_aggs['c'].pct_change())
            thirty_aggs['volume_change_absolute'] = abs(thirty_aggs['v'].pct_change())
            hour_aggs['price_change_absolute_H'] = abs(hour_aggs['c'].pct_change())
            hour_aggs['volume_change_absolute'] = abs(hour_aggs['v'].pct_change())
            daily_aggs['price_change_absolute_D'] = abs(daily_aggs['c'].pct_change())
            daily_aggs['volume_change_absolute'] = abs(daily_aggs['v'].pct_change())
            thirty_aggs['std_volatility'] = thirty_aggs['c'].pct_change()
            thirty_aggs['range_volatility'] = (thirty_aggs['h'] - thirty_aggs['l']) / thirty_aggs['c']
            thirty_aggs['volume_change'] = thirty_aggs['v'].pct_change()
            hour_aggs['price_change_H'] = hour_aggs['c'].pct_change()
            hour_aggs['volume_change'] = hour_aggs['v'].pct_change()
            daily_aggs['price_change_D'] = daily_aggs['c'].pct_change()
            daily_aggs['volume_change'] = daily_aggs['v'].pct_change()

            ## Range volatiltiy features
            hour_aggs['price_range_H'] = (hour_aggs['h'] - hour_aggs['l'])/hour_aggs['c']
            daily_aggs['price_range_D'] = (daily_aggs['h'] - daily_aggs['l'])/daily_aggs['c']
            hour_aggs['price_range_8MA'] = hour_aggs['price_range_H'].rolling(8).mean()
            hour_aggs['price_range_8MA_diff'] = (hour_aggs['price_range_H'] - hour_aggs['price_range_8MA'])/ hour_aggs['price_range_8MA']
            daily_aggs['price_range_5DMA'] = daily_aggs['price_range_D'].rolling(5).mean()
            daily_aggs['price_range_5DMA_diff'] = (daily_aggs['price_range_D'] - daily_aggs['price_range_5DMA'])/ daily_aggs['price_range_5DMA']

            ## Return Vol Features
            hour_aggs['return_vol_8H'] = hour_aggs['price_change_absolute_H'].rolling(window=8).mean()
            hour_aggs['return_vol_8H_diff'] = ((hour_aggs['price_change_absolute_H'] - hour_aggs['return_vol_8H'])/hour_aggs['return_vol_8H'])
            daily_aggs['return_vol_5D'] = daily_aggs['price_change_absolute_D'].rolling(window=5).mean()
            daily_aggs['return_vol_5D_diff'] = ((daily_aggs['price_change_absolute_D'] - daily_aggs['return_vol_5D'])/daily_aggs['return_vol_5D'])
            daily_aggs['return_vol_10D'] = daily_aggs['price_change_absolute_D'].rolling(window=10).mean()
            daily_aggs['return_vol_10D_diff'] = ((daily_aggs['price_change_absolute_D'] - daily_aggs['return_vol_10D'])/daily_aggs['return_vol_10D'])

            ## STDDEV Features
            hour_aggs['ret_vol_stddev40H'] = hour_aggs['price_change_absolute_H'].rolling(window=40).std()
            hour_aggs['ret_vol_stddev40H_diff'] = ((hour_aggs['price_change_H'] - hour_aggs['ret_vol_stddev40H'])/hour_aggs['ret_vol_stddev40H'])
            hour_aggs['volume_vol_stddev40H'] = hour_aggs['volume_change_absolute'].rolling(window=40).std()
            hour_aggs['volume_vol_stddev40H_diff'] = ((hour_aggs['volume_change'] - hour_aggs['volume_vol_stddev40H'])/hour_aggs['volume_vol_stddev40H'])
            daily_aggs['ret_vol_stddev20D'] = daily_aggs['price_change_absolute_D'].rolling(window=20).std()
            daily_aggs['ret_vol_stddev20D_diff'] = ((daily_aggs['price_change_D'] - daily_aggs['ret_vol_stddev20D'])/daily_aggs['ret_vol_stddev20D'])
            hour_aggs['range_vol_stddev40H'] = hour_aggs['price_range_H'].rolling(window=40).std()
            hour_aggs['range_vol_stddev40H_diff'] = ((hour_aggs['price_range_H'] - hour_aggs['range_vol_stddev40H'])/hour_aggs['range_vol_stddev40H'])
            daily_aggs['range_vol_stddev20D'] = daily_aggs['price_range_D'].rolling(window=20).std()
            daily_aggs['range_vol_stddev20D_diff'] = ((daily_aggs['price_range_D'] - daily_aggs['range_vol_stddev20D'])/daily_aggs['range_vol_stddev20D'])


            ## Volume Features
            thirty_aggs['volume_5MA'] = thirty_aggs['v'].rolling(5).mean()
            thirty_aggs['volume_14MA'] = thirty_aggs['v'].rolling(14).mean()
            thirty_aggs['volume_28MA'] = thirty_aggs['v'].rolling(28).mean()
            thirty_aggs['volume_56MA'] = thirty_aggs['v'].rolling(56).mean()
            thirty_aggs['volume_84MA'] = thirty_aggs['v'].rolling(84).mean()
            thirty_aggs['volume_14_56MA_diff'] = (thirty_aggs['volume_14MA'] - thirty_aggs['volume_56MA'])/thirty_aggs['volume_56MA']
            thirty_aggs['volume_14_84MA_diff'] = (thirty_aggs['volume_14MA'] - thirty_aggs['volume_84MA'])/thirty_aggs['volume_84MA']
            thirty_aggs['volume_28_84MA_diff'] = (thirty_aggs['volume_28MA'] - thirty_aggs['volume_84MA'])/thirty_aggs['volume_84MA']
            thirty_aggs['volume_sum15'] = thirty_aggs['v'].rolling(15).sum()
            thirty_aggs['volume_sum15_5DMA'] = thirty_aggs['v'].rolling(5).mean()
            thirty_aggs['volume_sum15_10DMA'] = thirty_aggs['v'].rolling(10).mean()
            thirty_aggs['volume_sum15_5DMA_diff'] = (thirty_aggs['volume_sum15'] - thirty_aggs['volume_sum15_5DMA'])/thirty_aggs['volume_sum15_5DMA']
            thirty_aggs['volume_sum15_10DMA_diff'] = (thirty_aggs['volume_sum15'] - thirty_aggs['volume_sum15_10DMA'])/thirty_aggs['volume_sum15_10DMA']

            ## Trend Features 
            daily_aggs['price_3Ddiff'] = daily_aggs['c'].pct_change(3)
            daily_aggs['price_5Ddiff'] = daily_aggs['c'].pct_change(5)
            daily_aggs['price_10Ddiff'] = daily_aggs['c'].pct_change(10)
            daily_aggs['price_20Ddiff'] = daily_aggs['c'].pct_change(20)
            daily_aggs['price_3D20D_diff'] = (daily_aggs['price_3Ddiff'] - daily_aggs['price_20Ddiff'])/daily_aggs['price_20Ddiff']

            ##CLOSE MA Features
            daily_aggs['close_20DMA'] = daily_aggs['c'].rolling(20).mean()
            daily_aggs['close_20DMA_diff'] = (daily_aggs['c'] - daily_aggs['close_20DMA'])/daily_aggs['close_20DMA']
            daily_aggs['close_10DMA'] = daily_aggs['c'].rolling(10).mean()
            daily_aggs['close_10DMA_diff'] = (daily_aggs['c'] - daily_aggs['close_10DMA'])/daily_aggs['close_10DMA']

            ## Technical Indicators
            daily_aggs['rsi'] = ta.rsi(daily_aggs['c'],window=14)
            hour_aggs['rsiH'] = ta.rsi(hour_aggs['c'],window=16)
            daily_aggs['rsi_15MA'] = daily_aggs['rsi'].rolling(15).mean()
            hour_aggs['rsiH_15MA'] = hour_aggs['rsiH'].rolling(15).mean()
            daily_aggs['rsi_15MA_diff'] = (daily_aggs['rsi'] - daily_aggs['rsi_15MA'])/daily_aggs['rsi_15MA']
            hour_aggs['rsiH_15MA_diff'] = (hour_aggs['rsiH'] - hour_aggs['rsiH_15MA'])/hour_aggs['rsiH_15MA']
            daily_aggs['roc'] = ta.roc(daily_aggs['c'],window=10)
            hour_aggs['roc8H'] = ta.roc(hour_aggs['c'],window=8)
            daily_aggs['roc3'] = ta.roc(daily_aggs['c'],window=3)
            daily_aggs['roc5'] = ta.roc(daily_aggs['c'],window=5)
            daily_aggs['roc_15MA'] = daily_aggs['roc'].rolling(15).mean()
            hour_aggs['roc8H_15MA'] = hour_aggs['roc8H'].rolling(15).mean()
            daily_aggs['roc_15MA_diff'] = (daily_aggs['roc'] - daily_aggs['roc_15MA'])/daily_aggs['roc_15MA']
            hour_aggs['roc8H_15MA_diff'] = (hour_aggs['roc8H'] - hour_aggs['roc8H_15MA'])/hour_aggs['roc8H_15MA']
            daily_aggs['adx'] = ta.adx(daily_aggs,window=14)
            daily_aggs['macd'] = ta.macd(daily_aggs['c'])
            daily_aggs['macd_15MA'] = daily_aggs['macd'].rolling(15).mean()
            daily_aggs['macd_15MA_diff'] = (daily_aggs['macd'] - daily_aggs['macd_15MA'])/daily_aggs['macd_15MA']
            upper_band, lower_band, middle_band = ta.bbands(daily_aggs['c'],window=20)
            daily_aggs['bbu'] = upper_band
            daily_aggs['bbl'] = lower_band
            daily_aggs['bbm'] = middle_band
            daily_aggs['bb_spread'] = (daily_aggs['bbu'] - daily_aggs['bbl'])/daily_aggs['c']
            daily_aggs['bb_trend'] = (daily_aggs['c'] - daily_aggs['bbm'])/daily_aggs['bbm']
            daily_aggs['bb_category'] = daily_aggs.apply(lambda x: ta.bbands_category(x['c'],x['bbu'],x['bbl']), axis=1)
            daily_aggs['cmf'] = ta.cmf(daily_aggs,window=20)
            daily_aggs['cmf_15MA'] = daily_aggs['cmf'].rolling(15).mean()
            daily_aggs['cmf_15MA_diff'] = (daily_aggs['cmf'] - daily_aggs['cmf_15MA'])/daily_aggs['cmf_15MA']

            ## Hi/Lo Features
            daily_aggs['max_30'] = daily_aggs['h'].rolling(30).max()
            daily_aggs['min_30'] = daily_aggs['l'].rolling(30).min()
            daily_aggs['max_30_diff'] = (daily_aggs['h'] - daily_aggs['max_30'])/daily_aggs['max_30']
            daily_aggs['min_30_diff'] = (daily_aggs['l'] - daily_aggs['min_30'])/daily_aggs['min_30']

            ## Volatiltiy Oscialltor
            daily_aggs['roc_vol'] = ta.roc(daily_aggs['price_range_D'],window=10)
            daily_aggs['roc_vol3'] = ta.roc(daily_aggs['price_range_D'],window=3)
            daily_aggs['roc_vol5'] = ta.roc(daily_aggs['price_range_D'],window=5)
            daily_aggs['roc_vol_15MA'] = daily_aggs['roc_vol'].rolling(15).mean()
            daily_aggs['roc_vol_15MA_diff'] = (daily_aggs['roc_vol'] - daily_aggs['roc_vol_15MA'])/daily_aggs['roc_vol_15MA']


            ## Volatility Wavelet Features
            thirty_aggs = wavelet_features_vol(thirty_aggs)



            thirty_features = thirty_aggs.iloc[-1]
            hour_features = hour_aggs.iloc[-1]
            daily_features = daily_aggs.iloc[-1]
            thirty_features.drop(['t','o','h','l','v','t','price_change_absolute','volume_change_absolute','volume_change'], inplace=True)
            hour_features.drop(['o','h','l','v','c','volume_change_absolute','volume_change'], inplace=True)
            daily_features.drop(['o','h','l','v','c','volume_change_absolute','volume_change'], inplace=True)
            df_combined = pd.concat([thirty_features, hour_features, daily_features])
            features.append(df_combined)
        except Exception as e:
            print(f"{e} in feature engineering")
            continue
    features_df = pd.DataFrame(features)
    features_df['date'] = date
    features_df['hour'] = hour
    features_df['alert_price'] = features_df['c']
    features_df.drop(['minute','c'], axis=1, inplace=True)

    return features_df


def convert_timestamp_est(timestamp):
    # Create a naive datetime object from the UNIX timestamp
    dt_naive = datetime.utcfromtimestamp(timestamp)
    # Convert the naive datetime object to a timezone-aware one (UTC)
    dt_utc = pytz.utc.localize(dt_naive)
    # Convert the UTC datetime to EST
    dt_est = dt_utc.astimezone(pytz.timezone('US/Eastern'))
    
    return dt_est

def call_polygon_option_snapshot(symbol,expiration_dates):
    symbol_dfs = []
    for expiry in expiration_dates:
        for option_type in ['call','put']:
            url = f"https://api.polygon.io/v3/snapshot/options/{symbol}?expiration_date={expiry}?contract_type={option_type}&limit=250&apiKey=A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp&apiKey={KEY}"
            response = execute_polygon_call(url)
            response_data = json.loads(response.text)
            results = response_data['results']
            results_df = pd.DataFrame(results)
            results_df['symbol'] = symbol
            results_df['expiry'] = expiry
            results_df['option_type'] = option_type
            symbol_dfs.append(results_df)
    full_df = pd.concat(symbol_dfs)
    return full_df

def configure_vti_features(df):
    # spy,_ = call_polygon_features(["SPY"], from_stamp, to_stamp, timespan="minute", multiplier="30", hour=hour)
    # spy_features = feature_engineering(spy,datetime.strptime(to_stamp, "%Y-%m-%d"),hour)
    vti = df.loc[df['symbol'] == "VTI"]
    fived = vti["price_5Ddiff"].values[0]
    twentyd = vti["price_20Ddiff"].values[0]
    vti_range = vti["price_range_D"].values[0]
    hour = vti['price_change_absolute_H'].values[0]
    day = vti['price_change_absolute_D'].values[0]
    df['VTI_diff_H'] = (df['price_change_absolute_H'] - hour)/df['price_change_absolute_H']
    df['VTI_diff_D'] = (df['price_change_absolute_D'] - day)/df['price_change_absolute_D']
    df["VTI_5d"] = fived
    df["VTI_20d"] = twentyd 
    df["VTI_5d_diff"] = (df["price_5Ddiff"] - df["VTI_5d"])/df["price_5Ddiff"]
    df["VTI_20d_diff"] = (df["price_20Ddiff"] - df["VTI_20d"])/df["price_20Ddiff"]
    df["VTI_range_vol"] = vti_range
    return df

def calculate_volume_cycle_features(features, cycle_length=14):
    volume_data = features['v']
    # Extract daily and weekly cycles
    daily_cycle = extract_cycle(volume_data.values, cycle_length=cycle_length)
    weekly_cycle = extract_cycle(volume_data.values, cycle_length=cycle_length * 5)
    
    # Compare daily cycle to weekly cycle
    cycle_difference, cycle_z_scores = compare_cycles(daily_cycle, weekly_cycle, features['symbol'])
    if cycle_difference is None:
        return features, True
    # Add cycle-based features
    features['daily_cycle'] = daily_cycle
    features['weekly_cycle'] = weekly_cycle
    features['cycle_difference'] = cycle_difference
    features['volume_cycle_z_scores'] = cycle_z_scores.values

    
    # Add cycle strength features
    features['daily_volume_cycle_strength'] = np.abs(daily_cycle) / np.std(volume_data.values)
    features['weekly_volume_cycle_strength'] = np.abs(weekly_cycle) / np.std(volume_data.values)
    return features, False

# def wavelet_analysis(data, wavelet='db8', max_level=None):
#     if max_level is None:
#         max_level = pywt.dwt_max_level(len(data), pywt.Wavelet(wavelet).dec_len)
    
#     coeffs = pywt.wavedec(data, wavelet, level=max_level)
#     reconstructed = []
#     for i in range(max_level + 1):
#         coeff_list = [np.zeros_like(c) for c in coeffs]
#         coeff_list[i] = coeffs[i]
#         reconstructed.append(pywt.waverec(coeff_list, wavelet))
#     return reconstructed, coeffs

def extract_cycle(data, cycle_length):
    kernel = np.ones(cycle_length) / cycle_length
    extracted_cycle = np.convolve(data, kernel, mode='same')
    return extracted_cycle

def compare_cycles(short_cycle, long_cycle, symbol):
    try:
        difference = short_cycle - long_cycle
    except ValueError:
        return None, None
    window = len(short_cycle) // 10
    rolling_mean = pd.Series(difference).rolling(window=window).mean()
    rolling_std = pd.Series(difference).rolling(window=window).std()
    
    # Add a small epsilon to avoid division by zero
    epsilon = 1e-8
    z_scores = (difference - rolling_mean) / (rolling_std + epsilon)
    
    # Replace infinite values with NaN
    z_scores = z_scores.replace([np.inf, -np.inf], np.nan)
    
    # Forward fill NaN values
    z_scores = z_scores.ffill().bfill()

    
    return difference, z_scores

# def compute_wavelet_energy(coeffs):
#     energy = [np.sum(np.square(c)) for c in coeffs]
#     total_energy = np.sum(energy)
#     return np.array(energy) / total_energy

def wavelet_features_vol(df, volatility_columns=['v', 'range_volatility'], 
                              wavelet='db8', max_level=4):
    features = {}
    
    for col in volatility_columns:
        series = df[col].dropna()
        coeffs = pywt.wavedec(series, wavelet, level=max_level)
        
        # Reconstruct signals at each level
        reconstructed = []
        for i in range(len(coeffs)):
            coeff_copy = [np.zeros_like(c) for c in coeffs]
            coeff_copy[i] = coeffs[i]
            reconstructed.append(pywt.waverec(coeff_copy, wavelet))
        
        for level, signal in enumerate(reconstructed):
            if level == 0:
                suffix = 'smooth'
            else:
                suffix = f'detail_{level}'
            
            # Trend features
            features[f'{col}_{suffix}_trend'] = pd.Series(signal).diff()
            
            # Volatility of the wavelet component
            features[f'{col}_{suffix}_volatility'] = pd.Series(signal).rolling(window=20).std()
            
            # Anomaly detection using Z-score
            z_scores = stats.zscore(signal)
            features[f'{col}_{suffix}_anomaly'] = pd.Series(z_scores)
            
            # Relative strength of the component
            total_energy = np.sum([np.sum(np.abs(c)**2) for c in coeffs])
            component_energy = np.sum(np.abs(coeffs[level])**2)
            features[f'{col}_{suffix}_relative_strength'] = pd.Series(np.full(len(signal), component_energy / total_energy))
    
    # Additional cross-scale features
    for col in volatility_columns:
        for i in range(1, max_level + 1):
            for j in range(i+1, max_level + 1):
                ratio_name = f'{col}_detail_{i}_to_{j}_ratio'
                features[ratio_name] = features[f'{col}_detail_{i}_volatility'] / features[f'{col}_detail_{j}_volatility']
    
    feat_df = pd.DataFrame(features)
    df_len = len(df)
    feat_len = len(feat_df)
    diff = df_len - feat_len
    if diff != 0:
        feat_df = feat_df.iloc[abs(diff):]
        print(feat_df)
    feature_columns = feat_df.columns
    for col in feature_columns:
        df[col] = feat_df[col].values
    return df

# Identify the most recent index where the rolling maximum occurred
# This involves using apply to check backwards from the current point
def get_recent_max_idx(x):
    recent_window = df['value'].iloc[x.name - 30 + 1 : x.name + 1]
    return recent_window.idxmax()

if __name__ == "__main__":
    # import time 
    # dt = time.time()
    # df = pd.DataFrame()
    # build_date_dfs(df, dt)
    calculate_sellby_date(datetime.strptime("2021-06-01","%Y-%m-%d"),4)