import requests
import json
import pandas as pd
from datetime import datetime, timedelta, time
import pandas_ta as ta
import numpy as np
import pytz
import warnings
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

warnings.filterwarnings("ignore",category=FutureWarning)

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
    t = row['t']
    date = row['date']
    from_stamp = date.strftime("%Y-%m-%d")
    date = date.astimezone(pytz.timezone('US/Eastern'))
    aggs = call_polygon_price(row['symbol'], from_stamp, "hour", 1, row['hour'])
    one_day, three_day = build_date_dfs(aggs, t)
    open = one_day.head(1)['o'].values[0]
    one_c = one_day.tail(1)['c'].values[0]
    one_h = one_day['h'].max()
    one_l = one_day['l'].min()
    three_c = three_day.tail(1)['c'].values[0]
    three_h = three_day['h'].max()
    three_l = three_day['l'].min()
    one_high = (one_h - open)/ open
    one_low = (one_l - open)/ open
    one_pct = (one_c - row['c'])/row['c']
    three_high = (three_h - open)/ open
    three_low = (three_l - open)/ open
    three_pct = (three_c - row['c'])/row['c']
    return {"one_max": one_high, "one_min": one_low, "one_pct": one_pct, "three_max": three_high, "three_min": three_low, "three_pct": three_pct,"symbol": row['symbol']}

def build_date_dfs(df, t):
    date = convert_timestamp_est(t)
    sell_1d = calculate_sellby_date(date, 2)
    sell_3d = calculate_sellby_date(date, 4)
    one_day_df = df.loc[df['date'] < sell_1d]
    three_day_df = df.loc[df['date'] < sell_3d]
    return one_day_df, three_day_df
    
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

def call_polygon_histH(symbol_list, from_stamp, to_stamp, timespan, multiplier):
    payload={}
    headers = {}
    dfs = []
    error_list = []

    if timespan == "minute":
        from_stamp = to_stamp
    for symbol in symbol_list:
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={KEY}"

        response = execute_polygon_call(url)

        try:
            response_data = json.loads(response.text)
            results = response_data['results']
        except:
            continue
        results_df = pd.DataFrame(results)
        results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
        results_df['date'] = results_df['t'].apply(lambda x: convert_timestamp_est(x))
        results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
        results_df['minute'] = results_df['date'].apply(lambda x: x.minute)
        results_df['symbol'] = symbol
        trimmed_df = results_df.loc[results_df['hour'].isin([9,10,11,12,13,14,15])]
        filtered_df = trimmed_df.loc[~((trimmed_df['hour'] == 9) & (trimmed_df['minute'] < 30))]
        dfs.append(filtered_df)

    return dfs, error_list

def call_polygon_vol(symbol_list, from_stamp, to_stamp, timespan, multiplier,hour):
    payload={}
    headers = {}
    dfs = []
    trading_hours = [9,10,11,12,13,14,15]
    error_list = []

    year, month, day = to_stamp.split("-")
    current_date = datetime(int(year), int(month), int(day), int(hour),tzinfo=pytz.timezone('US/Eastern'))

    for symbol in symbol_list:
        data = []
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={KEY}"
        with requests.Session() as session:
            next_url = url
            while next_url:
                response = execute_polygon_call(next_url)
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
                results_df['symbol'] = symbol
                trimmed_df = results_df.loc[results_df['hour'].isin(trading_hours)]
                filtered_df = trimmed_df.loc[~((trimmed_df['hour'] == 9) & (trimmed_df['minute'] < 30))]
                filtered_df = filtered_df.loc[filtered_df['date'] <= current_date]
                data.append(filtered_df)
                try:
                    next_url = response_data['next_url']
                except:
                    next_url = None
            full_df = pd.concat(data, ignore_index=True)
            dfs.append(full_df)

    return dfs, error_list

def call_polygon_histD(symbol_list, from_stamp, to_stamp, timespan, multiplier):
    payload={}
    headers = {}
    dfs = []
    trading_hours = [9,10,11,12,13,14,15]
    
    error_list = []

    for symbol in symbol_list:
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={KEY}"

        response = execute_polygon_call(url)

        try:
            response_data = json.loads(response.text)
            results = response_data['results']
        except:
            continue
        results_df = pd.DataFrame(results)
        results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
        results_df['date'] = results_df['t'].apply(lambda x:convert_timestamp_est(x))
        results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
        results_df['minute'] = results_df['date'].apply(lambda x: x.minute)
        results_df['symbol'] = symbol
        results_df['day_of_week'] = results_df['date'].apply(lambda x: x.weekday())
        trimmed_df = results_df.loc[results_df['hour'].isin(trading_hours)]
        filtered_df = trimmed_df.loc[~((trimmed_df['hour'] == 9) & (trimmed_df['minute'] < 30))]
        filtered_df = filtered_df.loc[filtered_df['day_of_week'] < 5]
        filtered_df.set_index('date',inplace=True)
        agg_dict = {
            'v': 'sum',
            'o': 'first',
            'c': 'last',
            'h': 'max',
            'l': 'min'
        }

        # Perform resampling and aggregation
        daily_stats = filtered_df.resample('D').agg(agg_dict)
        daily_stats.dropna(inplace=True)
        daily_stats['date_stamp'] = daily_stats.index
        daily_stats['hour'] = 0
        daily_stats['symbol'] = symbol
        daily_stats['t'] = 0
        daily_stats.rename(columns={"date_stamp": "date"}, inplace=True)
        daily_stats.reset_index(drop=True, inplace=True)
        dfs.append(daily_stats)

    return dfs, error_list

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


def call_polygon_price_day(symbol, from_stamp, to_stamp, timespan, multiplier):
    payload={}
    headers = {}
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={KEY}"

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
            # print(f"{e} for {symbol}")
            continue
        values.append({"high": results_df['h'].max(),"low": results_df['l'].min(),"volume": results_df['v'].sum(),"symbol": symbol})

    full_df = pd.DataFrame.from_dict(values)
    return full_df

def calc_vdiff(row):
    try:
        to_stamp = datetime.strptime(row['date'], '%Y-%m-%d %H:%M:%S')
        from_stamp = to_stamp - timedelta(days=10)
        from_str = from_stamp.strftime("%Y-%m-%d")
        to_str = row['date'].split(" ")[0]
        aggs,_ = call_polygon_hist([row['symbol']], from_str, to_str, "day", 1)
        v = aggs.iloc[-1]['v']
        v_1 = aggs.iloc[-2]['v']
        v_1_avg = (v_1/7)
        v_avg = (v/(abs(9-row['hour'])+1))
        v_diff_pct = (v_avg - v_1_avg) / v_1_avg
        return v_diff_pct
    except Exception as e:
        print(e)
        print(row['symbol'])
        print('vdiff')
        return 0.111021999
    
def calc_vdiff_pipeline(volumes, hour):
    metrics = []
    for index ,v in enumerate(volumes):
        if index == 0 or index == 1:
            metrics.append(0.111021999)
            continue
        else:
            v_1 = volumes[index-1]
            v_1_avg = (v_1/7)
            v_avg = (v/(abs(9-int(hour))))
            v_diff_pct = (v_avg - v_1_avg) / v_1_avg
            metrics.append(v_diff_pct)
    return metrics

def create_adjusted_volume(volumes, hour):
    metrics = []
    for index ,v in enumerate(volumes):
        if index == 0 or index == 1:
            metrics.append(0.111021999)
            continue
        else:
            v_avg = (v/(abs(9-int(hour))))
            adj_v = v_avg * 7
            metrics.append(adj_v)
    return metrics
   

def build_analytics(aggregates, hour):
    indicators = []
    for d in aggregates:
        try: 
            d['price7'] = ta.slope(d['c'],7)    
            d['price14'] = ta.slope(d['c'],14) 
            d['adjusted_volume'] = create_adjusted_volume(d['v'].tolist(), hour)
            d['vol7'] = ta.slope(d['adjusted_volume'],7)    
            d['vol14'] = ta.slope(d['adjusted_volume'],14)
            d['rsi'] = ta.rsi(d['c'])
            d['rsi3'] = ta.rsi(d['c'],length=3)
            d['rsi5'] = ta.rsi(d['c'],length=5)
            d['roc'] = ta.roc(d['c'])
            d['roc3'] = ta.roc(d['c'],length=3)
            d['roc5'] = ta.roc(d['c'],length=5)
            d['threeD_returns_close'] = d['c'].pct_change(3)
            d['oneD_returns_close'] = d['c'].pct_change(1)
            d['range_vol'] = (d['h'] - d['l'])/ d['c']
            d['range_vol5MA'] = d['range_vol'].rolling(5).mean()
            d['range_vol10MA'] = d['range_vol'].rolling(10).mean()
            d['range_vol25MA'] = d['range_vol'].rolling(25).mean()
            d['oneD_stddev50'] = np.std(d['oneD_returns_close'])
            d['threeD_stddev50'] = np.std(d['threeD_returns_close'])
            d['cmf'] = ta.cmf(d['h'], d['l'], d['c'], d['v'])
            try:
                d['close_diff'] = d['c'].pct_change()
            except:
                d['close_diff'] = 0
            d['close_diff3'] = d['c'].pct_change(3)
            d['close_diff5'] = d['c'].pct_change(5)
            d['v_diff_pct'] = calc_vdiff_pipeline(d['v'].tolist(), hour)
            adx = ta.adx(d['h'],d['l'],d['c'])
            d['adx'] = adx['ADX_14']
            d['volume_10MA'] = d['adjusted_volume'].rolling(10).mean()
            d['volume_25MA'] = d['adjusted_volume'].rolling(25).mean()
            d['price_10MA'] = d['c'].rolling(10).mean()
            d['price_25MA'] = d['c'].rolling(25).mean()
            d['volume_10DDiff'] = d.apply(lambda x: ((x.adjusted_volume - x.volume_10MA)/x.volume_10MA)*100, axis=1)
            d['volume_25DDiff'] = d.apply(lambda x: ((x.adjusted_volume - x.volume_25MA)/x.volume_25MA)*100, axis=1)
            d['price_10DDiff'] = d.apply(lambda x: ((x.c - x.price_10MA)/x.price_10MA)*100, axis=1)
            d['price_25DDiff'] = d.apply(lambda x: ((x.c - x.price_25MA)/x.price_25MA)*100, axis=1)
            # macd = ta.macd(d['c'])
            # d['macd'] = macd.MACD_12_26_9
            indicators.append(d)
        except Exception as e:
            print(d.symbol)
            print('')
            print(f"In Aggs: {e}")
            indicators.append(d)
            continue
        
        
    
    df = pd.concat(indicators).round(3)

    return df

def call_polygon_spy(from_stamp, to_stamp, timespan, multiplier):   
    trading_hours = [9,10,11,12,13,14,15]
    url = f"https://api.polygon.io/v2/aggs/ticker/SPY/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={KEY}"

    response = execute_polygon_call(url)

    response_data = json.loads(response.text)
    results = response_data['results']

    results_df = pd.DataFrame(results)
    results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
    results_df['date'] = results_df['t'].apply(lambda x: convert_timestamp_est(x))
    results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
    results_df['minute'] = results_df['date'].apply(lambda x: x.minute)
    results_df['symbol'] = "SPY"
    results_df = results_df.loc[results_df['hour'].isin(trading_hours)]
    results_df = results_df.loc[~((results_df['hour'] == 9) & (results_df['minute'] < 30))]
    return results_df['c'].to_list()

def call_polygon_spyH(from_stamp, to_stamp, timespan, multiplier, hour):
    url = f"https://api.polygon.io/v2/aggs/ticker/SPY/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={KEY}"

    response = execute_polygon_call(url)

    response_data = json.loads(response.text)
    results = response_data['results']

    results_df = pd.DataFrame(results)
    results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
    results_df['date'] = results_df['t'].apply(lambda x: convert_timestamp_est(x))
    results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
    results_df['symbol'] = "SPY"
    results_df = results_df.loc[results_df['hour'] == int(hour-1)]


    return results_df['c']

def build_spy_features(df, spy_aggs, current_spy):
    SPY_diff   = (current_spy - spy_aggs.iloc[-2])/spy_aggs.iloc[-2]
    SPY_diff3  = (current_spy - spy_aggs.iloc[-4])/spy_aggs.iloc[-4]
    SPY_diff5  = (current_spy - spy_aggs.iloc[-6])/spy_aggs.iloc[-6]
    df['SPY_diff'] = (((df['close_diff']/100) - SPY_diff)/SPY_diff)
    df['SPY_diff3'] = (((df['close_diff']/100) - SPY_diff3)/SPY_diff3)
    df['SPY_diff5'] = (((df['close_diff']/100) - SPY_diff5)/SPY_diff5)
    df['SPY_1D'] = SPY_diff
    df['SPY_3D'] = SPY_diff3
    df['SPY_5D'] = SPY_diff5
    return df

def vol_feature_engineering(df, Min_aggs,Thirty_aggs):
    features = []
    agg_dict = {
            'v': 'sum',
            'o': 'first',
            'c': 'last',
            'h': 'max',
            'l': 'min'
        }
    
    for index,min_aggs in enumerate(Min_aggs):
        print(index)
        min_aggs.reset_index(drop=True,inplace=True)
        thirty_aggs = Thirty_aggs[index]
        thirty_aggs.set_index('date',inplace=True)

        # Perform resampling and aggregation
        hour_aggs = thirty_aggs.resample('H').agg(agg_dict)
        daily_aggs = thirty_aggs.resample('D').agg(agg_dict)

        hour_aggs.dropna(inplace=True)
        daily_aggs.dropna(inplace=True)

        min_aggs['price_change'] = abs(min_aggs['c'].pct_change())
        min_aggs['volume_change'] = abs(min_aggs['v'].pct_change())
        hour_aggs['price_change'] = abs(hour_aggs['c'].pct_change())
        hour_aggs['volume_change'] = abs(hour_aggs['v'].pct_change())
        daily_aggs['price_change'] = abs(daily_aggs['c'].pct_change())
        daily_aggs['volume_change'] = abs(daily_aggs['v'].pct_change())

        min_aggs['return_vol_15M'] = min_aggs['price_change'].rolling(window=15).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['volume_vol_15M'] = min_aggs['volume_change'].rolling(window=15).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['return_vol_30M'] = min_aggs['price_change'].rolling(window=30).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['volume_vol_30M'] = min_aggs['volume_change'].rolling(window=30).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['return_vol_60M'] = min_aggs['price_change'].rolling(window=60).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['volume_vol_60M'] = min_aggs['volume_change'].rolling(window=60).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['return_vol_120M'] = min_aggs['price_change'].rolling(window=120).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['volume_vol_120M'] = min_aggs['volume_change'].rolling(window=120).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['return_vol_240M'] = min_aggs['price_change'].rolling(window=240).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['volume_vol_240M'] = min_aggs['volume_change'].rolling(window=240).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['return_vol_450M'] = min_aggs['price_change'].rolling(window=450).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['volume_vol_450M'] = min_aggs['volume_change'].rolling(window=450).apply(lambda x: abs(x).mean(), raw=True)
        hour_aggs['return_vol_4H'] = hour_aggs['price_change'].rolling(window=4).apply(lambda x: abs(x).mean(), raw=True)
        hour_aggs['return_vol_8H'] = hour_aggs['price_change'].rolling(window=8).apply(lambda x: abs(x).mean(), raw=True)
        hour_aggs['return_vol_16H'] = hour_aggs['price_change'].rolling(window=16).apply(lambda x: abs(x).mean(), raw=True)
        hour_aggs['volume_vol_4H'] = hour_aggs['volume_change'].rolling(window=4).apply(lambda x: abs(x).mean(), raw=True)
        hour_aggs['volume_vol_8H'] = hour_aggs['volume_change'].rolling(window=8).apply(lambda x: abs(x).mean(), raw=True)
        hour_aggs['volume_vol_16H'] = hour_aggs['volume_change'].rolling(window=16).apply(lambda x: abs(x).mean(), raw=True)
        daily_aggs['return_vol_3D'] = daily_aggs['price_change'].rolling(window=3).apply(lambda x: abs(x).mean(), raw=True)
        daily_aggs['return_vol_5D'] = daily_aggs['price_change'].rolling(window=5).apply(lambda x: abs(x).mean(), raw=True)
        daily_aggs['return_vol_10D'] = daily_aggs['price_change'].rolling(window=10).apply(lambda x: abs(x).mean(), raw=True)
        daily_aggs['return_vol_30D'] = daily_aggs['price_change'].rolling(window=30).apply(lambda x: abs(x).mean(), raw=True)
        daily_aggs['volume_vol_3D'] = daily_aggs['volume_change'].rolling(window=3).apply(lambda x: abs(x).mean(), raw=True)
        daily_aggs['volume_vol_5D'] = daily_aggs['volume_change'].rolling(window=5).apply(lambda x: abs(x).mean(), raw=True)
        daily_aggs['volume_vol_10D'] = daily_aggs['volume_change'].rolling(window=10).apply(lambda x: abs(x).mean(), raw=True)
        daily_aggs['volume_vol_30D'] = daily_aggs['volume_change'].rolling(window=30).apply(lambda x: abs(x).mean(), raw=True)

        min_aggs['15min_vol_diff'] = min_aggs['return_vol_15M'] - min_aggs['return_vol_240M']
        min_aggs['15min_vol_diff_pct'] = min_aggs['15min_vol_diff']/min_aggs['return_vol_240M']
        min_aggs['min_vol_diff'] = min_aggs['return_vol_30M'] - min_aggs['return_vol_240M']
        min_aggs['min_vol_diff_pct'] = min_aggs['min_vol_diff']/min_aggs['return_vol_240M']
        hour_aggs['hour_vol_diff'] = hour_aggs['return_vol_4H'] - hour_aggs['return_vol_16H']
        hour_aggs['hour_vol_diff_pct'] = hour_aggs['hour_vol_diff']/hour_aggs['return_vol_16H']
        daily_aggs['daily_vol_diff'] = daily_aggs['return_vol_3D'] - daily_aggs['return_vol_10D']
        daily_aggs['daily_vol_diff_pct'] = daily_aggs['daily_vol_diff']/daily_aggs['return_vol_10D']
        daily_aggs['daily_vol_diff30'] = daily_aggs['return_vol_3D'] - daily_aggs['return_vol_30D']
        daily_aggs['daily_vol_diff_pct30'] = daily_aggs['daily_vol_diff30']/daily_aggs['return_vol_30D']
        min_aggs['min_volume_vol_diff'] = min_aggs['volume_vol_240M'] - min_aggs['volume_vol_450M']
        min_aggs['min_volume_vol_diff_pct'] = min_aggs['min_volume_vol_diff']/min_aggs['volume_vol_450M']
        hour_aggs['hour_volume_vol_diff'] = hour_aggs['volume_vol_8H'] - hour_aggs['volume_vol_16H']
        hour_aggs['hour_volume_vol_diff_pct'] = hour_aggs['hour_volume_vol_diff']/hour_aggs['volume_vol_16H']
        daily_aggs['daily_volume_vol_diff'] = daily_aggs['volume_vol_3D'] - daily_aggs['volume_vol_10D']
        daily_aggs['daily_volume_vol_diff_pct'] = daily_aggs['daily_volume_vol_diff']/daily_aggs['volume_vol_10D']
        daily_aggs['daily_volume_vol_diff30'] = daily_aggs['volume_vol_3D'] - daily_aggs['volume_vol_30D']
        daily_aggs['daily_volume_vol_diff_pct30'] = daily_aggs['daily_volume_vol_diff30']/daily_aggs['volume_vol_30D']

        min_features = min_aggs.iloc[-1]
        hour_features = hour_aggs.iloc[-1]
        daily_features = daily_aggs.iloc[-1]
        min_features.drop(['t','o','h','l','v','c','price_change','volume_change','hour','minute','date'], inplace=True)
        hour_features.drop(['o','h','l','v','c','price_change','volume_change'], inplace=True)
        daily_features.drop(['o','h','l','v','c','price_change','volume_change'], inplace=True)
        df_combined = pd.concat([min_features, hour_features, daily_features])
        features.append(df_combined)
    
    features_df = pd.DataFrame(features)
    results_df = pd.merge(df, features_df, on=['symbol'], how='outer')
    return results_df


def convert_timestamp_est(timestamp):
    # Create a naive datetime object from the UNIX timestamp
    dt_naive = datetime.utcfromtimestamp(timestamp)
    # Convert the naive datetime object to a timezone-aware one (UTC)
    dt_utc = pytz.utc.localize(dt_naive)
    # Convert the UTC datetime to EST
    dt_est = dt_utc.astimezone(pytz.timezone('US/Eastern'))
    
    return dt_est

if __name__ == "__main__":
    import time 
    dt = time.time()
    df = pd.DataFrame()
    build_date_dfs(df, dt)