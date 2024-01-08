import pandas as pd
import boto3
from helpers.aws import pull_files_s3, get_s3_client
import os
from datetime import timedelta, datetime
import concurrent.futures
import pandas_market_calendars as mcal

s3 = boto3.client('s3')

idx = ["QQQ","SPY","IWM"]

def run_process(date_str):
    try:
        raw = build_pcr_features(date_str)
    except Exception as e:
        print(f"{date_str} {e}")
        build_pcr_features(date_str)
    return raw


def generate_dates_historic_vol(date_str):
    end = datetime.strptime(date_str, "%Y-%m-%d")
    start = end - timedelta(weeks=4)
    to_stamp = end.strftime("%Y-%m-%d")
    hour_stamp = end.strftime("%Y-%m-%d")
    from_stamp = start.strftime("%Y-%m-%d")
    return from_stamp, to_stamp, hour_stamp

def build_pcr_features(date_str):
    hours = ["10","11","12","13","14","15"]
    for hour in hours:
        key_str = date_str.replace("-","/")
        s3 = get_s3_client()
        from_stamp, to_stamp, hour_stamp = generate_dates_historic_vol(date_str)
        raw_pcr_data = pull_pcr_data(from_stamp,to_stamp,hours, current_hour=hour)
        pcr_df = pcr_feature_engineering(raw_pcr_data)
        put_response = s3.put_object(Bucket="inv-alerts", Key=f"idx_alerts/{key_str}/{hour}/pcr_features.csv", Body=pcr_df.to_csv())
    return raw_pcr_data


def pull_pcr_data(from_stamp,to_stamp,hours,current_hour):
    date_list = build_date_list(from_stamp,to_stamp)
    raw_pcr_data = {}
    for symbol in idx:
        dfs = []
        for date_str in date_list:
            for hour in hours:
                if date_str == date_list[-1] and hour > current_hour:
                    continue
                else:
                    key_str = date_str.replace("-","/")
                    try:
                        df = s3.get_object(Bucket="icarus-research-data", Key=f"options_snapshot/{key_str}/{hour}/{symbol}.csv")
                        df = pd.read_csv(df['Body'])
                        df['date'] = key_str
                        df['date_hour'] = f"{key_str}-{hour}"
                    except Exception as e:
                        print(f"options_snapshot/{key_str}/{hour}/{symbol}.csv {e}")
                        continue
                    dfs.append(df)
        full_sym = pd.concat(dfs)
        raw_pcr_data[symbol] = full_sym
    return raw_pcr_data

def build_date_list(from_stamp,to_stamp):
    start_date = datetime.strptime(from_stamp, "%Y-%m-%d")
    end_date = datetime.strptime(to_stamp, "%Y-%m-%d")
    date_diff = end_date - start_date
    numdays = date_diff.days 
    date_list = []
    print(numdays)
    for x in range (0, numdays):
        temp_date = start_date + timedelta(days = x)
        if temp_date.weekday() < 5:
            date_str = temp_date.strftime("%Y-%m-%d")
            date_list.append(date_str)

    return date_list


def pcr_feature_engineering(raw_pcr_data):
    feature_data = {}
    for symbol in idx:
        sym_data = raw_pcr_data[symbol]
        aggregated_df = sym_data.groupby(['date_hour', 'option_type'])['volume'].sum().reset_index()
        pivot_df = aggregated_df.pivot(index='date_hour', columns='option_type', values='volume')
        pivot_df.columns = ['call_volume', 'put_volume']
        pivot_df['total_volume'] =  pivot_df['call_volume'] + pivot_df['put_volume']
        hour = pivot_df.iloc[-1]
        day = pivot_df.iloc[-6:]
        ten_day = pivot_df.iloc[-60:]
        try:
            hour_pcr = hour['put_volume'] / hour['call_volume']
        except Exception as e:
            print(f"{symbol} {e}")
            print(hour)
            continue
        hour_pcr = hour['put_volume'].sum() / hour['call_volume'].sum()
        day_pcr = day['put_volume'].sum() / day['call_volume'].sum()
        ten_day_pcr = ten_day['put_volume'].sum() / ten_day['call_volume'].sum()
        hour_pcr_dff1D = ((hour_pcr - day_pcr)/ day_pcr) * 100
        hour_pcr_dff10D = ((hour_pcr - ten_day_pcr)/ ten_day_pcr) * 100
        day_pcr_dff10D = ((day_pcr - ten_day_pcr)/ ten_day_pcr) * 100
        feature_data[symbol] = [hour_pcr, day_pcr, ten_day_pcr, hour_pcr_dff1D, hour_pcr_dff10D, day_pcr_dff10D]
    feature_df = pd.DataFrame.from_dict(feature_data, orient='index', columns=['hour_pcr', 'day_pcr', 'ten_day_pcr', 'hour_pcr_dff1D', 'hour_pcr_dff10D', 'day_pcr_dff10D'])
    return feature_df



if __name__ == "__main__":
    # build_historic_data(None, None)
    cpu =os.cpu_count()
    date_list = build_date_list("2018-01-02","2023-12-23")        

    with concurrent.futures.ProcessPoolExecutor(max_workers=cpu) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(run_process, date_str) for date_str in date_list]