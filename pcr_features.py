import pandas as pd
import boto3
from helpers.aws import pull_files_s3, get_s3_client
import os
from datetime import timedelta, datetime
from helpers.data import call_polygon_PCR_price
import concurrent.futures
import pandas_market_calendars as mcal

s3 = boto3.client('s3')

def run_process(date_str):
    try:
        build_pcr_features(date_str)
        print(f"Finished {date_str}")
    except Exception as e:
        print(f"{date_str} {e}")
        build_pcr_features(date_str)
    return "raw"


def generate_dates(date_str):
    end = datetime.strptime(date_str, "%Y-%m-%d")
    start = end - timedelta(weeks=4)
    to_stamp = end.strftime("%Y-%m-%d")
    from_stamp = start.strftime("%Y-%m-%d")
    return from_stamp, to_stamp

def build_pcr_features(date_str):
    hours = ["10","11","12","13","14","15"]
    for hour in hours:
        key_str = date_str.replace("-","/")
        s3 = get_s3_client()
        from_stamp, to_stamp = generate_dates(date_str)

        raw_pcr_data = pull_pcr_data(from_stamp,to_stamp,hours,current_hour=hour)
        pcr_df = pcr_feature_engineering(raw_pcr_data,date_str,hour)
        put_response = s3.put_object(Bucket="inv-alerts", Key=f"bf_alerts/spy_pcr_features/{key_str}/{hour}/pcr_features.csv", Body=pcr_df.to_csv())
    return raw_pcr_data


def pull_pcr_data(from_stamp,to_stamp,hours,current_hour):
    date_list = build_date_list(from_stamp,to_stamp)
    raw_pcr_data = {}
    for symbol in ["SPY"]:
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
    for x in range (0, (numdays+1)):
        temp_date = start_date + timedelta(days = x)
        if temp_date.weekday() < 5:
            date_str = temp_date.strftime("%Y-%m-%d")
            date_list.append(date_str)

    return date_list

def build_distance_metrics_price(sym_data):
    call_df = sym_data[sym_data['option_type'] == 'call']
    put_df = sym_data[sym_data['option_type'] == 'put']
    call_df['in_money'] = call_df['strike'] < call_df['underlying_price']
    put_df['in_money'] = put_df['strike'] > put_df['underlying_price']
    put_df = put_df[put_df['in_money'] == False]
    call_df = call_df[call_df['in_money'] == False]

    call_df['distance'] = abs(call_df['strike'] - call_df['underlying_price'])
    put_df['distance'] = abs(put_df['strike'] - put_df['underlying_price'])

def build_distance_metrics_date(sym_data,date):
    trim_data = sym_data.loc[sym_data['date'] == date.replace('-','/')].reset_index(drop=True)
    print(trim_data['days_to_expiration'].unique())
    call_df = trim_data[trim_data['option_type'] == 'call']
    put_df = trim_data[trim_data['option_type'] == 'put']
    call_df['in_money'] = call_df['strike'] < call_df['underlying_price']
    put_df['in_money'] = put_df['strike'] > put_df['underlying_price']
    put_df = put_df[put_df['in_money'] == False]
    call_df = call_df[call_df['in_money'] == False]


def pcr_feature_engineering(raw_pcr_data,date_str,hour):
    for symbol in ["SPY"]:
        underlying_price = call_polygon_PCR_price(symbol,date_stamp=date_str,timespan="hour",multiplier=1,hour=hour)
        sym_data = raw_pcr_data[symbol]
        sym_data = sym_data[sym_data['volume'] > 0]
        sym_data['underlying_price'] = underlying_price
        sym_data['expiration_date'] = sym_data['symbol'].apply(lambda x: x[-15:-9])
        sym_data['expiration_date'] = pd.to_datetime(sym_data['expiration_date'], format='%y%m%d')
        sym_data['days_to_expiration'] = sym_data.apply(lambda x: (x['expiration_date'] - datetime.strptime(x['date'], "%Y/%m/%d")).days,axis=1)
        sym_data = sym_data.loc[sym_data['days_to_expiration'] > 0].reset_index(drop=True)
        # put_distance_metrics_df, call_distance_metrics_df = build_distance_metrics_price(sym_data)
        # put_distance_metrics_df, call_distance_metrics_df = build_distance_metrics_date(sym_data,date_str)
        aggregated_dh_df = sym_data.groupby(['date_hour', 'option_type'])['volume'].sum().reset_index()
        aggregated_date_df = sym_data.groupby(['date', 'option_type'])['volume'].sum().reset_index()
        pivot_dh_df = aggregated_dh_df.pivot(index='date_hour', columns='option_type', values='volume')
        pivot_dh_df.columns = ['call_volume', 'put_volume']
        pivot_dh_df['total_volume_H'] =  pivot_dh_df['call_volume'] + pivot_dh_df['put_volume']
        pivot_date_df = aggregated_date_df.pivot(index='date', columns='option_type', values='volume')
        pivot_date_df.columns = ['call_volume', 'put_volume']
        pivot_date_df['total_volume_D'] =  pivot_date_df['call_volume'] + pivot_date_df['put_volume']

        ## PCR Features
        pivot_date_df['PCR_D'] = pivot_date_df['put_volume'] / pivot_date_df['call_volume']
        pivot_dh_df['PCR_H'] = pivot_dh_df['put_volume'] / pivot_dh_df['call_volume']
        pivot_date_df['PCR_D_change'] = pivot_date_df['PCR_D'].pct_change()
        pivot_dh_df['PCR_H_change'] = pivot_dh_df['PCR_H'].pct_change()
        pivot_date_df['PCR_D_change_10std'] = pivot_date_df['PCR_D_change'].rolling(window=10).std()
        pivot_dh_df['PCR_H_change_10std'] = pivot_dh_df['PCR_H_change'].rolling(window=10).std()
        pivot_date_df['PCR_D_change_10std_diff'] = (pivot_date_df['PCR_D_change']-pivot_date_df['PCR_D_change_10std'])/pivot_date_df['PCR_D_change_10std']
        pivot_dh_df['PCR_H_change_10std_diff'] = (pivot_dh_df['PCR_H_change']-pivot_dh_df['PCR_H_change_10std'])/pivot_dh_df['PCR_H_change_10std']
        pivot_date_df['PCR_D_change5'] = pivot_date_df['PCR_D'].pct_change(5)
        pivot_dh_df['PCR_H_change8'] = pivot_dh_df['PCR_H'].pct_change(8)
        pivot_date_df['PCR_D_10MA'] = pivot_date_df['PCR_D'].rolling(window=10).mean()
        pivot_dh_df['PCR_H_10MA'] = pivot_dh_df['PCR_H'].rolling(window=10).mean()
        pivot_date_df['PCR_D_10MA_diff'] = (pivot_date_df['PCR_D']-pivot_date_df['PCR_D_10MA'])/pivot_date_df['PCR_D_10MA']
        pivot_dh_df['PCR_H_10MA_diff'] = (pivot_dh_df['PCR_H']-pivot_dh_df['PCR_H_10MA'])/pivot_dh_df['PCR_H_10MA']

        ## Options Volume Features
        pivot_dh_df['total_volume_H_change'] = pivot_dh_df['total_volume_H'].pct_change()
        pivot_dh_df['total_volume_H_change8'] = pivot_dh_df['total_volume_H'].pct_change(8)
        pivot_dh_df['total_volume_H_8MA'] = pivot_dh_df['total_volume_H'].rolling(window=8).mean()
        pivot_dh_df['total_volume_H_8MA_diff'] = (pivot_dh_df['total_volume_H']-pivot_dh_df['total_volume_H_8MA'])/pivot_dh_df['total_volume_H_8MA']
        pivot_dh_df['total_volume_change_10std'] = pivot_dh_df['total_volume_H_change'].rolling(window=10).std()
        pivot_dh_df['total_volume_change_10std_diff'] = (pivot_dh_df['total_volume_H_change']-pivot_dh_df['total_volume_change_10std'])/pivot_dh_df['total_volume_change_10std']


        pivot_dh_df.drop(['call_volume','put_volume'],axis=1,inplace=True)
        pivot_date_df.drop(['call_volume','put_volume'],axis=1,inplace=True)
        features_H = pivot_dh_df.iloc[-1]
        features_D = pivot_date_df.iloc[-1]
        # feature_data = features_H.append(features_D)
        feature_data = {**features_H.to_dict(), **features_D.to_dict()}

        # Convert the dictionary to a DataFrame
        df = pd.DataFrame([feature_data])
        df['date'] = date_str
        df['hour'] = hour
        df['PCR_HD_diff'] = df['PCR_H'] - df['PCR_D']

        
    return df

def extract_strike_price(option_symbol,option_type):
    # Find the position of 'C' or 'P' indicating the start of the strike price
    if option_type == 'call':
        strike_price_start = option_symbol.split('C')[1]
    elif option_type == 'put':
        strike_price_start = option_symbol.split('P')[1]

    cleaned_strike = strike_price_start.lstrip('0')
    
    # Convert to integer and then to a float by dividing by 1000
    try:
        strike_price = float(strike_price_part) / 1000
    except ValueError:
        raise ValueError("Strike price is not in a valid numeric format")
    
    return strike_price

def add_pcr_features(date_str):
    hours = ["10","11","12","13","14","15"]
    key_str = date_str.replace("-","/")
    s3 = boto3.client('s3')
    for alert_type in ['cdvol_gainers','cdvol_losers']:
        for hour in hours:
            try:
                pcr_df = s3.get_object(Bucket="inv-alerts", Key=f"bf_alerts/spy_pcr_features/{key_str}/{hour}/pcr_features.csv")
                pcr_df = pd.read_csv(pcr_df['Body'])
            except Exception as e:
                print(f"pcr data {date_str} {e}")
            try:
                    alert_df = s3.get_object(Bucket="inv-alerts", Key=f"sorted_alerts/{key_str}/{alert_type}/{hour}.csv")
                    alert_df = pd.read_csv(alert_df['Body'])
            except Exception as e:
                print(f"sorted_alerts/{key_str}/{alert_type}/{hour}.csv {e}")
            
            try:
                alert_df['PCR_H'] = pcr_df['PCR_H'].values[0]
                alert_df['PCR_D'] = pcr_df['PCR_D'].values[0]
                alert_df['PCR_HD_diff'] = pcr_df['PCR_HD_diff'].values[0]
                alert_df['PCR_H_change'] = pcr_df['PCR_H_change'].values[0]
                alert_df['PCR_H_change_10std_diff'] = pcr_df['PCR_H_change_10std_diff'].values[0]
                alert_df['PCR_H_change8'] = pcr_df['PCR_H_change8'].values[0]
                alert_df['PCR_H_10MA_diff'] = pcr_df['PCR_H_10MA_diff'].values[0]
                alert_df['total_volume_H_change'] = pcr_df['total_volume_H_change'].values[0]
                alert_df['total_volume_H_change8'] = pcr_df['total_volume_H_change8'].values[0]
                alert_df['total_volume_H_8MA_diff'] = pcr_df['total_volume_H_8MA_diff'].values[0]
                alert_df['total_volume_change_10std_diff'] = pcr_df['total_volume_change_10std_diff'].values[0]
                alert_df['PCR_D_change'] = pcr_df['PCR_D_change'].values[0]
                alert_df['PCR_D_change_10std_diff'] = pcr_df['PCR_D_change_10std_diff'].values[0]
                alert_df['PCR_D_change5'] = pcr_df['PCR_D_change5'].values[0]
                alert_df['PCR_D_10MA_diff'] = pcr_df['PCR_D_10MA_diff'].values[0]
                put_response = s3.put_object(Bucket="inv-alerts", Key=f"sorted_alerts/pcr_features/{key_str}/{alert_type}/{hour}.csv", Body=alert_df.to_csv())
            except Exception as e:
                print(f"{date_str} {e} features")
                continue


        




if __name__ == "__main__":
    # build_historic_data(None, None)
    cpu =os.cpu_count()
    date_list = build_date_list("2015-02-10","2024-04-16")        

    # add_pcr_features("2024-03-21")
    with concurrent.futures.ProcessPoolExecutor(max_workers=cpu) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(add_pcr_features, date_str) for date_str in date_list]