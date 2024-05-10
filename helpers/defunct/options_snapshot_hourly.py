# import pandas as pd
# import requests 
# import json
# from datetime import timedelta, datetime
# from helpers import data
# import boto3
# import logging
# from botocore.exceptions import ClientError
# import concurrent.futures
# import math
# import numpy as np
# import pandas_market_calendars as mcal
# import os
# import time
# from helpers.constants import ALL_SYM, TRADING_SYMBOLS, WEEKLY_EXP

# nyse = mcal.get_calendar('NYSE')
# holidays = nyse.holidays()
# holidays_multiyear = holidays.holidays

# s3 = boto3.client('s3', aws_access_key_id="AKIAWUN5YYJZHGIGMLQJ", aws_secret_access_key="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")

# def options_snapshot_runner(monday,symbol):
#     fridays = build_days(monday)
#     try:
#         print(symbol)
#         call_tickers, put_tickers = build_options_tickers(symbol, fridays, monday)
#         get_options_snapshot_hist(call_tickers, put_tickers, monday, symbol)
#         print(f"Finished {monday} for {symbol}")
#     except Exception as e:
#         print(f"{symbol} failed at {monday} with: {e}. Retrying")
#         try:
#             call_tickers, put_tickers = build_options_tickers(symbol, fridays, monday)
#             get_options_snapshot_hist(call_tickers, put_tickers, monday, symbol)
#             print(f"Finished {monday} for {symbol}")
#         except Exception as e:
#             print(f"{symbol} failed twice at {monday} with: {e}. Skipping")