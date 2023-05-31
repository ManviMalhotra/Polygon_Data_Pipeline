#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Dependency - 
#pip install "snowflake-connector-python[pandas]"
#pip install polygon-api-client
#pip install polygon
#pip install config
#pip install pandas


# In[17]:


# Importing all the libraries
import pandas as pd
from polygon import ForexClient
from polygon import StocksClient
from datetime import datetime
import pickle
import config
import keyring
from datetime import date
# The Snowflake Connector library.
import snowflake.connector as snow
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
from snowflake.connector.pandas_tools import write_pandas


# In[22]:


# Extracting data 
class data_extract_polygon:
    def __init__(self):
        self.API_KEY = keyring.get_password('polygon','manvi_api')
        self.start = "2022-10-13"
        self.end = "2020-09-13"
        self.fx_client = ForexClient(self.API_KEY)
        self.ss_client = StocksClient(self.API_KEY)

    def from_epoch_to_datetime(self, epoch_time):
        return datetime.fromtimestamp(epoch_time).strftime("%d-%m-%Y %H:%M:%S")

    def get_all_tickers_forex(self,date="2022-09-13"):
        data = self.fx_client.get_grouped_daily_bars(date=date)
    #print(data)
        results = data['results']
    #print(results)
        all_pairs = [pair['T'] for pair in results]
        return all_pairs

    def get_history_day_forex(self,ticker, start, end):
        bars = self.fx_client.get_aggregate_bars(
            symbol=ticker,
            from_date=end,
            to_date=start,
            multiplier=5,
            timespan='day',
            sort = 'asc',
            full_range=True,
            warnings=False
        )
        results = []
        for bar in bars:
            epoch_time = int(str(bar['t'])[:10])
            bar['t'] = self.from_epoch_to_datetime(epoch_time)
            results.append(bar)
        return results
   
    def data_cleaning(self,data):
        count_empty = 0
        count_data = 0
        final_data = {}
        for key, value in data.items():
            if len(value)< 1:
                count_empty+=1
            else:
                final_data[key] = value
                count_data+=1
        final_data2 = []
        for key, value in final_data.items():
            for i in value:
                i['ticker']= key
                final_data2.append(i)
        data = pd.DataFrame(final_data2)
        data.rename(columns = {'v':'Volume', 'vw':'Volume_weighted_avg_pric', 'o':'Opening_Price', 'c':'Closing_Price', 'h':'Highest_Price', 'l':'Lowest_Price', 't':'Start_Timestamp', 'n':'No_of_Transactions', 'ticker':'Ticker'}, inplace = True)
        #print(data.head())
        data.columns = [column_name.upper() for column_name in data.columns]
        #print(data.head())
        # Markets are closed on weekends hence we don't get any data. the below will give an error if run for weekends
        if data.shape[0]> 0:
            data["START_TIMESTAMP"] = pd.to_datetime(data["START_TIMESTAMP"])
        return data
    
    def get_day_data_forex(self, tickers, start, end, verbose=False, dump=False):
        num_tickers = len(tickers)
        all_data = {}
        count = 0
        for ticker in tickers:
            count += 1
            if verbose:
                if count % 50 == 0: print(f"{count}/{num_tickers} complete")
            ticker_hist = self.get_history_day_forex(ticker, start, end)
            all_data[ticker] = ticker_hist

        #if dump:
            #with open("all_data.pkl", "wb") as f:
             #   pickle.dump(all_data, f)
        all_data = self.data_cleaning(all_data)

        return all_data

    def get_all_tickers_stocks(self, date="2022-09-13"):
        data = self.ss_client.get_grouped_daily_bars(date=date)
        # print(data)
        results = data['results']
        # print(results)
        all_pairs = [pair['T'] for pair in results]
        return all_pairs

    def get_history_day_stocks(self, ticker, start, end):
        bars = self.ss_client.get_aggregate_bars(
            symbol=ticker,
            from_date=end,
            to_date=start,
            multiplier=5,
            timespan='day',
            sort='asc',
            full_range=True,
            warnings=False

        )
        results = []
        for bar in bars:
            epoch_time = int(str(bar['t'])[:10])
            bar['t'] = self.from_epoch_to_datetime(epoch_time)
            results.append(bar)
        return results


    def get_day_data_stocks(self, tickers, start, end, verbose=False, dump=False):
        num_tickers = len(tickers)
        all_data = {}
        count = 0
        for ticker in tickers:
            count += 1
            if verbose:
                if count % 50 == 0: print(f"{count}/{num_tickers} complete")
            ticker_hist = self.get_history_day_stocks(ticker, start, end)
            all_data[ticker] = ticker_hist

        # if dump:
        # with open("all_data.pkl", "wb") as f:
        #   pickle.dump(all_data, f)
        all_data = self.data_cleaning(all_data)

        return all_data


# In[12]:


class snowflake_functions:
    def get_private_key(self):
        with open("rsa_key.p8", "rb") as key:
            p_key= serialization.load_pem_private_key(
        key.read(),
        password= b'manvim2', #Setting through ENV Variable 
        backend=default_backend()
    )

        pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
        return pkb
    def connect_to_database(self):
        conn = snow.connect(user="MMALHOTRA",
               account="udwmrtv-mm2",# organisation name-account name
               private_key = self.get_private_key(),
               warehouse="TEST_1",
               database="TRADEDATA",
               schema= "PUBLIC") 

        # Create a cursor object.
        conn.cursor().execute("ALTER SESSION SET QUERY_TAG = 'EndOfDayTradeReports'")
                    
        sql = 'USE WAREHOUSE TRADE_DATA_WH'
        conn.cursor().execute(sql)
        return conn
        
    def push_data(self,daily_data,table_name,type):
        conn = self.connect_to_database()
        write_pandas(conn, daily_data,table_name)
        print(f'successfuly imported EndOfDayTrade {type}')
        cur = conn.cursor()
        # Close connection to database
        conn.close()


# Creating class object
data_extract = data_extract_polygon()
today = date.today()

# Extracting forex tickers and scrapping end of day trade details for the tickers
forex_tickers = data_extract.get_all_tickers_forex()
forex_daily_data = data_extract.get_day_data_forex(forex_tickers , start = today, end = today)

# Pushing forex data to snowflake
if forex_daily_data.shape[0] > 0:
    snowflake_func = snowflake_functions()
    snowflake_func.push_data(forex_daily_data, "FOREX_DATA")

# Extracting stock tickers and scrapping end of day trade details for the tickers
stock_tickers = data_extract.get_all_tickers_stocks()
stock_daily_data = data_extract.get_day_data_stocks(stock_tickers, start = today, end = today

# Pushing stocks data to snowflake
if stock_daily_data.shape[0] > 0:
    snowflake_func = snowflake_functions()
    snowflake_func.push_data(stock_daily_data, "STOCK_DATA")


with open('data_load.txt','a') as f:
    f.write('Forex Data Loading succesful, DateTime: {} No of rows: {}\n'.format(datetime.now(),forex_daily_data.shape[0]))
    f.write('Stock Data Loading succesful, DateTime: {} No of rows: {}\n'.format(datetime.now(), stock_daily_data.shape[0]))


print('Successful')

