import logging.config
import json
import sys
from dotenv import load_dotenv
import os
from datetime import datetime as dt
import pandas as pd
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from psycopg2 import extras
from functools import wraps

load_dotenv()
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('tracer')


class bulk_insert:

    def __init__(self):
        pass
    
    #principal decorator
    def consuming_data(function):
        try:
            @wraps(function)
            def wrapper(self, *args, **kwargs):

                # decorator  to insert data in db
                query3 = ""
                query4 = ""
                # unpacking objects from function call
                df1 = function(self, *args, **kwargs)
                cols = []
                # assigning columns to filter from dataframe
                for items in df1.columns:
                    cols.append(items)
                df2 = df1[cols[5:]]
                df3 = df2.copy()
                # performing operation to convert rows from dataframe in columns with json values
                for row in df2.iterrows():
                    df3['json_response'] = df2.apply(lambda x: json.dumps(x.to_dict()), axis=1)

                # capturing market data with usd current price data
                df4 = df3[["market_data"]]
                df4 = pd.DataFrame(df4)
                df4 = df4.rename(columns={"market_data": "price_data"})
                # looping throught key in market_data(price_data) series for usd current price calculation
                c = 0
                total_values = []
                total_index = []
                for item in df4.price_data:
                    for k in item.keys():
                        if k == "current_price":
                            k = "current_price"
                            v = item[k]  # key values with usd current price
                            total_values.append(v)
                            total_index.append(c)  # index assignation from every value in df
                            c = c + 1  # counter to find the index data in df

                # assigning usd currency data into df
                df5 = pd.DataFrame(total_values, total_index)
                df5 = df5[["usd"]].astype(object)

                # concatenation operations to filter final dataframes
                df5 = pd.concat([df5, df3], axis=1).rename(columns={"usd": "usd_current_price"}).reset_index(drop=True)
                df6 = df1[["date_info", "month_info", "id", "symbol", "name"]]
                df6 = pd.concat([df6, df5], axis=1)
                df7 = df6[["id", "date_info", "usd_current_price"]]
                df7 = df7.reset_index(drop=True)
                df7["year"] = df7["date_info"].apply(lambda x: dt.strftime(x, "%Y"))
                df7["month"] = df7["date_info"].apply(lambda x: dt.strftime(x, "%m"))
                df7 = df7.groupby(['id', 'year', 'month'])['usd_current_price'].agg(min_price='min',
                                                                                    max_price='max').reset_index()
                df8 = df6[["id", "usd_current_price", "date_info", "json_response"]]

                # querys creation with the postgresql actual connection
                user = os.getenv('user')
                passw = os.getenv('pass')
                host = os.getenv('host')

                conn = psycopg2.connect(database='coins_detail', user=user, password=passw, host=host,
                                        port='5432')
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cursor_ = conn.cursor()
                df8_query = f"""insert into coins_detail(id,usd_current_price ,date_info, json_response) values (%s,%s,%s,%s)"""
                df7_query = f"""insert into coins_price_detail(id, year, month,min_price, max_price) values (%s,%s,%s,%s,%s)"""

                # executing querys to clean and create final tables in postgresql database
                psycopg2.extras.execute_batch(cur=cursor_, sql=df8_query, argslist=df8.values)
                psycopg2.extras.execute_batch(cur=cursor_, sql=df7_query, argslist=df7.values)
                print("----please wait a moment..cleaning operation on tables....---")
                path = os.path.abspath("local_queries.json")
                with open(path, "r") as json_file:
                    json_queries = json.load(json_file)
                for key in json_queries.keys():
                    if "q3" in key:
                        query3 = json_queries[key]
                    if "q4" in key:
                        query4 = json_queries[key]
                cursor_.execute(query3)
                cursor_.execute(query4)
                cursor_.close()
                conn.close()

            return wrapper
        except Exception:
            logger.error("Some error occurred during the workload")
            raise sys.exit("exiting the program..")
