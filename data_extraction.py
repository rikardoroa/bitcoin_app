#script by rikardoroa
import logging.config
import json
import sys
import requests
from dotenv import load_dotenv
import os
import re
from datetime import datetime as dt
import pandas as pd
import datetime
from bulk_insertion import bulk_insert

load_dotenv()
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('tracer')




class data_extraction:

    def __init__(self):
        self.df4 = pd.DataFrame()
        self.df3 = pd.DataFrame()
        self.data = list
        self.pattern = '^[\d]{4}-[\d]{2}-[\d]{2}[?=,][a-z]*$'
        self.pattern_d = '^[\d]{4}-[\d]{2}-[\d]{2}$'
        self.pattern_nd = '^[\d]{4}-[\d]{2}-[\d]{2}[,]$'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 ''(KHTML, like Gecko) '
                          'Chrome/50.0.2661.102 Safari/537.36'}

    def get_coin_date(self):
        while True:
            try:
                # console input data, coin and date
                data_intro = input("enter a valid data in format YYYY-MM-DD and COIN name separated by comma:")
                print("------downloading data...please wait a moment------")
                # validating if pattern match condition
                if re.match(self.pattern_nd, data_intro) or not re.match(self.pattern, data_intro):
                    logger.error("please provide the correct info, try again!")
                # if not math applying date conversion string  in format dd-mm-yyyy
                else:
                    # conversion
                    def conversion(x):
                        x = dt.strptime(x, "%Y-%m-%d")
                        x = x.strftime("%d-%m-%Y")
                        return x

                    # applying lambda for date conversion, validating pattern
                    data_intro = data_intro.split(",")[:2]
                    self.data = list(map(lambda x: conversion(x) if re.match(self.pattern_d, x) else x, data_intro))
                    # returning final data
                    return self.data
            # logging errors messages
            except (EOFError, SystemExit, KeyboardInterrupt):
                logger.error("Some error during execution, please review! or you pressed CTRL - C")
                raise sys.exit("CTRL- C PRESSED!")

    @bulk_insert.consuming_data
    def get_request_data(self):
        try:
            json_initial_payload = []
            print("---------------------executing---------------------")
            self.data = self.data[::-1]
            path = os.path.abspath(f"json_data_s_" + self.data[1] + "_" + self.data[0] + "_.json")
            # validating data  and requesting the json data from url
            url = f'https://api.coingecko.com/api/v3/coins/{self.data[0]}/history?date={self.data[1]}'
            r = requests.get(url=url, headers=self.headers, verify=True, stream=True)
            body = r.content.decode("utf-8")
            payload = json.loads(body)
            json_initial_payload.append(payload)
            self.df4 = pd.DataFrame(json_initial_payload)
            df = pd.DataFrame([self.data[1]])
            df = df.rename(columns={0: 'date_info'}).reset_index(drop=True)
            df['date_info'] = df['date_info'].apply(lambda x: self.df_conversion(x))
            df['month_info'] = df['date_info'].apply(lambda x: dt.strftime(x, "%m"))
            df = df.reset_index(drop=True, inplace=False)
            self.df4 = pd.concat([df, self.df4], axis=1).reset_index(drop=True)
            # storing the data in the defined path
            with open(path, "w") as json_file:
                json.dump(payload, json_file)
            return self.df4
        except IndexError:
            logger.error("Some errors occurred writing the payload data:")
        except (KeyboardInterrupt, EOFError):
            logger.error("CTRL - C pressed exiting the program")
            raise sys.exit("CTRL- C PRESSED!")

    @bulk_insert.consuming_data
    def get_requets_bulk_data(self):
        try:
            while True:
                # initial variables for dates and coins
                info_dates = []
                dates = []
                json_payload = []
                coins = ['ethereum', 'bitcoin', 'cardano']
                # input date date for bulk insert
                initial = input("please enter a date in format yyyy-mm-dd:")
                print("------downloading data...please wait a moment------")
                # pattern validation
                if re.match(self.pattern_d, initial):
                    # applying string to date conversions
                    initial = dt.strptime(initial, "%Y-%m-%d")
                    current = str(datetime.date.today())
                    current = dt.strptime(current, "%Y-%m-%d")
                    initial = initial - datetime.timedelta(1)
                    days = int((current - initial).days)
                    # days segmentation for creating dates
                    for i in range(days):
                        today = initial + datetime.timedelta(i + 1)
                        today = today.strftime("%d-%m-%Y")
                        dates.append(today)
                    # iterating coins and date items in list for requesting data
                    for c in range(len(coins)):
                        for d in range(len(dates)):
                            info_dates.append(coins[c] + "_" + dates[d])
                            df = pd.DataFrame(info_dates)
                            df = df.rename(columns={0: 'date_info'}).reset_index(drop=True)
                            df['date_info'] = df['date_info'].apply(lambda x: self.df_conversion(x))
                            df['month_info'] = df['date_info'].apply(lambda x: dt.strftime(x, "%m"))
                            df = df.reset_index(drop=True, inplace=False)
                            path = os.path.abspath("json_data_bk_" + dates[d] + "_" + coins[c] + "_.json")
                            # requesting bulk data
                            url = f'https://api.coingecko.com/api/v3/coins/{coins[c]}/history?date={dates[d]}'
                            r = requests.get(url=url, headers=self.headers, verify=True, stream=True)
                            body = r.content.decode("utf-8")
                            payload = json.loads(body)
                            json_payload.append(payload)
                            # generating dataframe with all the payload data
                            df2 = pd.DataFrame(json_payload)
                            self.df3 = pd.concat([df, df2], axis=1)
                            # writing json payload in the app folder
                            with open(path, "w") as json_file:
                                json.dump(payload, json_file)
                    return self.df3
                else:
                    logger.error("please digit the correct format!")
        except ValueError:
            logger.error("please write the correct format")
        except (KeyboardInterrupt, EOFError):
            logger.error("CTRL - C pressed exiting the program")
            raise sys.exit("CTRL- C PRESSED!")

    def df_conversion(self, x):
        # applying some conversions
        try:
            if len(x) != 0:
                x = re.sub('[_]', ' ', x)
                x = re.sub('[A-Za-z]', '', x)
                x = x.strip()
                x = dt.strptime(x, "%d-%m-%Y")
            elif re.match(self.pattern_d, x):
                x = dt.strptime(x, "%d-%m-%Y")
            else:
                logger.error("empty data")
            return x
        except Exception:
            logger.error("Some error occurred!")
