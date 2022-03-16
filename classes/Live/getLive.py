import logging
from tkinter import N
from bsedata.bse import BSE
import mysql.connector
from cryptocmd import CmcScraper
import datetime
import dateparser
import yfinance as yf
from datetime import datetime, timezone
from classes.database.database import UpdateDB
import numpy as np 
from dateutil.parser import parse
import requests
import time

import mysql.connector
import classes.database.database_credentials_stocks as c
# from classes.log import logger
result = []
dbObj = UpdateDB(mysql.connector.connect(user=c.d_user, password=c.d_pass,
                                         host=c.d_host,
                                         database=c.d_name), "")



def calculate_delay_live():
    random_array = [i for i in range(7,31)]
    return np.random.choice(random_array)

def getBSE(sticker):

    b = BSE(update_codes=True)
    his = b.getPeriodTrend(sticker, '1M')
    s = [his]
    for i in s:
        data = {}
        # dtimeformat=dateparser.parse(i['date'])
        data['Exchange'] = 'BSE'
        data['Sticker'] = sticker
        data['Date'] = i['date']
        data['Price'] = i['value']
        data['Price-High'] = None
        data['Price-low'] = None
        data['Total-Volume'] = i['vol']
        data['Total-transaction'] = None
        data['Currency'] = 'Rupee'
        data['exch_sticker'] = data['Exchange']+'-'+data['Sticker']
        # result.append(data)
        sql = "INSERT INTO stock_price_tracker (Exchange, Sticker,Date,Price,Price_High,Price_low,Total_Volume,Total_transaction,Currency,exch_sticker) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s,%s)"
        val = (data['Exchange'], data['Sticker'], data['Date'], data['Price'], data['Price-High'], data['Price-low'],
               data['Total-Volume'], data['Total-transaction'], data['Currency'], data['exch_sticker'])
        dbObj.update(sql, val)


def getNASDAQ(sticker):
    try:
        print(sticker)
        msft = yf.Ticker(str(sticker))
        df = msft.history(period='1d')
        dates = df.index.values
        vals = df.values.tolist()
        for date, val in zip(dates, vals):
            # dtimeformat=dateparser.parse(date)
            data = {}
            data['Exchange'] = 'NASDAQ'
            data['Sticker'] = sticker
            data['Date'] = parse(str(date)).strftime('%Y-%m-%d')
            data['Price'] = val[3]
            data['Price-High'] = val[1]
            data['Price-low'] = val[2]
            data['Total-Volume'] = val[4]
            # data['Total-transaction'] = ""
            data['Currency'] = '$'
            data['exch_sticker'] = data['Exchange']+'-'+data['Sticker']
            # result.append(data)
            # print((data['Exchange'],data['Sticker'],data['Date'],data['Price'],data['Price-High'],data['Price-low'],data['Total-Volume'],data['Total-transaction'],data['Currency']))
            # sql = "INSERT INTO stock_price_tracker (Exchange, Sticker,Date,Price,Price_High,Price_low,Total_Volume,Currency,exch_sticker)\
            #    VALUES ('{}', '{}','{}', {},{}, {},{},'{}','{}')".format(data['Exchange'], data['Sticker'], data['Date'], data['Price'], data['Price-High'], data['Price-low'],
            #                                                             data['Total-Volume'], data['Currency'], data['exch_sticker'])
            # # val =

            # dbObj.insert(sql)

            sql = "INSERT INTO stock_price_tracker (Exchange, Sticker,Date,Price,Price_High,Price_low,Total_Volume,Currency,exch_sticker)\
               VALUES (%s, %s,%s, %s,%s, %s,%s,%s,%s)"
            dbObj.update(sql, (data['Exchange'], data['Sticker'], data['Date'], data['Price'], data['Price-High'], data['Price-low'],
                               data['Total-Volume'], data['Currency'], data['exch_sticker'],))
    except Exception as e:
        logging.Logger.error(
            "An error has occurred in getting NASDAQ Live data.")
        logging.Logger.error(f"Error: {e}")
        return


def get_nasdaq_live(sticker):
    delay = calculate_delay_live()
    time.sleep(delay)
    url = "https://query1.finance.yahoo.com/v8/finance/chart/{}?formatted=true&interval=1d&range=1d&useYfid=true&corsDomain=finance.yahoo.com".format(
        sticker)
    h = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36"}
    response = requests.get(url, headers=h)
    if response.status_code == 200:
        data = response.json()
        _live_parse(data, sticker, 'NASDAQ')
    else:
        print("Issue with yahoo API")


def get_lse_live(sticker):
    delay = calculate_delay_live()
    time.sleep(delay)
    url = "https://query1.finance.yahoo.com/v8/finance/chart/{}?formatted=true&interval=1d&range=1d&useYfid=true&corsDomain=finance.yahoo.com".format(
        sticker)
    if sticker.endswith("l"):
        sticker1 = sticker[:-1]+".l"
        url = "https://query1.finance.yahoo.com/v8/finance/chart/{}?formatted=true&interval=1d&range=1d&useYfid=true&corsDomain=finance.yahoo.com".format(
            sticker1)
    h = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36"}
    response = requests.get(url, headers=h)
    if response.status_code == 200:
        data = response.json()
        _live_parse(data, sticker, "LSE")
    else:
        print("Issue with yahoo API")


def get_nse_live(sticker):
    delay=calculate_delay_live()
    time.sleep(delay)
    url = "https://query1.finance.yahoo.com/v8/finance/chart/{}?formatted=true&interval=1d&range=1d&useYfid=true&corsDomain=finance.yahoo.com".format(
        sticker+".NS")
    h = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36"}
    response = requests.get(url, headers=h)
    if response.status_code == 200:
        data = response.json()
        _live_parse(data, sticker, "NSE")
    else:
        print("Issue with yahoo API")


def _live_parse(data, sticker, exchange):
    nas_data = {}
    if 'chart' in data and 'result' in data['chart'] and 'indicators' in data['chart']['result'][0]:
        nas_json = data['chart']['result'][0]
        indicators = nas_json['indicators']['quote'][0]
        date = nas_json['timestamp'][0] if 'timestamp' in nas_json else None
        nas_data['Exchange'] = exchange
        nas_data['Sticker'] = sticker
        nas_data['Date'] = time.strftime(
            '%Y-%m-%d', time.localtime(date)) if date else None
        nas_data['Price'] = indicators['close'][0] if 'close' in indicators else None
        nas_data['Price-High'] = indicators['high'][0] if 'high' in indicators else None
        nas_data['Price-low'] = indicators['low'][0] if 'low' in indicators else None
        nas_data['Total-Volume'] = indicators['volume'][0] if 'volume' in indicators else None
        # data['Total-transaction'] = ""
        nas_data['Currency'] = data['chart']['result'][0]['meta']['currency']
        nas_data['exch_sticker'] = nas_data['Exchange']+'-'+sticker

        sql = "INSERT INTO stock_price_tracker (Exchange, Sticker,Date,Price,Price_High,Price_low,Total_Volume,Currency,exch_sticker)\
               VALUES (%s, %s,%s, %s,%s, %s,%s,%s,%s)"
        dbObj.update(sql, (nas_data['Exchange'], nas_data['Sticker'], nas_data['Date'], nas_data['Price'], nas_data['Price-High'], nas_data['Price-low'],
                           nas_data['Total-Volume'], nas_data['Currency'], nas_data['exch_sticker'],))


def getBinance(sticker):
    try:
        scraper = CmcScraper(sticker)
        headers, data = scraper.get_data()
    except Exception as e:
        # logger.error(f"Error: {e}")
        # logging.Logger.error(
        #     "An error has occurred in getting Binance Live data.")
        # logging.Logger.error(f"Error: {e}")
        print(e)

        return
    df = scraper.get_dataframe()

    for date, price, high, low, vol in zip(df['Date'].values[:1], df['Close'].values[:1], df['High'].values[:1], df['Low'].values[:1], df['Volume'].values[:1]):
        #    dtimeformat=dateparser.parse(date)
        data = {}
        data['Exchange'] = 'Binance'
        data['Sticker'] = sticker
        data['Date'] = parse(str(date)).strftime('%Y-%m-%d')
        data['Price'] = price
        data['Price-High'] = high
        data['Price-low'] = low
        data['Total-Volume'] = vol
        # data['Total-transaction'] = ""
        data['Currency'] = '$'
        data['exch_sticker'] = data['Exchange']+'-'+data['Sticker']
        # sql = "INSERT INTO stock_price_tracker (Exchange, Sticker,Date,Price,Price_High,Price_low,Total_Volume,Currency,exch_sticker) \
        #   VALUES ('{}', '{}','{}', {},{}, {},{},'{}','{}')".format(data['Exchange'], data['Sticker'], data['Date'], data['Price'], data['Price-High'], data['Price-low'],
        #                                                            data['Total-Volume'], data['Currency'], data['exch_sticker'])
        # # val =

        # dbObj.insert(sql)
        sql = "INSERT INTO stock_price_tracker (Exchange, Sticker,Date,Price,Price_High,Price_low,Total_Volume,Currency,exch_sticker)\
               VALUES (%s, %s,%s, %s,%s, %s,%s,%s,%s)"
        dbObj.update(sql, (data['Exchange'], data['Sticker'], data['Date'], data['Price'], data['Price-High'], data['Price-low'],
                           data['Total-Volume'], data['Currency'], data['exch_sticker'],))

        # result.append(data)


def getData():
    global result
    query = "SELECT Exchange,Sticker FROM strickers"

    myresult = dbObj.get_data(query)
    exchanges = []
    stickers = []

    for x in myresult:
        exchanges.append(x['Exchange'])
        stickers.append(x['Sticker'])
    i = 0
    for sticker, exchange in zip(stickers, exchanges):
        # if exchange=='BSE':
        #  getBSE(sticker)
        try:
            print("collecting data for: {}".format(sticker))
            if exchange == 'NASDAQ':
                get_nasdaq_live(sticker)
            if exchange == 'Binance':
                getBinance(sticker)
            if exchange == 'LSE':
                get_lse_live(sticker)
            if exchange == 'NSE':
                get_nse_live(sticker)
        except Exception as e:
            print(e)
