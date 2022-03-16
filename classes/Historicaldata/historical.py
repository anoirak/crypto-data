import logging
from bsedata.bse import BSE
import mysql.connector
from cryptocmd import CmcScraper
import datetime
import dateparser
import yfinance as yf
from classes.database.database import UpdateDB
import numpy as np
import requests
import time
import mysql.connector
import classes.database.database_credentials_stocks as c
from dateutil.parser import parse

result = []
dbObj = UpdateDB(mysql.connector.connect(user=c.d_user, password=c.d_pass,
                                         host=c.d_host,
                                         database=c.d_name), "")

def calculate_history_delay():
    dict = {
    1 : [i for i in range(7,31)],
    2 : [i for i in range(4*3600,6*3600)]
    }
    # 90% probablity to choose a delay between 7 and 30 seconds
    choosen_index = np.random.choice(a=[1,2] , p = [0.9,0.1])
    return np.random.choice(a=dict[choosen_index])


def getBSE(sticker):
    b = BSE(update_codes=True)
    his = b.getPeriodTrend(sticker, '12M')
    for i in his:
        data = {}
        # dtimeformat=dateparser.parse(i['date'])
        data['Exchange'] = 'BSE'
        data['Sticker'] = sticker
        data['Date'] = i['date']
        data['Price'] = i['value']
        data['Price_High'] = ''
        data['Price_low'] = ''
        data['Total_Volume'] = i['vol']
        data['Total_transaction'] = ''
        data['Currency'] = 'Rupee'
        data['exch_sticker'] = data['Exchange']+'-'+data['Sticker']
        sql = "INSERT INTO stock_price_tracker (Exchange, Sticker,Date,Price,Price_High,Price_low,Total_Volume,Total_transaction,Currency,exch_sticker) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s,%s)"
        val = (data['Exchange'], data['Sticker'], data['Date'], data['Price'], data['Price_High'], data['Price_low'],
               data['Total_Volume'], data['Total_transaction'], data['Currency'], data['exch_sticker'])
        dbObj.update(sql, val)
        # result.append(data)


def getNASDAQ(sticker):
    try:
        msft = yf.Ticker(str(sticker))
        df = msft.history(period='5y')
        dates = df.index.values
        vals = df.values.tolist()

        for date, val in zip(dates, vals):
            # dtimeformat=dateparser.parse(date)
            data = {}
            data['Exchange'] = 'NASDAQ'
            data['Sticker'] = sticker
            data['Date'] = str(date)
            data['Price'] = val[3]
            data['Price_High'] = val[1]
            data['Price_low'] = val[2]
            data['Total_Volume'] = val[4]
            data['Total_transaction'] = ''
            data['Currency'] = '$'
            data['exch_sticker'] = data['Exchange']+'-'+data['Sticker']
            sql = "INSERT INTO stock_price_tracker (Exchange, Sticker,Date,Price,Price_High,Price_low,Total_Volume,Total_transaction,Currency,exch_sticker) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s,%s)"
            val = (data['Exchange'], data['Sticker'], data['Date'], data['Price'], data['Price_High'], data['Price_low'],
                   data['Total_Volume'], data['Total_transaction'], data['Currency'], data['exch_sticker'])
            dbObj.update(sql, val)
            # result.append(data)
    except Exception as e:
        logging.Logger.error(
            "An error has occurred in getting NASDAQ Historical data.")
        logging.Logger.error(f"Error: {e}")
        return


def get_nasdaq_history(sticker):
    delay = calculate_history_delay()
    time.sleep(delay)
    five_years_back_date = (datetime.datetime.now() +
                            datetime.timedelta(-1826)).strftime('%s')
    date_today = (datetime.datetime.now()).strftime('%s')

    url = "https://query1.finance.yahoo.com/v8/finance/chart/{}?interval=1d&period1={}&period2={}&corsDomain=finance.yahoo.com".format(
        sticker, five_years_back_date, date_today)
    h = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36"}
    response = requests.get(url, headers=h)
    if response.status_code == 200:
        data = response.json()
        _history_parse(data, sticker, 'NASDAQ')
    else:
        print("Issue with yahoo API")


def get_nse_history(sticker):
    delay = calculate_history_delay()
    time.sleep(delay)
    five_years_back_date = (datetime.datetime.now() +
                            datetime.timedelta(-1826)).strftime('%s')
    date_today = (datetime.datetime.now()).strftime('%s')

    url = "https://query1.finance.yahoo.com/v8/finance/chart/{}?interval=1d&period1={}&period2={}&corsDomain=finance.yahoo.com".format(
        sticker+".NS", five_years_back_date, date_today)
    h = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36"}
    response = requests.get(url, headers=h)
    if response.status_code == 200:
        data = response.json()
        _history_parse(data, sticker, "NSE")
    else:
        print("Issue with yahoo API")


def get_lse_history(sticker):
    delay = calculate_history_delay()
    time.sleep(delay)
    five_years_back_date = (datetime.datetime.now() +
                            datetime.timedelta(-1826)).strftime('%s')
    date_today = (datetime.datetime.now()).strftime('%s')
    url = "https://query1.finance.yahoo.com/v8/finance/chart/{}?interval=1d&period1={}&period2={}&corsDomain=finance.yahoo.com".format(
        sticker, five_years_back_date, date_today)
    if sticker.endswith("l"):
        sticker1 = sticker[:-1]+".l"
        url = "https://query1.finance.yahoo.com/v8/finance/chart/{}?interval=1d&period1={}&period2={}&corsDomain=finance.yahoo.com".format(
            sticker1, five_years_back_date, date_today)
    h = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36"}
    response = requests.get(url, headers=h)
    if response.status_code == 200:
        data = response.json()
        _history_parse(data, sticker, "LSE")
    else:
        print("Issue with yahoo API")


def _history_parse(data, sticker, Exchange):
    his_ins_data = []
    if 'chart' in data and 'result' in data['chart'] and 'indicators' in data['chart']['result'][0]:
        nas_json = data['chart']['result'][0]
        indicators = nas_json['indicators']['quote'][0]
        currency = data['chart']['result'][0]['meta']['currency']
        exch_sticker = Exchange+'-'+sticker
        open = indicators['open']
        high = indicators['high']
        volume = indicators['volume']
        low = indicators['low']
        close = indicators['close']
        date = nas_json['timestamp'] if 'timestamp' in nas_json else None
        sql = "INSERT INTO stock_price_tracker (Exchange, Sticker,Date,Price,Price_High,Price_low,Total_Volume,Currency,exch_sticker) VALUES (%s,%s, %s,%s, %s,%s, %s,%s,%s)"
        for dt, op, cl, ph, pl, vol in zip(date, open, close, high, low, volume):
            date = time.strftime('%Y-%m-%d', time.localtime(dt))
            his_ins_data.append(
                (Exchange, sticker, date, cl, ph, pl, vol, currency, exch_sticker))
        dbObj.executemany(sql, his_ins_data)


def getBinance(sticker):
    try:
        scraper = CmcScraper(sticker, "15-10-2017", "04-02-2022")
        headers, data = scraper.get_data()
        df = scraper.get_dataframe()
        bin_ins_data = []
        sql = "INSERT INTO stock_price_tracker (Exchange, Sticker,Date,Price,Price_High,Price_low,Total_Volume,Currency,exch_sticker) VALUES (%s,%s, %s,%s, %s,%s, %s,%s,%s)"
        for date, price, high, low, vol in zip(df['Date'].values, df['Close'].values, df['High'].values, df['Low'].values, df['Volume'].values):
            # dtimeformat=dateparser.parse(date)
            Exchange = 'Binance'
            Currency = '$'
            exch_sticker = Exchange+'-'+sticker
            date_val = parse(str(date)).strftime('%Y-%m-%d')
            bin_ins_data.append((Exchange, sticker, date_val, int(price), int(high),
                                 int(low), int(vol), Currency, exch_sticker))
            dbObj.executemany(sql, bin_ins_data)
    except Exception as e:
        print(e)
        # logging.Logger.error(
        #     "An error has occurred in getting Binance Historical data.")
        # logging.Logger.error(f"Error: {e}")
        return


def getData():
    global result
    query = "SELECT Exchange,Sticker FROM strickers"

    myresult = dbObj.get_data(query)
    exchanges = []
    stickers = []

    for x in myresult:
        exchanges.append(x['Exchange'])
        stickers.append(x['Sticker'])

    for sticker, exchange in zip(stickers, exchanges):
        # if exchange=='BSE':
        #  getBSE(sticker)
        try:
            print("collecting data for: {}".format(sticker))
            if exchange == 'NASDAQ':
                get_nasdaq_history(sticker)
            if exchange == 'Binance':
                getBinance(sticker)
            if exchange == "NSE":
                get_nse_history(sticker)
            if exchange == "LSE":
                get_lse_history(sticker)
        except Exception as e:
            print(e)
