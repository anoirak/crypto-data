from bsedata.bse import BSE
from Historic_Crypto import Cryptocurrencies
from yahoo_fin import stock_info as si
from classes.database.database import UpdateDB

import mysql.connector
import classes.database.database_credentials_stocks as c
result = []


def update_bse():
    global result
    b = BSE(update_codes=True)
    codes = b.getScripCodes()
    for i in codes:
        data = {}
        data['Exchange'] = 'BSE'
        data['Name'] = codes[i]
        data['Sticker'] = i
        data['Country'] = 'India'
        data['Website'] = ''
        data['Email'] = ''
        data['exch_sticker'] = data['Exchange']+'-'+data['Sticker']
        result.append(data)


def update_nasdaq():
    codes = si.tickers_nasdaq()
    for i in codes:
        data = {}
        data['Exchange'] = 'NASDAQ'
        data['Name'] = ''
        data['Sticker'] = i
        data['Country'] = 'USA'
        data['Website'] = ''
        data['Email'] = ''
        data['exch_sticker'] = data['Exchange']+'-'+data['Sticker']

        result.append(data)


def get_crypto_coin():
    x = []
    a = Cryptocurrencies().find_crypto_pairs()
    status = a['status']
    coins = a['id']
    for id, s in zip(coins, status):
        if 'USD' in id and s == 'online':
            x.append(id.split('-')[0])
    for i in x:
        data = {}
        data['Exchange'] = 'Binance'
        data['Name'] = ''
        data['Sticker'] = i
        data['Country'] = ''
        data['Website'] = ''
        data['Email'] = ''
        data['exch_sticker'] = data['Exchange']+'-'+data['Sticker']

        result.append(data)


def create_sql(df):
    dbObj = UpdateDB(mysql.connector.connect(user=c.d_user, password=c.d_pass,
                                             host=c.d_host,
                                             database=c.d_name), "")

    #mycursor = mydb.cursor()
    for i in df:
        sql = "INSERT INTO strickers (Exchange, Name,Sticker,Country,Website,Email,exch_sticker) VALUES (%s, %s,%s,%s,%s,%s,%s)"
        val = (i['Exchange'], i['Name'], i['Sticker'], i['Country'],
               i['Website'], i['Email'], i['exch_sticker'])
        try:
            dbObj.update(sql, val)
        except:
            continue
        #mycursor.execute(sql, val)

    # mydb.commit()


def getStickers():
    # update_bse()
    update_nasdaq()
    get_crypto_coin()
    create_sql(result)
