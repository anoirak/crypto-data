import mysql.connector
import requests
from datetime import datetime
import classes.database.database_credentials_stocks as c
from classes.database.database import UpdateDB  


class getPancake():
    def __init__(self):
        self.exchange = 'PancakeSwap'
        self.date = datetime.today().strftime('%Y-%m-%d')
        self.currency = '$'
        self.dbObj = UpdateDB(mysql.connector.connect(user=c.d_user, password=c.d_pass,
                                         host=c.d_host,
                                         database=c.d_name), "")


    def convert_price_usd(self , base_address):
        URL = f'https://api.pancakeswap.info/api/v2/tokens/{base_address}'
        res = requests.get(url = URL)
        data = res.json()['data']
        return data['price']
        
            
    def push_to_stock_price_tracker(self , data):
        '''
        Push to stock_price_tracker table  
        '''
        
        sql = "INSERT INTO stock_price_tracker (Exchange, Sticker,Date,Price,Total_Volume,Currency) VALUES (%s, %s,%s, %s, %s,%s)"
        val = (self.exchange, data['base_symbol'], self.date, data['price'], data['base_volume'],self.currency)
        try:
            self.dbObj.update(sql, val)
        except Exception as err:
            print('Could not push into stock_price_tracker',err)
            pass
        

    def push_to_stickers(self,data):
        '''
        Push to stickers table
        '''
        sql = "INSERT INTO strickers (Exchange, Name,Sticker) VALUES (%s, %s,%s)"
        val = (self.exchange, data['base_name'], data['base_symbol'])
        try:
            self.dbObj.update(sql, val)
        except Exception as err: 
            print('Could not push into stickers', err)
            pass

    def extract_data(self , data):
        for token in data:
            stickers = {
            'base_name' : data[token]['base_name'], # token0 name
            'base_address' : data[token]['base_address'], # token0 address, 
            'base_symbol': data[token]['base_symbol'],           # token0 symbol
            }

            self.push_to_stickers(stickers)
            stock_price_tracker = {
                'base_symbol': data[token]['base_symbol'],           # token0 symbol
                'price': self.convert_price_usd(data[token]['base_address'])  ,  #price denominated in USD
                'base_volume': data[token]['base_volume'],           # volume denominated in token0
                'liquidity': data[token]['liquidity'],             # liquidity denominated in USD
                'liquidity_BNB': data[token]['liquidity_BNB']          # liquidity denominated in BNB
            }

            self.push_to_stock_price_tracker(stock_price_tracker)

    def main(self):
        # api-endpoint
        URL = "https://api.pancakeswap.info/api/v2/pairs"
            
        # sending get request and saving the response as response object
        response = requests.get(url = URL)
    
        # extracting data in json format
        if response.status_code == 200:
            data = response.json()['data']
            print(len(data))
            self.extract_data(data)
        else : 
            print('Error with Pancake API')


if __name__== "__main__":
    stocks = getPancake()
    try:
        stocks.main()
    except Exception as e:
        print(e)
    
