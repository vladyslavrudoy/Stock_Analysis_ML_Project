import mysql.connector
import datetime
import time
import pytz
import pandas as pd
import os

from pytz import timezone
from datetime import datetime
from lxml import html
import requests
from bs4 import BeautifulSoup
from configparser import SafeConfigParser

config = SafeConfigParser()
config.read(os.path.abspath('../Configuration/config.ini'))

fmt = "%Y-%m-%d %H:%M:%S"
now= datetime.now(timezone('UTC')).astimezone(timezone('US/Eastern')).strftime(fmt)

class Ticker:

  def __init__(self):
    self.name = ''
    self.date_of_trade = ''
    self.openp = 0
    self.highp = 0
    self.lowp = 0
    self.closep = 0
    self.volume = 0
    self.datetime = 0

  def __repr__(self):
    return 'Name: %s, Date: %s, Opening Price: %s, High Price: %s, Low Price: %s, Close Price: %s, Volume: %s, Date Uploaded: %s' % \
    (self.name, self.date_of_trade, self.openp, self.highp, self.lowp, self.closep, self.volume, self.datetime)


def ticker_setup(symbol):
    
    new_ticker = Ticker()
    new_ticker.name = symbol
    new_ticker.datetime = now
    
    return new_ticker


def database_connect(db_name):     
  

  try:
        con = mysql.connector.connect(host=config.get('%s' % db_name, 'host'),
              user=config.get('%s' % db_name, 'user'),
              passwd=config.get('%s' % db_name, 'passwd'),
              db=config.get('%s' % db_name, 'db'))
        print('Database \'%s\' initialized!' % config.get('%s' % db_name, 'db'))
     
  except ValueError:
  	print('Error connecting to %s.' % (db_name))

  return con 


def get_alphavantage_data(symbol, db_name):  
    stock =  ticker_setup(symbol)
   
    time.sleep(15)
    final_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={}&interval=30min&outputsize=full&apikey={}'.format(stock.name,config.get('%s' % db_name, 'av_api'))
    r = requests.get(final_url)
    if (r.status_code == 200):
        print("Request has been successful!")
        
    con = database_connect(db_name)
    #cur = con.cursor()    
    result = r.json()
    dataForAllDays = pd.DataFrame(data=result['Time Series (30min)']).transpose().head(13)[::-1]
    for index, row in dataForAllDays.iterrows():
        cur = con.cursor()
        stock.date_of_trade = index
        stock.openp = row['1. open']
        stock.highp = row['2. high']
        stock.lowp = row['3. low']
        stock.closep = row['4. close']
        stock.volume = row['5. volume']
        
        try:
            cur.execute(
                """INSERT INTO {} (ticker, date_of_trade, open_price, 
                high_price, low_price, close_price, volume, date_uploaded) 
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s); """.format(symbol),
                       (stock.name, stock.date_of_trade, stock.openp, 
                        stock.highp, stock.lowp, stock.closep,stock.volume, stock.datetime))
            con.commit()
            print('Commiting changes to database!')
            
            #perhaps use ORM in the future
        except:
            con.rollback()
            
        finally:
            if (con.is_connected()):
                cur.close()
                print("Closing cursor")
        
        print('Data load completed!')
            
    #cur.close()
    con.close()
    print('Connection closed! Database load completed!')




if __name__ == "__main__":
    ticker_names = ['AAPL', 'CSCO', 'CVX', 'DWDP', 'GS', 'NKE', 'PFE', 'TRV', 'UNH', 'V' ,'VZ' ,'WBA']
    for i in ticker_names:
        get_alphavantage_data(i, 'securities_master')
        print('Loading {}'.format(i))



