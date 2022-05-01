
import sqlite3
from time import sleep
import sqlalchemy
import pandas as pd
from binance.client import Client


client = Client()


# Creating enigne object to connect with the database
engine = sqlalchemy.create_engine("sqlite:///test.db")

# df = pd.DataFrame(client.get_historical_klines('BTCUSDT', '1m', '30min ago UTC'))
# print(df)

def getsymbol(symbol, interval, lookback):
    df = pd.DataFrame(client.get_historical_klines(
        symbol, interval, lookback+'min ago UTC'))
    df = df.iloc[:, :6]
    df.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    df = df.set_index('Time')
    df.index = pd.to_datetime(df.index, unit="ms")
    df = df.astype(float)
    return df
    
df = getsymbol('BTCUSDT', '1m', '30')

#Updating data in the database
df.to_sql("PRICES", engine,  if_exists="append", index=False)

# Displaying records in the database
# df1 = pd.read_sql('PRICES', engine)


print("\n\n")
print("************************************************************************************************************")


# while True:
    # sleep(60)
    # df1 = getsymbol('BTCUSDT', '1m', '1')
    # df1.to_sql("PRICES", engine,  if_exists="append", index=False)
    # print(df1)
    # print("\n_________________________________________________________________________________________________")


# Trend = if the crypto was rising by some x% --> buy
# exit when  profit is above 0.15% (genereal tax of binance) or loss is crossing -0.15%

#Trading Strategy
def strategy(symbol,qty, entried=False):
    df = getsymbol('BTCUSDT', '1m', '30')
    cumret = (df.Open.pct_change()+1).cumprod() -1
    if not entried:
        if cumret[-1] < 0.002:
            tstamp = client.get_server_time
            print("ORDER HAS BEEN BOUGHT")
        else:
            print("No trade has been executed")
    if entried:
        while True:
            df = getsymbol(symbol, '1m', '30')
            sincebuy = df.loc[df.index > pd.to_datetime(tstamp, unit = 'ms')]
            if len(sincebuy) > 0:
                sincebuyret = (sincebuy.Open.pct_change() +1).cumprod()
                if sincebuyret[-1] > 0.0015 or sincebuyret[-1] < -0.0015:
                    print("ORDER HAS BEEN SOLD")
                    break


strategy('BTCUSDT', 0.001)
# Dropping PRICES table in the end
mycon = sqlite3.connect("test.db")
mycur = mycon.cursor()
mycur.execute('DROP TABLE PRICES')
