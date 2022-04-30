import sqlite3
import sqlalchemy
import pandas as pd

# Creating enigne object to connect with the database
engine = sqlalchemy.create_engine("sqlite:///test.db")


#Connecting Google Sheet
#Sheet URL = https://docs.google.com/spreadsheets/d/1Q9gnypX9DyXj45IuF9Fw4eCk0deh7ZCp1q-xDn1MTP0/edit#gid=1704940924
sheet_id = '1Q9gnypX9DyXj45IuF9Fw4eCk0deh7ZCp1q-xDn1MTP0'
sheet_name = 'Candlestickwithheaders'
url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'


#Reading from the Sheet
df = pd.read_csv(url)
df = df.iloc[:,:6]
df.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
df = df.set_index('Time')
df.index = pd.to_datetime(df.index, unit = "ms")






#Updating data in the database
df.to_sql("PRICES", engine,  if_exists="append", index = False)

#Displaying records in the database
df1 = pd.read_sql('PRICES', engine)
print(df1)




mycon = sqlite3.connect("test.db")
mycur = mycon.cursor()
mycur.execute('DROP TABLE PRICES')
