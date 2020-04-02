import pandas as pd
import numpy as np
import json
from datetime import date,timedelta
import time
import os
from os import path
import ne2volume
from Documents.ne2.Data_Scraping import CER
import sqlalchemy
from sqlalchemy import select,text,Table,MetaData,Column,Integer,String,Date,VARCHAR,NVARCHAR,Float,DateTime
#%%
conn,engine = CER.cer_connection()

def CreateTable(name,conn):
    
    meta = MetaData()
    
    if name == 'Enbridge_NOS':
        
        t = Table(
                name,meta,
                Column('Year',Integer),
                Column('Delivery Month',Date),
                Column('First Trade',Date),
                Column('Last Trade',Date),
                Column('Enbridge Notice of Shipments (NOS)',Date)
                )
    elif name == 'Canadian_Trade_Holidays':
        
        t = Table(
                name,meta,
                Column('Year',Integer),
                Column('Canadian Holidays',VARCHAR(150)),
                Column('Observed Holiday or Early Close',Date),
                Column('Holiday Type',VARCHAR(50))
                )
    
    else:
        print('invalid table name for net energy')
                
    meta.create_all(engine)
    return conn


def nos():
    
    df = pd.read_excel('net_energy_dates.xlsx',sheet_name='nos')
    for col in df.columns:
        if col != 'Year':
            df[col] = pd.to_datetime(df[col],errors='raise')
    
    CreateTable('Enbridge_NOS',conn)
    df.to_sql('Enbridge_NOS',index=False,if_exists='replace',con=conn)
    return df

def holidays():
    
    df = pd.read_excel('net_energy_dates.xlsx',sheet_name='holidays')
    df['Observed Holiday or Early Close'] = [' '.join(x.split(' ')[1:]) for x in df['Observed Holiday or Early Close']]
    df['Observed Holiday or Early Close'] = pd.to_datetime(df['Observed Holiday or Early Close'],errors='raise')
    df['Holiday Type'] = ['Holiday' if 'early close' not in x.lower() else 'Early Close' for x in df['Canadian Holidays']]
    
    CreateTable('Canadian_Trade_Holidays',conn)
    df.to_sql('Canadian_Trade_Holidays',index=False,if_exists='replace',con=conn)
    
    return df

if __name__ == "__main__":
    
    
    notice = nos()
    dates = holidays()
    conn.close()