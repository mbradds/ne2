import pandas as pd
import numpy as np
from datetime import date,timedelta,datetime
import time
import os
from os import path
import CER_CONN as CER
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
    #gets the current and post 2018 enbridge dates. These come from net energy and should be accurate
    df = pd.read_excel('net_energy_dates.xlsx',sheet_name='nos')
    for col in df.columns:
        if col != 'Year':
            df[col] = pd.to_datetime(df[col],errors='raise')
            
    return df

def holidays():
    df = pd.read_excel('net_energy_dates.xlsx',sheet_name='holidays')
    df['Observed Holiday or Early Close'] = [' '.join(x.split(' ')[1:]) for x in df['Observed Holiday or Early Close']]
    df['Observed Holiday or Early Close'] = pd.to_datetime(df['Observed Holiday or Early Close'],errors='raise')
    df['Holiday Type'] = ['Holiday' if 'early close' not in x.lower() else 'Early Close' for x in df['Canadian Holidays']]
    
    CreateTable('Canadian_Trade_Holidays',conn)
    df.to_sql('Canadian_Trade_Holidays',index=False,if_exists='replace',con=conn)
    
    return df

def bid_weeks():
    #gets the historical (pre 2018) enbridge dates. Some of these dates may be estimates
    def nos_day(date):
        valid = False
        date = date + timedelta(days=1)
        while not valid:
            if date.weekday() not in (5,6):
                valid = True
            else:
                date = date + timedelta(days=1)
                
        return date
    
    
    df = pd.read_csv('Bid Weeks.csv')
    for date_col in ['Start Date','End Date']:
        df[date_col] = pd.to_datetime(df[date_col])
    
    df['Bid Week'] = [x.replace('Delivery','01').strip() for x in df['Bid Week']]
    df['Bid Week'] = [datetime.strptime(x,'%B %Y %d') for x in df['Bid Week']]
    df['Enbridge Notice of Shipments (NOS)'] = [nos_day(x) for x in df['End Date']]
    df['Year'] = [x.year for x in df['Start Date']]
    
    df = df.rename(columns={'Bid Week':'Delivery Month',
                            'Start Date':'First Trade',
                            'End Date':'Last Trade'})
    
    return df


def all_dates(sql=False):
    current = nos()
    hist = bid_weeks()
    hist = hist[hist['Delivery Month']<min(current['Delivery Month'])]
    df = pd.concat([current,hist],ignore_index=True)
    if sql:
        CreateTable('Enbridge_NOS',conn)
        df.to_sql('Enbridge_NOS',index=False,if_exists='replace',con=conn)
    
    return None


if __name__ == "__main__":
    
    #current = nos()
    #dates = holidays()
    #historical = bid_weeks(sql=False)
    all_dates(sql=True)
    conn.close()
    
    
    
    