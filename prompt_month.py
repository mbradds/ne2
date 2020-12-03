import pandas as pd
import numpy as np
import json
from datetime import date,timedelta
import time
import os
from os import path
import sqlalchemy
from sqlalchemy import select,text,Table,MetaData,Column,Integer,String,Date,VARCHAR,NVARCHAR,Float,DateTime


def CreateTable(name,conn,engine):
    meta = MetaData()
    
    if name == 'Net_Energy_Spot':
        
        t = Table(
                name,meta,
                Column('SettlementDate',Date),
                Column('Market',VARCHAR(100)),
                Column('Instrument',VARCHAR(50)),
                Column('SettlementValue',Float(5)),
                Column('InstrumentStartDate',Date),
                Column('InstrumentEndDate',Date),
                Column('First Trade Date',Date),
                Column('Last Trade Date',Date),
                Column('Enbridge NOS',Date)
                )
    
    else:
        print('invalid table name for net energy')
                
    meta.create_all(engine)


def ne2(conn):
    df = pd.read_sql_query('select * from Net_Energy_Settlements',con=conn)
    ins = list(set(list(df['Instrument'])))
    rem = ['/','H','Q','Cal']
    prompt_ins = []
    for i in ins:
        found = False
        for r in rem:
            if r in i:
                found = True
        if not found:
            prompt_ins.append(i)
    
    for col in df.columns:
        if 'Date' in col:
            df[col] = pd.to_datetime(df[col])
    
    df = df[df['Instrument'].isin(prompt_ins)]
    #df = df[df['Market']=='C5+ (CFT)']
    return df

def enbridge_nos(conn):
    df = pd.read_sql_query('select * from Enbridge_NOS',con=conn)
    #dates = pd.read_sql_query('select * from Canadian_Trade_Holidays',con=conn)
    for col in df.columns:
        if col != 'Year':
            df[col] = pd.to_datetime(df[col])
    
    return df


def prompt(settle_date,enbridge):
    spot = enbridge.copy()
    spot['Current Settle'] = settle_date
    #spot['Date diff'] = [(last-settle_date).days for last in spot['Last Trade']]
    spot['Date diff'] = [(last-settle_date).days for last in spot['Enbridge Notice of Shipments (NOS)']]
    spot = spot[spot['Date diff']== min([n for n in spot['Date diff'] if n>0])] 
    
    spot = spot.reset_index()
    delivery_month = spot.loc[0,'Delivery Month']
    first_trade = spot.loc[0,'First Trade']
    last_trade = spot.loc[0,'Last Trade']
    nos = spot.loc[0,'Enbridge Notice of Shipments (NOS)']
    return [delivery_month,first_trade,last_trade,nos]

def prompt_test(settle_date,enbridge):
    spot = enbridge.copy()
    spot['Current Settle'] = settle_date
    spot['Date diff'] = [(last-settle_date).days for last in spot['Last Trade']]    
    spot = spot.reset_index()
    return spot


def spot_calculation(conn,engine,log):
    log.warning('pulling nos...')
    enbridge = enbridge_nos(conn)
    log.warning('pulled nos')
    log.warning('pulling net energy...')
    df = ne2(conn)
    try:
        max_date = pd.to_datetime(pd.read_sql_query('select max(SettlementDate) from Net_Energy_Spot',con=conn)[''][0])
    except:
        max_date = None
        
    log.warning(str(max_date))
    log.warning('pulled net energy')
    delivery_month,first_trade,last_trade,nos = [],[],[],[]
    unique_settle = list(set(list(df['SettlementDate'])))
    spots = {}
    
    for settle_date in unique_settle:
        spots[settle_date] = prompt(settle_date,enbridge)

    for settle_date in df['SettlementDate']:
        spot = spots[settle_date]
        delivery_month.append(spot[0])
        first_trade.append(spot[1])
        last_trade.append(spot[2])
        nos.append(spot[3])
        
    df['Delivery Month'] = delivery_month
    df['First Trade Date'] = first_trade
    df['Last Trade Date'] = last_trade
    df['Enbridge NOS'] = nos
    
    df['Spot'] = ['True' if i_s == d_m else 'False' for i_s,d_m in zip(df['InstrumentStartDate'],df['Delivery Month'])]
    df = df[df['Spot']=='True']
    del df['Spot']
    del df['Delivery Month']
    
    CreateTable('Net_Energy_Spot',conn=conn,engine=engine)
    if max_date != None:    
        df = df[df['SettlementDate']>max_date]
        if_exists = 'append'
    else:
        if_exists = 'replace'
        
        
    if len(df) > 0:    
        df.to_sql('Net_Energy_Spot',index=False,if_exists=if_exists,con=conn,chunksize=10000)
        log.warning('Added: '+str(len(df)))
    else:
        log.warning('no new data')


if __name__ == "__main__":
    
    enbridge = enbridge_nos()
    #test = prompt_test(pd.to_datetime('2020-02-14'),enbridge)
    #spot_calculation()
    
#%%






    
    