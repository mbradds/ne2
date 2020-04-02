import pandas as pd
import numpy as np
import json
from datetime import date,timedelta
import time
import os
from os import path
import matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters
from Documents.ne2.Data_Scraping import CER
register_matplotlib_converters()
#%%


conversion = 6.2898


def cersei(table='Net_Energy_Volumes'):
    
    conn,engine = CER.cer_connection()
    
    df = pd.read_sql_table(table,conn)
    
    conn.close()
    
    return df
    

def standardize(market_group=False):
    
    df = cersei()
    df['Start Date'] = pd.to_datetime(df['Start Date'])
    df['End Date'] = pd.to_datetime(df['End Date'])
    df['Date/Time'] = pd.to_datetime(df['Date/Time'])
    
    standard_volume = []
    
    for start,end,volume,uom in zip(df['Start Date'],df['End Date'],df['Volume'],df['Trade UOM']):
        
        delta = end-start
        
        if uom == 'BBLS':
            standard_volume.append(volume/delta.days)
        elif uom == 'M3':
            standard_volume.append((volume*conversion)/delta.days)
        elif uom == 'BBLS_PER_DAY':
            standard_volume.append(volume)
        elif uom == 'CONTRACT':
            standard_volume.append((volume*1000)/delta.days)
        
        else:
            print('Unrecognized UOM: '+str(uom))
        
    df['Volume bbls_per_day'] = standard_volume
    
    
    #TODO: group the data by instrument
    if market_group:
        group_list = ['Instrument','Market','Start Date','End Date']
    else:
        group_list = ['Instrument','Start Date','End Date']
        
    df = df.groupby(group_list).sum()
    df = df.reset_index()
    periods = []
    
    for start,end in zip(df['Start Date'],df['End Date']):
        month_year = []
        current_date = start
        
        while current_date < end:
    
            current_date = current_date + timedelta(days=1)
            month_year.append((current_date.month,current_date.year))
        
        month_year = list(set(month_year))
        periods.append(month_year)
    
    df['Periods'] = periods
    if not market_group:
        df['Market'] = 'All Markets'
    
    #TODO: split apply combine on periods, and then group on periods to get the physical volume of crude for that month across all contract types
    l = []
    
    for start, end, vol, periods,mkt in zip(df['Start Date'],df['End Date'],df['Volume bbls_per_day'],df['Periods'],df['Market']):
        
        p = {}
        
        for month in periods:
            p['Contract Period'] = month
            p['Market'] = mkt
            p['Volume'] = vol
            p['Units'] = 'bbls per day'
        
        l.append(p)
        
    
    df = pd.DataFrame.from_dict(l)
    df = df.groupby(['Contract Period','Units','Market']).sum()
    df = df.reset_index()
    df['Month'],df['Year'] = [x[0] for x in df['Contract Period']],[x[-1] for x in df['Contract Period']]
    df = df[['Contract Period','Month','Year','Market','Units','Volume']]
    
    df.to_csv('trades'+'_mkt_'+str(market_group)+'.csv',index=False)
    
    return df
    
    
if __name__ == "__main__":
    
    df = cersei()
    dfst = standardize(market_group=False)
    dfst = standardize(market_group=True)
    
    
#%%

