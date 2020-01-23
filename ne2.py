import pandas as pd
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import os
import json
import time
import logging
from bs4 import BeautifulSoup
import requests
import sys
import sqlalchemy
from sqlalchemy import select,text,Table,MetaData,Column,Integer,String,Date,VARCHAR,NVARCHAR,Float
from CER import cer_connection
logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': True
})
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
#%%

def ConfigureLog(file_name):
    
    #stdout_handler
    #stdout_handler = logging.StreamHandler(sys.stdout)
    #stdout_handler.setLevel(logging.DEBUG)
    #file_handler
    file_handler = logging.FileHandler(filename=file_name+'.log')
    file_handler.setLevel(logging.DEBUG)
    
    #handlers = [file_handler,stdout_handler]
    handlers = file_handler
    
    logging.basicConfig(
            filename = file_name+'.log',
            level=logging.DEBUG, 
            format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
            handlers=handlers
    )
    
    log = logging.getLogger(file_name)
    
    return(log)

#global logger here

log = ConfigureLog(file_name='settlements')
    

def config_file(login_file = 'login.json'):
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname('__file__')))
        
    try:
        with open(os.path.join(__location__,login_file)) as f:
            config = json.load(f)
            user_name,password = config[0]['user_name'],config[0]['password']
            return user_name,password
    except:
        raise
    
        
def cast_types_settlement(df):
    df['SettlementDate'] = pd.to_datetime(df['SettlementDate'], errors='coerce')
    df['InstrumentStartDate'] = pd.to_datetime(df['InstrumentStartDate'], errors='coerce')
    df['InstrumentEndDate'] = pd.to_datetime(df['InstrumentEndDate'], errors='coerce')
    df['SettlementValue'] = pd.to_numeric(df['SettlementValue'], errors = 'coerce')
    df['Market'] = df['Market'].astype('object')
    df['Instrument'] = df['Instrument'].astype('object')
    return df
    
def cast_types_index(df):
    df['Market'] = df['Market'].astype('object')
    df['Trades'] = pd.to_numeric(df['Trades'], errors = 'coerce')
    df['BBL/Day'] = pd.to_numeric(df['BBL/Day'], errors = 'coerce')
    df['M3'] = pd.to_numeric(df['M3'], errors = 'coerce')
    df['Avg Price'] = pd.to_numeric(df['Avg Price'], errors = 'coerce')
    df['bid window'] = df['bid window'].astype('object')
    df['window start'] = pd.to_datetime(df['window start'], errors='coerce')
    df['window end'] = pd.to_datetime(df['window end'], errors='coerce')
    df['current date'] = pd.to_datetime(df['current date'], errors='coerce')
    return df 
    
def cast_types_trade(df):
    df['Start Date'] = pd.to_datetime(df['Start Date'], errors='coerce')
    df['End Date'] = pd.to_datetime(df['End Date'], errors='coerce') 
    df['Date/Time'] = pd.to_datetime(df['Date/Time'], errors='coerce') 
    df['Instrument'] = df['Instrument'].astype('object')
    return df

def CreateTable(name):
    
    conn,engine = cer_connection()
    meta = MetaData()
    
    if name == 'Net_Energy_Settlements':
        
        t = Table(
                name,meta,
                Column('SettlementDate',Date,primary_key=True),
                Column('Market',VARCHAR(20),primary_key=True),
                Column('Instrument',VARCHAR(50),primary_key=True),
                Column('SettlementValue',Float(10)),
                Column('InstrumentStartDate',Date),
                Column('InstrumentEndDate',Date)
                )
    else:
        log.debug('invalid table name for net energy')
                
    
    meta.create_all(engine)
    conn.close()

def cersei(df,sql_table,conn,new):
        
    row_count = 0
    pk_error = 0
    
    if new:
        df.to_sql(sql_table,con=conn,if_exists='append',index=False,chunksize=1000)
        log.debug('Added: '+str(len(df))+' to new table: '+sql_table)
    else:
    
        for index,row in df.iterrows():
            
            try:
                t = df.iloc[[index]].copy()
                t.to_sql(sql_table,con=conn,if_exists='append',index=False)
                row_count = row_count + 1
            except:
                pk_error = pk_error+1
    
        log.debug('Added: '+str(row_count)+' to: '+sql_table+' '+str(pk_error)+' already in table')
    

def existing_database_dates(conn,table,col):
    
    existing_dates = list(set(list(pd.read_sql('select ('+col+') from '+table,con=conn)[col])))
    existing_dates = [pd.to_datetime(x).date() for x in existing_dates]
    
    if len(existing_dates) > 0:
        new = False
        return existing_dates,new
    else:
        new = True
        return [],new
    
    
def dates(start=None,end=None):
        #TODO: take out the first and last date. The last date is out of the 3 month range.
    date_list = []
        
    if not start or not end:
        end = (datetime.now().date())-relativedelta(days=1)
        start = end - relativedelta(months=3)
        exclude_list = [5,6]
    else:
        exclude_list = []
        
    delta = end - start
        
    for i in range(delta.days +1):
        next_day = start+timedelta(i)
        if next_day.weekday() not in exclude_list:
            date_list.append(start+timedelta(i))
        
    return date_list
    
    
def links(conn,base_link):
        
    #add username and password
    user_name,password = config_file()
    base_link = base_link.replace('USERNAME',user_name)
    base_link = base_link.replace('PASSWORD',password)
    
    
    if 'settlement' in base_link:
        table = 'Net_Energy_Settlements'
        date_list = dates()
        existing_dates,existing = existing_database_dates(conn,table,col='SettlementDate')
        date_list = [x for x in date_list if x not in existing_dates] #only gaher data for settlement dates not in the saved csv!
           
    link_list = []
    replace_list = ['YYYY','MM','DD']
        
    for d in date_list:
        new_list = [str(d.year),str(d.month),str(d.day)]
        link = base_link
            
        for old, new in zip(replace_list,new_list):
            if len(new) == 1:
                new = str(0)+new
            link = link.replace(old,new)
        
        link_list.append(link)
            
    return link_list,existing,table
    

#def instrument_type(df):
#        
#    #TODO: add options for dual half year and dual quarter
#    types = {'Jan':'Monthly',
#             'Feb':'Monthly',
#             'Mar':'Monthly',
#             'Apr':'Monthly',
#             'May':'Monthly',
#             'Jun':'Monthly',
#             'Jul':'Monthly',
#             'Aug':'Monthly',
#             'Sep':'Monthly',
#             'Oct':'Monthly',
#             'Nov':'Monthly',
#             'Dec':'Monthly',
#             'H':'Half year',
#             'Cal':'Calendar year',
#             'Q':'Quarter'}
#        
#    c = []
#    for i in df['Instrument']:
#        for m in types:
#            if i.find(m)!=-1:
#                if i.find('/')!=-1:
#                    c.append(types[m]+'_dual')
#                    break
#                        
#                else:
#                    c.append(types[m])
#                    break
#                    
#    df['InstrumentType'] = c
#    return df
    
    
def scrape(conn,link):
        
    link_list,existing,table = links(conn,link)
    
    if len(link_list)!=0:
        
        if 'settlement' in link:
            
            settlement_list = []
                  
            for link in link_list:
                    
                try:
                    df = pd.read_html(link)[0]
                    #df = cast_types_settlement(df) #dont need to cast types when doing database insert
                    settlement_list.append(df)
                    time.sleep(2) #wait to make sure net energy is not overloaded
                except:
                    None
                    #logger.info('cant get settlement data for '+str(link))
            
            #save contains all the new data
            if len(settlement_list) != 0:
                new_data = pd.concat(settlement_list,axis=0,ignore_index=True)
                cersei(new_data,table,conn,existing)
            else:
                log.debug('Cant insert settlements for: '+'_'.join(link_list))
    else:
        log.debug('No new settlement links')

                   
def main():
        
    ne2 = ['http://ne2.ca/nedd/exp/settlement-export?username=USERNAME&password=PASSWORD&format=HTML&exchange=no&date=YYYYMMDD']
    conn,engine = cer_connection()
    log.debug('Opened CERSEI connection')
    
    for link in ne2:
        if 'settlement' in link:
            scrape(conn,link)
        else:
            #TODO: add in trade volumes
            None
    
    conn.close()
    log.debug('Closed CERSEI connection')
    logging.shutdown()
   
#%%       
if __name__ == "__main__":
   main()
    
            
#%%
    
    



