from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException  
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
from logging import config
logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': True
})
os.chdir('C:/Users/mossgran/Documents/ne2/Data_Scraping')
import ne2
from CER import cer_connection
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
#%%

log = ne2.ConfigureLog(file_name='volumes.log')

def ScrapeDriver(download_path,headless = False):
        
    try:
        
        chromeOptions = webdriver.ChromeOptions()
        #prefs = {'download.default_directory' : r'C:\Users\mossgrant\Documents\net_energy\download_files'}
        prefs = {'download.default_directory' : download_path}
        chromeOptions.add_argument("--start-maximized")
        chromeOptions.add_experimental_option('prefs', prefs)
                
        if headless:
            chromeOptions.add_argument('headless')
        
        driver = webdriver.Chrome(options=chromeOptions)
        return(driver)
    
    except:
        log.warning('error creating the web driver',exc_info=True)
        raise

def Login(base_link='https://www.ne2.ca/'):
    
    user_name,password = ne2.config_file()
    driver = ScrapeDriver(r'C:\Users\mossgran\Documents\ne2\volumes')
    driver.get(base_link)
    #enter user id and password
    login_info = {'username':user_name,
                  'password':password}
        
    for key,value in login_info.items():
            
        login = driver.find_element_by_id(key)
        login.clear()
        login.send_keys(value)
        
    driver.find_element_by_id('submitbutton').click()
             
    return driver


def Wait(driver,delay,x_path):
    
    '''
    waits until the page is ready before clicking on the next step. Closes and popups that contain "Mark As Read" because
    these can get in the way of clicking on the proper link.

    Parameters
    ----------
    driver : object
        A selenium webdriver pre-loaded to a certain page.
    delay : int
        the number of seconds to delay.
    x_path : string
        The x-path of the object in question.

    Returns
    -------
    driver : object
        A selenium webdriver pre-loaded after the given x-path has been clicked.

    '''
    
    #check if there is a popup and close it
    
    try:
        driver.find_element_by_xpath('//*[text()="Mark As Read"]').click()
        log.warning('closed popup at: '+x_path)
    
    except NoSuchElementException:
        log.warning('no popup at: '+x_path)
    
    
    try:
        myElem = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH,x_path)))
        driver.find_element_by_xpath(x_path).click()
        log.warning('x path ready: '+x_path)
        return driver
    
    except:
        raise
        log.warning('x path failed: '+x_path)


def Trades():
    #TODO: make the download file overwrite the current one...
    driver = Login()
    delay=20
        
    driver = Wait(driver,delay,'//*[@title="Search Trades"]')
    driver = Wait(driver,delay,'//*[text()="Search All Trades"]')
    
    trade_date_from = driver.find_element_by_xpath('//*[@class="DYNI42D-tb-d DYNI42D-tb-k"]')
    datetime_obj = datetime.strptime(trade_date_from.get_attribute('value'),'%m/%d/%Y').date()
    insert_date = datetime.strftime(datetime_obj - relativedelta(months=3),'%m/%d/%Y')
    trade_date_from.clear()
    trade_date_from.send_keys(insert_date)
    
    driver = Wait(driver,delay,x_path='//*[text()="Search"]')
    driver = Wait(driver,delay,x_path='//*[text()="Download to CSV"]')
    driver = Wait(driver,delay,x_path='//*[text()="Click here"]')
    
    found = False
    
    while not found:
        
        if os.path.isfile('volumes/tradesCSV.csv'):
            found = True
            driver.close()
        else:
            found = False
    
        
def CerseiVolumes():
    
    conn,engine = ne2.cer_connection()
    
    if os.path.isfile('volumes/tradesCSV.csv'):
        df = pd.read_csv('volumes/tradesCSV.csv')
    else:
        Trades()
        df = pd.read_csv('volumes/tradesCSV.csv')

    ne2.cersei(df,'Net_Energy_Volumes',conn,new=False)
    
    os.remove('volumes/tradesCSV.csv')    
    conn.close()


if __name__ == "__main__":
    
    #ne2.CreateTable('Net_Energy_Volumes')
    CerseiVolumes()
    
    
    
    
