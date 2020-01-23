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
import ne2
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
#%%

log2 = ne2.ConfigureLog(file_name='get_a_stick')

log2.debug('ne2 volumes')

logging.shutdown()