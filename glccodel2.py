# -*- coding: utf-8 -*-
"""
Created on Tue Apr 19 08:51:42 2022

@author: JJTHOMAS
"""
import requests
import pandas as pd
import numpy as np
import sys
import logging
import warnings
from datetime import datetime, timedelta


warnings.filterwarnings("ignore")

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', 
                              '%m-%d-%Y %H:%M:%S')

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(formatter)

file_handler = logging.FileHandler('intermediate.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stdout_handler)


toDateYY='2022'
toDateMM='2'
toDateDD='18'
tohour='23'

logging.info("Running upto year {}-{}-{}".format(toDateDD,toDateMM,toDateYY))

# fromDate = sqlfromdate
# fromDateYY = fromDate.strftime('%Y')
# fromDateMM = fromDate.strftime('%m')
# fromDateDD = fromDate.strftime('%d')
fromDateYY='2022'
fromDateMM='2'
fromDateDD='17'
fromhour='0'
mailid="testwbeal2data@outlook.com"

logging.info("Running from {}-{}-{}".format(fromDateDD,fromDateMM,fromDateYY))

#stations = ['2','4','5','11']
stations=['2']
all_available_dfs = []

for station in stations:
    logging.info("Running for station {}".format(station))
    data_={'s': station,'f': 'T','sy':fromDateYY ,'sm': fromDateMM,'sd': fromDateDD,'sh': fromhour,'ey': toDateYY,'em': toDateMM,'ed': toDateDD,'eh': tohour,'tt': 'l2mn','tt': 'l2mn','email': mailid,'n': 'false','_': ''}

    #endpoint = 'https://wbea.org/asi-php/ams_hist/helpers/hist_download.php?s='+ station +'&f=T&sy=' + fromDateYY + '&sm=' + fromDateMM + '&sd=' + fromDateDD + '&sh=0&ey=' + toDateYY + '&em=' + toDateMM + '&ed=' + toDateDD + '&eh=23&tt=l2mn&tt=l2mn' + '&email='+mailid +'&n=false&_='
    endpoint='https://wbea.org/asi-php/ams_hist/helpers/hist_download.php?'
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    r = requests.post(endpoint, headers=headers,data=data_)

    logging.info("API response status code {}".format(r.status_code))