# -*- coding: utf-8 -*-
"""
Created on Sun Mar 20 14:48:52 2022

@author: JJTHOMAS
"""

"""
Created on Wed Jan  5 14:41:30 2022
@author: JJTHOMAS
"""

""" GLC Data Scraper
This script scrapes web data from the WBEA website (in 15 minute intervals). 
The provided station ID's are used to specify which table values will be collected. 
After the required values are stored in a dataframe array, an SQL query is used 
to insert and/or merge the data into the provided database and corresponding table(s).
This script requires the following Python modules be installed:
    - `requests`
    - `pandas`
    - `numpy`
    - `pyodbc`
The script's general structure:
    Import statements
    Log handling statements and values
    Try:
        Database configuration
        Establishing connection to database
        Time interval and station setup
        For (station within the list of stations):
            Establishing communication with api endpoint
            If (status code == "OK"):
                Collect and store requested data points for each station
            Else:
                Generate "error" log with corresponding status code
        Clean up and format data
        Query SQL database and merge/insert data
        Commit changes to database
    Except:
        Generate "error" log with exception info
"""

import requests
import pandas as pd
import numpy as np
import pyodbc
import os, uuid
import sys
import logging
import warnings
import configparser
from datetime import datetime, timedelta


warnings.filterwarnings("ignore")
#logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',level=logging.INFO,datefmt='%Y-%m-%d %H:%M:%S')

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

#Variable declaration
sqlfromdate = ''
selectstmt = "SELECT cast(min(Date_Time) as date) as Final_Date \
                  from (SELECT max(Date_Time) as date_time, station_id \
                        FROM [WBEA_Live_Bronze_v2] group by station_id) as temp"


try:
    logging.info("Reading 'credentials.ini' file")

    config = configparser.ConfigParser()
    config.read('credentials.ini')
    host=config.get("Database Credentials",'host')
    db=config.get("Database Credentials",'database')
    username=config.get("Database Credentials",'username')
    password=config.get("Database Credentials",'password')
    logging.info("Connecting to the database session")
    conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=%s;Database=%s;UID=%s;PWD=%s;Trusted_Connection=no;'%(host,db,username,password))
    cursor = conn.cursor()
    logging.info("Connected to database")    
    
    resultset = cursor.execute(selectstmt)
    for r in resultset:
        sqlfromdate = r[0]     
        logging.info('Data fetched is =',r[0])

    toDateYY = datetime.today().strftime('%Y')
    toDateMM = datetime.today().strftime('%m')
    toDateDD = datetime.today().strftime('%d')

    logging.info("Running upto year {}-{}-{}".format(toDateDD,toDateMM,toDateYY))

    fromDate = sqlfromdate
    fromDateYY = fromDate.strftime('%Y')
    fromDateMM = fromDate.strftime('%m')
    fromDateDD = fromDate.strftime('%d')

    logging.info("Running from {}-{}-{}".format(fromDateDD,fromDateMM,fromDateYY))

    stations = ['2','4','5','11']
    all_available_dfs = []

    for station in stations:
        logging.info("Running for station {}".format(station))

        endpoint = 'https://wbea.org/asi-php/ams_hist/helpers/hist_table.php?s='+ station +'&f=T&sy=' + fromDateYY + '&sm=' + fromDateMM + '&sd=' + fromDateDD + '&sh=0&ey=' + toDateYY + '&em=' + toDateMM + '&ed=' + toDateDD + '&eh=23&tt=l1mn'

        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        r = requests.get(endpoint, headers=headers) 

        logging.info("API response status code {}".format(r.status_code))

        if r.status_code == 200:
            if station == '2':
        # may require bs4, lxml, html5lib in environment
                dfs = pd.read_html(r.content, header=0)
                df_station_2 = dfs[0].copy()
                df_station_2.columns = ['Date_Time','SO2_ppb','H2S_ppb','THC_ppm','NMHC_ppm','CH4_ppm','Temp2m_deg_C','RH_','WindSpeed10m_km_h','WindDir_10m_deg_']
                df_station_2['station_id'] = station
                all_available_dfs.append(df_station_2)
                logging.info("Number of data points fetched for station {} are: {}".format(station,df_station_2.shape[0]))
                #logging.info("Number of columns available information for are {}".format(df_2.shape[1]))

            elif station == '4':
                temp_df = pd.read_html(r.content, header=0)
                df_station_4 = temp_df[0].copy()
                df_station_4.columns=['Date_Time','SO2_ppb','H2S_ppb','THC_ppm','NO_ppb','NO2_ppb','NOX_ppb','O3_ppb','PM2_5_ug_m3','CH4_ppm','NMHC_ppm','Temp2m_deg_C','RH_','WindSpeed10m_km_h','WindDir_10m_deg_']
                df_station_4['station_id'] = station
                all_available_dfs.append(df_station_4)
                logging.info("Number of data points fetched for station {} are: {}".format(station,df_station_4.shape[0]))
                #logging.info("Number of columns are {}".format(df_4.shape[1]))

            elif station == '5':
                temp_df = pd.read_html(r.content, header=0)
                df_station_5 = temp_df[0].copy()
                df_station_5.columns = ['Date_Time','SO2_ppb','H2S_ppb','THC_ppm','CH4_ppm','NMHC_ppm','Temp2m_deg_C','Temp20m_deg_C','Temp45m_deg_C','Temp75m_deg_C','Temp90m_deg_C','RH2m_','RH20m_','RH45m_','RH75m_','RH90m_','WindSpeed20m_km_h','WindSpeed45m_km_h','WindSpeed75m_km_h','WindSpeed90m_km_h','WindDir_20m_deg_','WindDir_45m_deg_','WindDir_75m_deg_','WindDir_90m_deg_','VerticalWindSpeed20m_km_h','VerticalWindSpeed45m_km_h','VerticalWindSpeed75m_km_h','VerticalWindSpeed90m_km_h']
                df_station_5['station_id'] = station
                all_available_dfs.append(df_station_5)
                logging.info("Number of data points fetched for station {} are: {}".format(station,df_station_5.shape[0]))
                #logging.info("Number of columns are {}".format(df_5.shape[1]))

            elif station == '11':
                temp_df = pd.read_html(r.content, header=0)
                df_station_11 = temp_df[0].copy()
                df_station_11.columns = ['Date_Time','SO2_ppb','H2S_ppb','THC_ppm','CH4_ppm','NMHC_ppm','Temp2m_deg_C','RH_','WindSpeed10m_km_h','WindDir_10m_deg_']
                df_station_11['station_id'] = station
                all_available_dfs.append(df_station_11)
                logging.info("Number of data points fetched for station {} are: {}".format(station,df_station_11.shape[0]))
                #logging.info("Number of columns are {}".format(df_11.shape[1]))
        else:
            logging.error('Error! Status code is: {r.status_code}')
    
    all_dfs = pd.concat(all_available_dfs, axis=0)

    logging.info("Number of data points for merged data is {}".format(all_dfs.shape[0]))
    #logging.info("Number of columns of merged data is {}".format(all_dfs.shape[1]))

    columns_list = all_dfs.columns.tolist()
    columns_list.append(columns_list.pop(columns_list.index('station_id')))

    final_df = all_dfs[columns_list].copy()
  # final_df = final_df.sort_values(by='Date_Time', ascending=False)
  # final_df = final_df.iloc[16:]
    final_df = final_df.astype('str')

  #  params = [tuple(x) for x in final_df.values]

    columns_list = final_df.columns.tolist()
    columns_list_query = f'({(",".join(columns_list))})'

    sr_columns_list = [f'Source.{i}' for i in columns_list]
    sr_columns_list_query = f'({(",".join(sr_columns_list))})'

    rows_to_insert = [row.tolist() for idx, row in final_df.iterrows()]
    rows_to_insert = str(rows_to_insert).replace('[', '(').replace(']', ')')[1:][:-1]
    table_name='dbo.WBEA_Live_Bronze_Test_v4'

    # SQL 'MERGE' Query
    query = f"MERGE INTO {table_name} as Target \
            USING (SELECT * FROM \
            (VALUES {rows_to_insert}) \
            AS s {columns_list_query}\
            ) AS Source \
            ON Target.station_id=Source.station_id AND Target.Date_Time=Source.Date_Time \
            WHEN NOT MATCHED THEN \
            INSERT {columns_list_query} VALUES {sr_columns_list_query} \
            WHEN MATCHED AND (Target.Temp2m_deg_C != Source.Temp2m_deg_C) THEN UPDATE \
            SET Target.Temp2m_deg_C = Source.Temp2m_deg_C, \
            Target.Temp20m_deg_C = Source.Temp20m_deg_C, \
            Target.Temp45m_deg_C = Source.Temp45m_deg_C, \
            Target.Temp75m_deg_C = Source.Temp75m_deg_C, \
            Target.Temp90m_deg_C = Source.Temp90m_deg_C, \
            Target.RH2m_ = Source.RH2m_, \
            Target.RH20m_ = Source.RH20m_, \
            Target.RH45m_ = Source.RH45m_, \
            Target.RH75m_ = Source.RH75m_, \
            Target.RH90m_ = Source.RH90m_, \
            Target.WindSpeed20m_km_h = Source.WindSpeed20m_km_h, \
            Target.WindSpeed45m_km_h = Source.WindSpeed45m_km_h, \
            Target.WindSpeed75m_km_h = Source.WindSpeed75m_km_h, \
            Target.WindSpeed90m_km_h = Source.WindSpeed90m_km_h, \
            Target.WindDir_20m_deg_ = Source.WindDir_20m_deg_, \
            Target.WindDir_45m_deg_ = Source.WindDir_45m_deg_, \
            Target.WindDir_75m_deg_ = Source.WindDir_75m_deg_, \
            Target.WindDir_90m_deg_ = Source.WindDir_90m_deg_, \
            Target.VerticalWindSpeed20m_km_h = Source.VerticalWindSpeed20m_km_h, \
            Target.VerticalWindSpeed45m_km_h = Source.VerticalWindSpeed45m_km_h, \
            Target.VerticalWindSpeed75m_km_h = Source.VerticalWindSpeed75m_km_h, \
            Target.VerticalWindSpeed90m_km_h = Source.VerticalWindSpeed90m_km_h;"
    
    logging.info("Inserting {} records into database".format((all_dfs.shape[0])))

    cursor.execute(query)
    conn.commit()

    logging.info("Inserted into the database")
except Exception as ex:
    logging.error('Exception is : {}'.format(ex))