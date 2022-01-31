# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 14:41:30 2022

@author: JJTHOMAS
"""


import requests
import pandas as pd
from datetime import datetime, timedelta

import os, uuid
import numpy as np
import pyodbc 




try:

    conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                        'Server=glcserver001.database.windows.net;'
                        'Database=glc;'
                        'UID=glc_admin;'
                        'PWD=Jeslin123;'
                        'Trusted_Connection=no;')
    
    cursor = conn.cursor()
    
    


    url = 'https://wbea.org/asi-php/ams_hist/helpers/hist_table.php?s=2&f=T&sy=2021&sm=7&sd=8&sh=0&ey=2021&em=7&ed=9&eh=23&tt=l1mn'


    toDateYY = datetime.today().strftime('%Y')
    toDateMM = datetime.today().strftime('%m')
    toDateDD = datetime.today().strftime('%d')
    print (toDateYY)
    print (toDateMM)
    print (toDateDD)

    fromDate = datetime.today() - timedelta(days=1)
    fromDateYY = fromDate.strftime('%Y')
    fromDateMM = fromDate.strftime('%m')
    fromDateDD = fromDate.strftime('%d')

    print (fromDateYY)
    print (fromDateMM)
    print (fromDateDD)
    stations = ['2','4','5','11']
    all_available_dfs=[]
    for station in stations:
        print (station)
    

        endpoint = 'https://wbea.org/asi-php/ams_hist/helpers/hist_table.php?s='+ station +'&f=T&sy=' + fromDateYY + '&sm=' + fromDateMM + '&sd=' + fromDateDD + '&sh=0&ey=' + toDateYY + '&em=' + toDateMM + '&ed=' + toDateDD + '&eh=23&tt=l1mn'



        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        r = requests.get(endpoint, headers=headers) 
        print (r.status_code)
        if r.status_code == 200:
            if station=='2':
        # may require bs4, lxml, html5lib in environment
                dfs = pd.read_html(r.content, header=0)
                df_2=dfs[0].copy()
                df_2.columns=['Date_Time','SO2_ppb','H2S_ppb','THC_ppm','NMHC_ppm','CH4_ppm','Temp2m_deg_C','RH_','WindSpeed10m_km_h','WindDir_10m_deg_']
                df_2['station_id']=station
                all_available_dfs.append(df_2)
                print("length of station {} are {}".format(station,df_2.shape[0]))
                print(" no.of colums are {}".format(df_2.shape[1]))
            elif station=='4':
                temp_df= pd.read_html(r.content, header=0)
                df_4=temp_df[0].copy()
                df_4.columns=['Date_Time','SO2_ppb','H2S_ppb','THC_ppm','NO_ppb','NO2_ppb','NOX_ppb','O3_ppb','PM2_5_ug_m3','CH4_ppm','NMHC_ppm','Temp2m_deg_C','RH_','WindSpeed10m_km_h','WindDir_10m_deg_']
                df_4['station_id']=station
                all_available_dfs.append(df_4)
                print("length of station {} are {}".format(station,df_4.shape[0]))
                print(" no.of colums are {}".format(df_4.shape[1]))
            elif station=='5':
                temp_df= pd.read_html(r.content, header=0)
                df_5=temp_df[0].copy()
                df_5.columns=['Date_Time','SO2_ppb','H2S_ppb','THC_ppm','CH4_ppm','NMHC_ppm','Temp2m_deg_C','Temp20m_deg_C','Temp45m_deg_C','Temp75m_deg_C','Temp90m_deg_C','RH2m_','RH20m_','RH45m_','RH75m_','RH90m_','WindSpeed20m_km_h','WindSpeed45m_km_h','WindSpeed75m_km_h','WindSpeed90m_km_h','WindDir_20m_deg_','WindDir_45m_deg_','WindDir_75m_deg_','WindDir_90m_deg_','VerticalWindSpeed20m_km_h','VerticalWindSpeed45m_km_h','VerticalWindSpeed75m_km_h','VerticalWindSpeed90m_km_h']
                df_5['station_id']=station
                all_available_dfs.append(df_5)
                print("length of station {} are {}".format(station,df_5.shape[0]))
                print(" no.of colums are {}".format(df_5.shape[1]))
            elif station=='11':
                temp_df= pd.read_html(r.content, header=0)
                df_11=temp_df[0].copy()
                df_11.columns=['Date_Time','SO2_ppb','H2S_ppb','THC_ppm','CH4_ppm','NMHC_ppm','Temp2m_deg_C','RH_','WindSpeed10m_km_h','WindDir_10m_deg_']
                df_11['station_id']=station
                all_available_dfs.append(df_11)
                print("length of station {} are {}".format(station,df_11.shape[0]))
                print(" no.of colums are {}".format(df_11.shape[1]))
        else:
            print(f'Error. Status code is {r.status_code}')
    all_dfs=pd.concat(all_available_dfs, axis=0)
    print("length of rows of merged data is {}".format(all_dfs.shape[0]))
    print(" no.of colums of merged data is {}".format(all_dfs.shape[1]))
    columns_list=all_dfs.columns.tolist()
    columns_list.append(columns_list.pop(columns_list.index('station_id')))
    #print(columns_list)
    final_df=all_dfs[columns_list].copy()
    
    #final_df = final_df.replace(np.nan, '', regex=True)



    final_df=final_df.astype('str')
    #final_df.to_csv('merged_data.csv',index=False)
    params = [tuple(x) for x in final_df.values]
    columns_list = final_df.columns.tolist()
    columns_list_query = f'({(",".join(columns_list))})'
    sr_columns_list = [f'Source.{i}' for i in columns_list]
    sr_columns_list_query = f'({(",".join(sr_columns_list))})'
    rows_to_insert = [row.tolist() for idx, row in final_df.iterrows()]
    rows_to_insert = str(rows_to_insert).replace('[', '(').replace(']', ')')[1:][:-1]
    table_name='dbo.GenericTable'
    query = f"MERGE INTO {table_name} as Target \
            USING (SELECT * FROM \
            (VALUES {rows_to_insert}) \
            AS s {columns_list_query}\
            ) AS Source \
            ON Target.station_id=Source.station_id AND Target.Date_Time=Source.Date_Time \
            WHEN NOT MATCHED THEN \
            INSERT {columns_list_query} VALUES {sr_columns_list_query};"
    cursor.execute(query)
    conn.commit()

    
    
    
    
except Exception as ex:
    print('Exception:')
    print(ex)


