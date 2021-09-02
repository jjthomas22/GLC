
import requests
import pandas as pd
from datetime import datetime, timedelta

import os, uuid
import pyodbc 



try:

    conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=glcserver001.database.windows.net;'
                        'Database=glc;'
                        'UID=glc_admin;'
                        'PWD=Jeslin123;'
                        'Trusted_Connection=no;')
    
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM glc.dbo.test')
 
    for row in cursor:
        print(row)
    


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

    endpoint = 'https://wbea.org/asi-php/ams_hist/helpers/hist_table.php?s=2&f=T&sy=' + fromDateYY + '&sm=' + fromDateMM + '&sd=' + fromDateDD + '&sh=0&ey=' + toDateYY + '&em=' + toDateMM + '&ed=' + toDateDD + '&eh=23&tt=l1mn'



    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    r = requests.get(endpoint, headers=headers) 
    print (r.status_code)
    if r.status_code == 200:
        # may require bs4, lxml, html5lib in environment
        dfs = pd.read_html(r.content, header=0)
        for i, df in enumerate(dfs):
            dfs[i].columns = ['Time_Date','SO2_ppb','H2S_ppb','THC_ppm','NMHC_ppm','CH4_ppm','Temp_2m_deg_C','RH','WindSpeed_10m','WindDir_10m']
            print(dfs[i])
            for index, row in dfs[i].iterrows():
                
            #cursor.execute("INSERT INTO GLC.dbo.test4(DepartmentID,Name,GroupName) values(?,?,?)", row.DepartmentID, row.Name, row.GroupName)
 
                sqlstmt = "INSERT INTO [dbo].[test4] \
                    ([Time_Date] \
                         ,[SO2_ppb] \
                             ,[H2S_ppb] \
                                 ,[THC_ppm] \
                                     ,[NMHC_ppm] \
                                         ,[CH4_ppm] \
                                             ,[Temp_2m_deg_C] \
                                                 ,[RH] \
                                                     ,[WindSpeed_10m] \
                                                         ,[WindDir_10m]) \
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
     
                cursor = conn.cursor()
               
                cursor.execute(sqlstmt,row.Time_Date, row.SO2_ppb, row.H2S_ppb, row.THC_ppm, row.NMHC_ppm,row.CH4_ppm, row.Temp_2m_deg_C, row.RH, row.WindSpeed_10m, row.WindDir_10m)

                conn.commit()
                cursor.close()
            
           

        
    else:
        print(f'Error. Status code is {r.status_code}')
    
    
    
except Exception as ex:
    print('Exception:')
    print(ex)


# If all works well you should see something like this:
'''
Table 0 with shape (4, 28)
Columns are:
Index(['Timeyyyy-mm-ddhh:mm', 'SO2(ppb)', 'H2S(ppb)', 'THC(ppm)', 'CH4(ppm)',
       'NMHC(ppm)', 'Temp@ 2m(deg. C)', 'Temp@ 20m(deg. C)',
       'Temp@ 45m(deg. C)', 'Temp@ 75m(deg. C)', 'Temp@ 90m(deg. C)',
       'RH@ 2m(%)', 'RH@ 20m(%)', 'RH@ 45m(%)', 'RH@ 75m(%)', 'RH@ 90m(%)',
       'WindSpeed@ 20m(km/h)', 'WindSpeed@ 45m(km/h)', 'WindSpeed@ 75m(km/h)',
       'WindSpeed@ 90m(km/h)', 'WindDir.@ 20m(deg.)', 'WindDir.@ 45m(deg.)',
       'WindDir.@ 75m(deg.)', 'WindDir.@ 90m(deg.)',
       'VerticalWindSpeed@ 20m (km/h)', 'VerticalWindSpeed@ 45m (km/h)',
       'VerticalWindSpeed@ 75m (km/h)', 'VerticalWindSpeed@ 90m(km/h)'],
      dtype='object')
'''