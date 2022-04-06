# -*- coding: utf-8 -*-
"""
Created on Sun Mar 27 14:55:02 2022

@author: vahid
crul google based 
"""

print('in the name of gad')
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup as b
import pandas as pd
import numpy as np
from bs2json import bs2json
import re
import json
import copy
from copy import deepcopy
import requests
from collections import OrderedDict
#from iteration_utilities import unique_everseen
import time
import itertools

from urllib.parse import urlparse



import datetime
import os

import pyodbc
from pyodbc import *
import psycopg2
import pandas.io.sql as psql
from sqlalchemy import create_engine



connection = psycopg2.connect(user="postgres",
                            password="12344321",
                            host="10.32.141.17",
                            port="5432",
                            database="Vahid01")
cursor = connection.cursor()

sarasari_00= psql.read_sql('select * from public.sarasari_1', connection)

print('فراخونی سراسری')

print(len(sarasari_00))
# date

sarasari_00['تاریخ'] = sarasari_00['تاریخ'].astype(str)
sarasari_01 = sarasari_00
# df_EPG_8 = df_EPG
sarasari_01['m_date'] = sarasari_01['تاریخ'].str[:8]
sarasari_01['m_date_4'] = sarasari_01['تاریخ'].str[:4]

sarasari_01['m_date_4'] = sarasari_01['m_date_4'].astype(int)
# sarasari_01['m_date_4'] = sarasari_01['m_date_4'].int.strip()

# sarasari_0100 = sarasari_01[1:1000]
print(len(sarasari_01))

# sarasari_01 = list(filter(lambda x: x > 2019 ,sarasari_01['m_date_4']))

df_new = sarasari_01[sarasari_01['m_date_4']>2019]
df_new_1= sarasari_01[sarasari_01['m_date_4']>2020]

print(len(df_new))

df1 = df_new_1

# df1 = pd.read_excel(r'D:\python\source code\google crawler\row data for crawler1.xlsx')
df111 = df1.copy()


df1 = df1[['نام شبکه','نام برنامه اولیه','اپراتور']]
df0 = df1.copy()
df0 = df0.rename(columns = {"نام شبکه":"chan"})
df0 = df0.rename(columns = {"اپراتور":"operator"})
df0 = df0.rename(columns = {"نام برنامه اولیه":"Program"})
df0['chan'] = df0['chan'].str.strip()
df0['operator'] = df0['operator'].str.strip()

# df2 =df0
df2=df0.query("operator == 'لنز'")

df2.to_excel(r"C:\Users\Administrator\Desktop\vahid\data\out_lenz\rowdata_lenz.xlsx", index=False)

df20=df2.query("chan == 'شبکه 1'")
print()
df21=df2.query("chan == 'شبکه 2'")
df22=df2.query("chan == 'شبکه 3'")
df23=df2.query("chan == 'شبکه 4'")

df24=df2.query(" chan == 'شبکه 5'")
df25=df2.query(" chan == 'افق'")
df26=df2.query(" chan == 'قرآن'")
df27=df2.query(" chan == 'پویا'")
df28=df2.query(" chan == 'نسیم'")
df29=df2.query(" chan == 'مستند'")
df30=df2.query(" chan == 'جام جم 1'")

df31=df2.query(" chan == 'ورزش'")
df32=df2.query(" chan == 'العالم'")
df33=df2.query(" chan == 'آموزش'")
df34=df2.query(" chan == 'امید'")
df35=df2.query(" chan == 'آی فیلم'")
df36=df2.query(" chan == 'تماشا'")
df37=df2.query(" chan == 'سلامت'")
df38=df2.query(" chan == 'شما'")
df39=df2.query(" chan == 'ایران کالا'")
df40=df2.query(" chan == 'نمایش'")
df41=df2.query(" chan == 'خبر'")
df42=df2.query(" chan == 'پرس تی وی'")
df43=df2.query(" chan == 'الکوثر'")
df44=df2.query(" chan == 'سپهر'")
# df45=df2.query(" chan == 'جام جم 1'")




len(df21)


a= pd.DataFrame()

my_list = [df20, df21, df22, df23, df24,df25, df26, df27, df28, df29, df30, df31,df32,df33,df34,df35,df36,df37,df38,df39,df40,df41,df42,df43,df44]

print('write compeletly')

    
for vahid in range(25):

#        print("number of data frame for sarasari", vahid)

    ch= vahid + 1
#        print("path_out", ch)
    a=my_list[vahid]

    path_out  =r'C:\Users\Administrator\Desktop\vahid\data\channel\{}.xlsx'.format(ch)

    a.to_excel( path_out, index=False)



df20 = df20.drop_duplicates(subset=['Program'])

df21 = df21.drop_duplicates(subset=['Program'])
df22 = df22.drop_duplicates(subset=['Program'])
df23 = df23.drop_duplicates(subset=['Program'])
df24 = df24.drop_duplicates(subset=['Program'])

df25 = df25.drop_duplicates(subset=['Program'])
df26 = df26.drop_duplicates(subset=['Program'])
df27 = df27.drop_duplicates(subset=['Program'])
df28 = df28.drop_duplicates(subset=['Program'])
df29 = df29.drop_duplicates(subset=['Program'])
df30 = df30.drop_duplicates(subset=['Program'])

df31 = df31.drop_duplicates(subset=['Program'])
df32 = df32.drop_duplicates(subset=['Program'])
df33 = df33.drop_duplicates(subset=['Program'])
df34 = df34.drop_duplicates(subset=['Program'])
df35 = df35.drop_duplicates(subset=['Program'])
df36 = df36.drop_duplicates(subset=['Program'])
df37 = df37.drop_duplicates(subset=['Program'])
df38 = df38.drop_duplicates(subset=['Program'])
df39 = df39.drop_duplicates(subset=['Program'])
df40 = df40.drop_duplicates(subset=['Program'])

df41 = df41.drop_duplicates(subset=['Program'])
df42 = df42.drop_duplicates(subset=['Program'])
df43 = df43.drop_duplicates(subset=['Program'])
df44 = df44.drop_duplicates(subset=['Program'])



dff = [df20, df21, df22, df23, df24,df25, df26, df27, df28, df29, df30, df31,df32,df33,df34,df35,df36,df37,df38,df39,df40,df41,df42,df43,df44]

bb = pd.DataFrame()

for vahid1 in range(25):

#        print("number of data frame for sarasari", vahid)

    ch1= vahid1 + 1
#        print("path_out", ch)
    bb=dff[vahid1]

    path_out  =r'C:\Users\Administrator\Desktop\vahid\data\dub\{}.xlsx'.format(ch1)

    bb.to_excel( path_out, index=False)

# dff = [df20,df21,df22,df23,df24]

# dff.to_excel(r"D:\python\source code\google crawler\fff.xlsx", index=False)

az = 'telewebion.com'
print(az)
# dfsample = df22
# print(dfsample)

# ii = 0
total = pd.DataFrame()

for ii in range(25):
    dfsample = dff[ii]
    print(dfsample)
    
    channel_list = list(itertools.chain(*dfsample.iloc[:, [0]].values.tolist()))
    programName_list = list(itertools.chain(*dfsample.iloc[:, [1]].values.tolist()))
    print(programName_list)

    print(len(programName_list))
    print(len(channel_list))



# i=1

    counter = len(programName_list)

    cc = 'H'
    cc=list(cc)

    dd=list(cc)

    

    for i  in range(counter):
        
        bb = programName_list[i]
        aa = programName_list[i] + ' ' + '+ ' + channel_list[i] + ' ' + '+ تلوبیون '
        print(bb)

        driver = webdriver.Chrome(r'C:\Users\Administrator\Desktop\vahid\data\chromedriver.exe')
        driver.get('https://www.google.com/')

        c=driver.find_element_by_name("q")
# c.send_keys("تلوبیون + اخبار شبانگاهی شبکه سه")
        c.send_keys(aa)

        c.send_keys(Keys.ENTER)
        
        try:
            fb= WebDriverWait(driver, 50).until(EC.presence_of_element_located((By.XPATH, '//*[@id="rso"]')))
            html=driver.execute_script("return arguments[0].outerHTML;",fb)

            page_html_soup=b(html,'html.parser')
            converter = bs2json() 
# print(page_html_soup)
        
            
            class_find_name= page_html_soup.findAll('div',{'class':'yuRUbf'}) 

            json_class_find_name = converter.convertAll(class_find_name)
    # print(len(json_class_find_name))

    # print(json_class_find_name [0]['div']['a']['attributes']['href'])
            try:
                domain = urlparse(json_class_find_name [0]['div']['a']['attributes']['href']).netloc
                # print(domain)

                if az == domain:
                    # print(json_class_find_name [0]['div']['a']['h3']['span']['text'])
                    telewbion_name = json_class_find_name [0]['div']['a']['h3']['span']['text']
        
                    cc.append(bb)
                    dd.append(telewbion_name)
        

                lst11 = cc[1 :]
                lst22 = dd[1 :]
            # len(cc)


            # print(lst11)
                df = pd.DataFrame((zip(lst11, lst22)),
                          columns =['lenz', 'telewbion_name'])
        
#                print(df)    

                df.to_excel(r"C:\Users\Administrator\Desktop\vahid\data\test\g_telwebion_2.xlsx", index=False)

                dfc = df.copy()

                dfe = dfc['telewbion_name'].str.split('|', expand=True)


                dfe.to_excel(r"C:\Users\Administrator\Desktop\vahid\data\test\google telebion_split1.xlsx", index=False)

                result = pd.concat([df, dfe], axis=1)

                result = result.rename(columns = {0:"telewbion_name_0"})
                result = result.rename(columns = {1:"telewbion_0"})

                dfe1 = result['telewbion_name_0'].str.split('-', expand=True)

                result0 = pd.concat([result, dfe1], axis=1)


                result0 = result0.rename(columns = {0:"telewbion_name_new"})

                result0 = result0[['lenz','telewbion_name','telewbion_name_0','telewbion_name_new','telewbion_0',1]]

                result0.to_excel(r"C:\Users\Administrator\Desktop\vahid\data\test\split2.xlsx", index=False)

                total = total.append(result0)
                iii = ii + 1
    
    
                path_out  = r'C:\Users\Administrator\Desktop\vahid\data\result\{}.xlsx'.format(iii)
        
                result0.to_excel( path_out, index=False)
    
            

            except:
                pass
        except:
            pass      
        
        
total.to_csv(r"C:\Users\Administrator\Desktop\vahid\data\total_csv.csv", index=False)
total.to_excel(r"C:\Users\Administrator\Desktop\vahid\data\total.xlsx", index=False)        