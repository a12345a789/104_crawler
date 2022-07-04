# -*- coding: utf-8 -*-
"""
Created on Tue Apr 19 13:22:33 2022

@author: Owen
"""
#匯入套件
import numpy as np
import pandas as pd
import requests #爬蟲必備套件
from bs4 import BeautifulSoup #爬蟲必備套件
import re
import random
import time
from urllib.parse import quote
from tqdm import tqdm, trange #讀取進度條套件
import csv
from selenium import webdriver #爬蟲必備套件(可以模擬瀏覽器操作)
from selenium.webdriver.common.keys import Keys #爬蟲必備套件
from selenium.webdriver.chrome.options import Options #爬蟲必備套件
import datetime
from datetime import date
from IPython.display import clear_output #可以清除tqdm進度條
import os,os.path
from glob import glob 
import itertools
#多線程任務套件
from threading import Thread
from queue import Queue
#追蹤多線程的bar
from concurrent.futures import ThreadPoolExecutor

#查看目前路徑
os.getcwd()
os.chdir('/Users/a12345/Desktop/python')
path2='./industry/'

#建立資料夾路徑，同時看資料夾是否存在
try:
  os.makedirs(path2)
except FileExistsError:
  print("檔案已存在。")
# 權限不足的例外處理
except PermissionError:
  print("權限不足。")

#手動先指定產業list(沒有上述清單時，可用下面的list，下方list是104的產業編號)
industry_list=['1003000000','1005000000','1006000000','1007000000','1009000000','1001000000','1002000000','1014000000','1010000000','1013000000','1004000000','1008000000','1011000000','1012000000','1015000000','1016000000']

#清空暫存檔案
files_remove=glob('response_company_new_104**.csv')
print(files_remove)
try:
    for i in files_remove:
        os.remove(i)
except OSError as e:
    print(e)
else:
    print("File is deleted successfully")

#程式開始執行的時間
start_time = time.time()
#多程序執行初始化，注意：多執行程序化有一定的可能性會被擋ip，後續可以加入代理ip流程
Que = Queue() #初始化隊伍
for n in range(100): #上限100頁
    page_num=n+1
    Que.put(page_num) #將頁碼送進去隊伍

#定義爬蟲函式
def get_job():
    while Que.qsize()>1:
        page_num = Que.get()
        for i in industry_list:
            for j in range(0,9):
                url = f' https://www.104.com.tw/company/ajax/list?jobsource=tab_job_to_cs&mode=s&page='+format(page_num)+'&emp='+str(j)+'&indcat='+str(i)
                print('目前爬取頁面是：' + url)
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.92 Safari/537.36',
                'Referer': f'https://www.104.com.tw/company/ajax/list?jobsource=tab_job_to_cs&mode=s&page='+format(page_num)
                }
                r = requests.get(url, headers=headers)
                soup=BeautifulSoup(r.text, "html.parser") # text 屬性就是 html 檔案
                if r.status_code != requests.codes.ok:
                    print('請求失敗', r.status_code)
                    return       
                data = r.json()
                df_i=pd.DataFrame(data['data'])
                fname='response_company_new_104_'+str(page_num)+'_'+str(i)+'part_'+str(j)+'.csv'
                df_i.to_csv(fname)
                time.sleep(random.randint(10,15)) #隨機等待
            
#運作多線程
Thread_Team = []
for x in range(10): #共10個線程，數值愈大爬蟲愈快，但反之ip容易被擋
    t=Thread(target=get_job)
    t.start()
    Thread_Team.append(t)
for t in Thread_Team:
    t.join()

#將公司的資料合併成一個DataFrame
files=glob('response_company_new_104*.csv')
print(files)
df_a = pd.concat(
            (pd.read_csv(file) for file in files if len(file) > 0), ignore_index=True)#忽略沒有資料的DataFrame
df_a['網爬日期']=datetime.date.today()   

#處理基本資料欄位
df_a.rename(columns={'encodedCustNo':'顧客編碼',
                     'name':'公司名稱',
                     'areaDesc':'地區',
                     'industryDesc':'產業類別',
                     'capitalDesc':'資本額',
                     'jobCount':'工作數量',
                     'employeeCountDesc':'員工人數'},inplace=True)
column_r=['顧客編碼','公司名稱','地區','產業類別','資本額','工作數量','員工人數']
df_b=df_a[column_r].copy()
#處理字段，取出數值
df_b['員工人數']=df_a['員工人數'].str.extract('(\d+)')
df_b['資本額金額']=df_a['資本額'].str.extract('(\d+)')
#將資本額單位擷取出來
unit =['億','萬元']
def matcher(x):
    for i in unit:
        if i.lower() in x.lower():
            return i
    else:
        return np.nan
df_b['資本額單位'] = df_b['資本額'].apply(matcher) #新增資本額單位的欄位
df_b.to_csv('company_info_part_all_new.csv',encoding='utf-8-sig',index=False)

total=df_b["顧客編碼"].nunique()
finish_time = time.time()
#clear_output() #可以清掉長條圖的進度
print("執行完畢，共花費{total_time}分，總共{total}筆".format(total_time=(finish_time-start_time)/60,total=total))
