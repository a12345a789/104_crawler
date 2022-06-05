# -*- coding: utf-8 -*-
"""
Created on Tue May 10 23:56:09 2022

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
import threading #多線程任務套件
from threading import Thread
from queue import Queue
from concurrent.futures import ThreadPoolExecutor#追蹤多線程的bar
import string
import json

path='./company_info/'
#建立資料夾路徑，同時看資料夾是否存在
try:
    os.makedirs(path)
except FileExistsError:
    print("檔案已存在。")
# 權限不足的例外處理
except PermissionError:
    print("權限不足。")
df=pd.read_csv('company_info_part_all_new.csv',encoding='utf-8-sig')
df.drop_duplicates(inplace=True)

#讀取已爬完的url
try:
    url_list_backup=pd.read_json('url_list.json')
    url_list_backup.rename(columns={0:'url'},inplace=True)
    t=url_list_backup['url'].tolist()
except:
    print('資料不存在')


#測試寫入log，此為本次新增的部分

logs=[]
#建立一個list清單
custno=[]
for i in df['顧客編碼']:
    custno.append(i)
#將url塞進清單中
url_b=[]
for i in custno:
    url_list=f'https://www.104.com.tw/company/ajax/content/'+str(i)+'?'
    url_b.append(url_list)
urls=[]
#建立一個尚未爬取的url清單，先檢測是否有已經爬過的url清單(從json讀取的t檢視)
for x in url_b:
    try:
        if x not in t:
            urls.append(x)
    except:
        urls=url_b
start_time=time.time()   
Que = Queue() #初始化隊伍
#定義一個線程函數
class myThread(threading.Thread):
    def __init__(self, name, link_range):
        threading.Thread.__init__(self)
        self.name = name
        self.link_range = link_range
    def run(self):
        print("Starting " + self.name)
        crawl(self.name, self.link_range)
        print("Exiting " + self.name)
#建立爬蟲函數
def crawl(threadNmae, link_range):
    for i in range(link_range[0], link_range[1]+1):
        try:
            headers = {
              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.92 Safari/537.36',
                'Referer': f'https://www.104.com.tw/company/ajax/content/'+str(i)+'?'
                        }
            r = requests.get(urls[i], timeout=1.5,headers=headers)
            soup=BeautifulSoup(r.text, "html.parser") # text 屬性就是 html 檔案
            if r.status_code != requests.codes.ok:
                print('請求失敗', r.status_code)
                return
            data = r.json()
            df_i=pd.json_normalize(data['data'])
            fname=path+'response_company_info'+str(i)+'.csv'
            df_i.to_csv(fname,encoding='utf-8-sig',index=False)
            time.sleep(random.randint(15,20)) #隨機等待
            Que.put(urls[i])
            while True:
                try:
                    logLi=Que.get(timeout=1)
                    if logLi !=[]:
                        logs.append(logLi)
                except:
                    break
            with open('url_list.json','w',encoding='utf8') as fp:
                json.dump(logs,fp)
            print(threadNmae, r.status_code, urls[i])
        except Exception as e:
            print(threadNmae, "Error: ", e)

threads = []
#link_range_list =[(0,10000),(10001,20000),(20001,30000),(30001,40000),(40001,50000),(50001,60000),(60001,70000),(70001,80000),(80001,100000)]
#定義urls相對的位置
link_range_list =[(0,int((len(urls)/5)/len(urls) *len(urls))),
 (int((len(urls)/5)/len(urls) *len(urls)),int((len(urls)/5)/len(urls) *len(urls))*2),
 (int((len(urls)/5)/len(urls) *len(urls))*2,int((len(urls)/5)/len(urls) *len(urls))*3),
 (int((len(urls)/5)/len(urls) *len(urls))*3,int((len(urls)/5)/len(urls) *len(urls))*4),
 (int((len(urls)/5)/len(urls) *len(urls))*4,int((len(urls)/5)/len(urls) *len(urls))*5)]

#創建n個線程，每個線程分別爬取
for i in range(1,6): #5個線程
    thread = myThread("Thread-" + str(i), link_range=link_range_list[i-1])
    # 開啟新的線程
    thread.start()
    # 添加新的線程到線程列表
    threads.append(thread)
    
finish_time = time.time()   
print("執行完畢，共花費{total_time}分".format(total_time=(finish_time-start_time)/60))

