# 104_crawler
先執行01_crawler，再執行02_crawler
## 01_crawler
多線程爬取104公司url，並將url存取成清單，以利爬取公司各別網頁資訊
## 02_crawler
透過01_crawler爬完的url，多線程爬取每間公司的資訊，並存成json做為紀錄檔
