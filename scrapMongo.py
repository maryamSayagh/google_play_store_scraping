# -*- coding: utf-8 -*-
"""
Created on Thu Nov 17 20:24:55 2022

@author: Lenovo
"""
#import pymongo
from pymongo import MongoClient
import concurrent.futures
import pandas as pd 
from google_play_scraper import app ,Sort,reviews_all,permissions 
from urllib.parse import urlparse

def file_to_dataframe(file):
    df=pd.read_csv(file)
    return df

def reviews_ID(review,ID): #reviews is a list of dictionnaries 
    for dictio in review: 
        dictio['ID']=ID
    return review 
    
client=MongoClient("mongodb://mrymsygh:saymarsaymar@ac-6ym3kr5-shard-00-00.exdetja.mongodb.net:27017,ac-6ym3kr5-shard-00-01.exdetja.mongodb.net:27017,ac-6ym3kr5-shard-00-02.exdetja.mongodb.net:27017/?ssl=true&replicaSet=atlas-5rew4d-shard-0&authSource=admin&retryWrites=true&w=majority",authSource="admin")        
db=client['scrape_db']
applications=db['apps']
reviews=db['reviews']
permission=db['perm']
def result_app(link):
    
    try:
        ID=urlparse(link).query.split('=')[1]
        apps=app(ID,lang='en',country='us')
    #replacing missing values 
        apps = {k: v or None for k, v in apps.items() }
        review=reviews_all(ID,lang='en',country='us',sort=Sort.MOST_RELEVANT)
        review=reviews_ID(review,ID)
        perm=permissions(ID,lang='en',country='us')
        perm['ID']=ID
        applications.insert_one(apps)
        permission.insert_one(perm)
        reviews.insert_many(review)
    
    except  :
        print('oops')
    
    
    
def extract():
    print('what')
    df=file_to_dataframe("C:\\Users\\Lenovo\\Downloads\\Sample_apps")
        
    for index,row in df.iterrows():
        result_app(row['app'])
        #delete row from dataframe 
        df=df.drop(index)
        df.to_csv("C:/Users/Lenovo/lnks/final2.csv",mode='w',index=False,header=True)

    #delete row from file 
        
 
        
if __name__ == '__main__':
    extract()
    # with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
     #      executor.map(extract)    
           
        
        
        
        
        
        
