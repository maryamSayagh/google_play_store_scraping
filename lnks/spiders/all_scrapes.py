# -*- coding: utf-8 -*-
"""
Created on Sat Nov 12 17:42:28 2022

@author: Lenovo
"""
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
    
        

def result_app(link,app_file,review_file,perm_file):
    
    #try:
    ID=urlparse(link).query.split('=')[1]
    apps=app(ID,lang='en',country='us')
    #replacing missing values 
    apps = {k: v or None for k, v in apps.items() }
    for item in apps.items():
        str(item)
    review=reviews_all(ID,lang='en',country='us',sort=Sort.MOST_RELEVANT)
    review=reviews_ID(review,ID)
    perm=permissions(ID,lang='en',country='us')
    perm['ID']=ID
    
    df_app=pd.DataFrame(apps)
    df_review=pd.DataFrame(review)
    df_perm=pd.DataFrame(perm)
    df_app.to_csv(app_file, mode='a', index=False, header=False)
    df_review.to_csv(review_file, mode='a', index=False, header=False)
    df_perm.to_csv(perm_file, mode='a', index=False, header=False)
    
    except  :
        print('oops')
    
    
    
def extract():
    df=file_to_dataframe("C:/Users/Lenovo/lnks/final2.csv")
    print('1')
    app_file="C:/Users/Lenovo/lnks/app.csv"
    review_file="C:/Users/Lenovo/lnks/reviews.csv"
    perm_file="C:/Users/Lenovo/lnks/perm.csv"
    print('2')
    for index,row in df.iterrows():
        print('3')
        result_app(row['link'],app_file,review_file,perm_file)
        #delete row from dataframe 
        df=df.drop(index)
        df.to_csv("C:/Users/Lenovo/lnks/final2.csv",mode='w',index=False,header=True)

        

with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
    executor.map(extract)        
        
#if __name__ == '__main__':
 #   extract()
           
        
        
        
        
        
        
        
        
        
        