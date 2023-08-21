# -*- coding: utf-8 -*-


from google_play_scraper import app ,Sort,reviews_all
import pandas as pd 
from urllib.parse import urlparse

def file_to_dataframe(file):
    df=pd.read_csv(file)
    return df

def extract_data(df,file):
    data=pd.DataFrame()
    for index,row in df.iterrows():
        try:
            ID=urlparse(row['link']).query.split('=')[1]
            result=app(ID,lang='en',country='us')
            Data=data.append(result,ignore_index=True)
            Data.to_csv(file, mode='a', index=False, header=False)
        except  :
            print('oops')
    return Data
#def dataframe_to_file(Data,file):
    
    
def extract_reviews(df,file):
    data=pd.DataFrame()
    for index,row in df.iterrows():
        try:
            ID=urlparse(row['link']).query.split('=')[1]
            result=reviews_all(ID,lang='en',country='us',sort=Sort.MOST_RELEVANT)
            Data=data.append(result,ignore_index=True)
            Data.to_csv(file, mode='a', index=False, header=False)
        except  :
            print('oops')
    return Data
    
    
    
def main():
    df=file_to_dataframe('C:/Users/Lenovo/lnks/lnks/spiders/final.csv')
    ata=extract_data(df,'C:/Users/Lenovo/lnks/lnks/spiders/final_undup.csv')
    #dataframe_to_file(ata,'C:/Users/Lenovo/lnks/lnks/spiders/final_undup.csv')
    extract_reviews(df, 'C:/Users/Lenovo/lnks/lnks/spiders/reviews.csv')
    return ata 


if __name__ == '__main__':
    main()
    
