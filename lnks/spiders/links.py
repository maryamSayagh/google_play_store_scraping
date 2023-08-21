import scrapy
import pandas as pd 
from ..items import LnksItem
from urllib.parse import urlparse,urljoin,urlencode
list_a=[]
list_b=[]

class LinksSpider(scrapy.Spider):
    name = 'links'
    allowed_domains = ['play.google.com']
    start_urls = ['http://play.google.com/']
   

    def get_link(self , response):#giving a list of links
        apps=response.xpath('//*[@class="Si6A0c ZD8Cqc"]')
        l=[]
        for app in apps: 
            link=app.xpath('@href').extract()
            l.append(link)
        return l
    
    def start_requests(self):
        url=['https://play.google.com/store/apps']
        yield scrapy.Request(url=url[0],callback=self.parse)
    def parse(self, response):
        df=pd.read_csv('C:/Users/Lenovo/lnks/lnks/spiders/final.csv')
        df=df.reset_index()
        
        for index,row  in df.iterrows():
            url=urljoin('https://play.google.com',row['link'])
            yield scrapy.Request(url=url,callback=self.parse_similar,meta={'Link':row['link']})
    def parse_similar(self,response):
        apps=response.xpath('//*[@class="Si6A0c nT2RTe"]')
        for app in apps: 
            link=app.xpath('@href').extract()
            list_a.append(link)
        #maybe change this list with a file.. so the programme continue from where it stoped    
        #it will also reduce the number of request 
        list_b.append(response.meta.get('Link'))
        item=LnksItem()
        item['Link']=response.meta.get('Link')
        yield item 
        ####
        for link in list_a:
            r=0
            for i in range(len(list_b)):
                if link[0] ==list_b[i] :
                    r=1
        
            if r==0:
                url=urljoin('https://play.google.com',link[0])
                yield scrapy.Request(url=url,callback=self.parse_similar,meta={'Link':link[0]})
                
        # link in list_b:
            #yield {'link':link}