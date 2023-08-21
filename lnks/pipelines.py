# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pandas as pd 
from scrapy.exceptions import DropItem

class LnksPipeline:
    def __init__(self):
       self.ids_seen = set()

    
    def process_item(self, item, spider):
        adapter=ItemAdapter(item)
        read=pd.read_csv('C:/Users/Lenovo/lnks/lnks/spiders/final.csv')
        for index,row in read.iterrows():
            r=0
            if adapter['Link']== row['link']:
                r=1
            elif adapter['Link'] in self.ids_seen:
                r=2
        if r==1:
            raise DropItem(f"Duplicate item found: {item!r}")
        elif r==2:
            raise DropItem(f"Duplicate item found: {item!r}")
        else:
            self.ids_seen.add(adapter['Link'])
            return item
