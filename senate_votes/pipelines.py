# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

### -------- Import all of the necessary files -------- ###
import psycopg2

class SenateVotesPipeline(object):
    # Open database connection for uploading packets
    def open_spider(self, spider):
        hostname = 'localhost'
        username = 'postgres'
        password = 'postgres'
        database = 'politics'
        self.conn = psycopg2.connect(
            host = hostname,
            user = username,
            password = password,
            dbname = database)
        self.cur = self.conn.cursor()

    # Closes database for uploading packets
    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()

    def process_item(self, item, spider):
        vote_packet = (item['bill_num'], 
                       item['amendment_num'], 
                       item['pol_id'], 
                       item['vote_cast'], 
                       item['vote_date'], 
                       item['house'],
                       item['state'])
        insert_query = """insert into all_votes 
            (bill_num, amdt_num, pol_id, vote_cast, vote_date, house, state)
            values ({0}, {1}, {2}, {3}, {4}, {5}, {6})""".format(vote_packet)
        self.cur.execute(insert_query)
        self.conn.commit()
        return item
