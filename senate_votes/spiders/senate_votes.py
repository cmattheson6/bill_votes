# -*- coding: utf-8 -*-

### -------- Import all of the necessary files -------- ###
import scrapy
from datetime import datetime
from selenium import webdriver
import psycopg2

### -------- Start of the spider -------- ###
class SenateVotesSpider(scrapy.Spider):
    name = 'senate_votes_old'
    
    # List out all of the startings
    def start_requests(self):
        start_urls = ["https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_115_2.htm",
                      "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_115_1.htm"]
#                       "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_114_2.htm",
#                       "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_114_1.htm",
#                       "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_113_2.htm",
#                       "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_113_1.htm",
#                       "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_112_2.htm",
#                       "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_112_1.htm",
#                       "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_111_2.htm",
#                       "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_111_1.htm",
#                       "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_110_2.htm",
#                       "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_110_1.htm",
#                       "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_109_2.htm",
#                       "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_109_1.htm",
#                       "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_108_2.htm",
#                       "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_108_1.htm",
#                       "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_107_2.htm",
#                       "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_107_1.htm",
#                       "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_106_2.htm"]
#         hostname = "localhost"
#         username = "postgres"
#         password = "postgres"
#         database = "politics"
#         self.conn = psycopg2.connect(host = hostname,
#                                 user = username,
#                                 password = password,
#                                 dbname = database)
#         self.cur = self.conn.cursor()
#         select_query = "select max(vote_date) from senate_votes"
#         self.cur.execute(select_query)
#         max_date = list(self.cur)[0]
#         self.cur.close()
#         self.conn.close()
        for url in start_urls:
            yield scrapy.Request(url=url, callback=self.parse_all_bills);
    def parse_all_bills(self, response):
        ### Insert meta term from start-requests for SQL info

        # pulls a list of all bills that are currently in the database
#         hostname = "localhost"
#         username = "postgres"
#         password = "postgres"
#         database = "politics"
#         select_query = """select id from table_name"""
#         
#         conn = psycopg2.connect(
#             host = hostname,
#             user = username,
#             password = password,
#             dbname = database)
#         cur = conn.cursor()
#         
#         cur.execute(select_query)
#         all_bills = list(cur)
#         
#         cur.close()
#         conn.close()
#                 
        vote_link_list = []
        for vote in response.xpath("//table/tr"):
            bill_id = vote.xpath(".//td/a/text()").extract()[1]
            if bill_id in all_bills:
                break
            else:
                vote_link_rel = vote.xpath(".//td/a/@href").re("^.*vote=.*$")
                vote_link = response.urljoin(vote_link_rel)
                vote_link_list.append(vote_link);
        
        vote_link_rel_list = response.xpath("//body/main/div/table/tr/td/a/@href").re("^.*vote=.*$")
        vote_link_list = [response.urljoin(vote) for vote in vote_link_rel_list]
        for url in vote_link_list:
            yield scrapy.Request(url = url, callback = self.parse_bill);
    def parse_bill(self, response):
        xml_link_rel = response.xpath("//body/main/div/span/a/@href").re_first("^.*xml.*$")
        xml_link = response.urljoin(xml_link_rel)
        request1 = scrapy.Request(url = xml_link, callback = self.parse_vote)
        print(xml_link)
        yield request1;

    def parse_vote(self, response):
        document_id_raw = response.xpath("//document/document_name/text()").extract_first()
        document_id_wip = document_id_raw.replace(" ", "")
        document_id = document_id_wip.replace(".", "")
        vote_date_raw = response.xpath("//roll_call_vote/vote_date/text()").extract_first()
        vote_date_processed = datetime.strptime(vote_date_raw, "%B %d, %Y, %I:%M %p")
        vote_date = vote_date_processed.date()
        for voter in response.xpath("//roll_call_vote/members/member"):
            first_name = voter.xpath(".//first_name/text()").extract_first()
            last_name = voter.xpath(".//last_name/text()").extract_first()
            pol_party = voter.xpath(".//party/text()").extract_first()
            vote_cast = voter.xpath(".//vote_cast/text()").extract_first()
            vote_dict = {
                'document_id': document_id,
                'first_name': first_name,
                'last_name': last_name,
                'party': pol_party,
                'vote_cast': vote_cast,
                'vote_date': vote_date}
            yield vote_dict;
