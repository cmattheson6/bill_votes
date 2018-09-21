# -*- coding: utf-8 -*-
import scrapy
from datetime import datetime
from selenium import webdriver

class VoteVotesSpider(scrapy.Spider):
    name = 'vote_votes'
    
    def start_requests(self):
        ### Generate response.xpath variable here that will cascade all of the URL links down; they can be outside of the parse variable (I think)
        start_urls = ['https://www.senate.gov/legislative/LIS/roll_call_votes/vote1152/vote_115_2_00082.xml']
        for url in start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        document_id = response.xpath("//document/document_name/text()").extract_first()
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
            yield vote_dict
