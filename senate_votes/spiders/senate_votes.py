# -*- coding: utf-8 -*-

### -------- Import all of the necessary files -------- ###
import scrapy
from datetime import datetime
from datetime import date
import re
import unidecode

### -------- Define all custom fxns here -------- ###

def clean_bill(x):
    bill = x.replace(" ", "").replace(".", "").upper()
    return bill;

### -------- Start of spider -------- ###

class SenateVotesSpider(scrapy.Spider):
    name = 'senate_votes'
    
    def start_requests(self):
        start_urls = ["https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_115_2.htm",
                      "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_115_1.htm"]
        # Start the parsing request
        for u in start_urls:
            yield scrapy.Request(url = u, callback = self.parse_all_bills) #replace with each parse to test
    def parse_all_bills(self, response):
        # Pull all links to the votes from this page
        all_bill_urls = [] # empty list to hold all of the URLs
        for i in response.xpath(".//table/tr"):
            # Pull bill URL
            bill_url = i.xpath(".//td/a/@href").re_first("^.*vote=.*$")
            # Check the date of the URL
            bill_date = i.xpath(".//td/text()").extract()
            bill_date = bill_date[len(bill_date)-1]
            bill_Date = bill_date.replace("\xa[0-9]*", "")
            bill_date = bill_date + ", " + str(date.today().year)
            bill_date = datetime.strptime(bill_date, "%b %d, %Y")
            bill_date = bill_date.date()
            date_today = date.today()
            all_bill_urls.append(bill_url) #WILL EVENTUALLY BE ELIMINATED
            # If the date is not today, do not add it
#             if bill_date == date_today:
#                 all_bill_urls.append(bill_url)
#             else:
#                 pass;
        # Pass all links to the next parsing request
        for u in all_bill_urls:
            url = response.urljoin(u)
            print(url)
            yield scrapy.Request(url = url, callback = self.parse_bill);
    def parse_bill(self, response):
        # Go to the XML link on this page
        bill_xml = response.xpath(".//span[@style='float: right;']/a/@href").extract_first()
        bill_xml = response.urljoin(bill_xml)
        # Need to join the url here correctly
        # Pass the link to the nex parsing request
        yield scrapy.Request(url = bill_xml, callback = self.parse_votes);
    def parse_votes(self, response):
        # Parse out all of the necessary information from the vote
        # Bill num parse
        try:
            bill_num = clean_bill(response.xpath(".//document/document_name/text()").extract_first())
        except Exception:
            bill_num = clean_bill(response.xpath(".//amendment/amendment_to_document_number/text()").extract_first())
        else:
            raise ValueError;
        # Pull vote date and process it for proper formatting
        vote_date = response.xpath(".//vote_date/text()").extract_first()
        vote_date = datetime.strptime(vote_date, "%B %d, %Y, %I:%M %p")
        vote_date = vote_date.date()
        # Parse amendment number
        try:
            amendment_num = clean_bill(response.xpath(".//amendment/amendment_number/text()").extract_first())
        except Exception:
            amendment_num = None
        else:
            raise ValueError;
        if amendment_num == "":
            amendment_num = None;
        # See if I need any exceptions or conditions w/ test
        for i in response.xpath(".//member"):
            # All first names parse
            fname = i.xpath(".//first_name/text()").extract_first()
            # All last names parse
            lname = i.xpath(".//last_name/text()").extract_first()
            # All states parse
            state = i.xpath(".//state/text()").extract_first()
            # All parties parse
            party = i.xpath(".//party/text()").extract_first()
            # Pull all votes cast
            vote_cast = i.xpath(".//vote_cast/text()").extract_first()
            # Repurpose Yea as 1, Nay as 0, anything else as None
            if vote_cast == "Yea":
                vote_cast = 1
            elif vote_cast == "Nay":
                vote_cast = 0
            else:
                vote_cast = None;
            # Build out an individual dict for each vote and yield that
            vote_dict = {'bill_num': bill_num,
                         'amendment_num': amendment_num,
                         'pol_fn': fname,
                         'pol_ln': lname,
                         'pol_party': party,
                         'pol_state': state,
                         'vote_cast': vote_cast,
                         'vote_date': vote_date,
                         'house': 'SN',
                         'state': 'US'}
            #send each dict to the pipeline
            yield vote_dict;
             
    
