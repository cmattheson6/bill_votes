# -*- coding: utf-8 -*-

### -------- Import all of the necessary files -------- ###
import scrapy
from datetime import datetime, timedelta
from datetime import date
import re
import unidecode

# Set date for yesterday's bills that are published
date_yesterday = date.today() - timedelta(days=1)
# Sets the year of yesterday's bills
this_year = date_yesterday.year

### -------- All reusable formulas are listed here -------- ###  

def clean_bill(x):
    bill = x.replace(" ", "").replace(".", "").upper()
    return bill;

### -------- Start of spider -------- ###    

class HouseVotesSpider(scrapy.Spider):
    name = 'house_votes'
    
    def start_requests(self):
        start_urls = ['http://clerk.house.gov/evs/{0}/index.asp'.format(this_year)]
        
        # Start the parsing request
        for u in start_urls:
            yield scrapy.Request(url = u, callback = self.parse_roll_calls)
    def parse_roll_calls(self, response):
        # Pull all links to the votes from this page
        all_roll_urls = response.xpath('.//a/@href').re('^.*ROLL.*$')
        # Pass all links to the next parsing request
        for u in all_roll_urls:
            url = response.urljoin(u)
            yield scrapy.Request(url = url, callback = self.parse_all_bills);
    def parse_all_bills(self, response): # THIS MAY NO LONGER REDIRECT TO XMLS BUT TO VOTE PAGE; EXTRA STEP NEEDED
        # Go to the XML link on this page
        roll_call_votes = response.xpath(".//table/tr")
        roll_call_votes = roll_call_votes[1:len(roll_call_votes)]
        bill_vote_dicts = []
        for i in roll_call_votes:
            vote_url = i.xpath(".//a/@href").re_first("^.*rollnumber.*$")
            print(vote_url)
            vote_url = response.urljoin(vote_url)
            bill_url = i.xpath(".//a/@href").re_first("^.*congress.gov.*$");
            bill_url = response.urljoin(bill_url) + "/amendments?pageSort=asc"
            bill_date = i.xpath(".//text()").extract()[2]
            bill_date = bill_date + ", " + str(date.today().year)
            bill_date = datetime.strptime(bill_date, "%d-%b, %Y").date()
            if bill_date >= date_yesterday:
                request = scrapy.Request(url = vote_url, 
                                 callback = self.parse_votes)
                request.meta['bill_url'] = bill_url
                yield request
            else: #CHANGE THIS TO 'BREAK' WHEN ALL OF THIS YEAR HAS BEEN PROPERLY UPLOADED
                request = scrapy.Request(url = vote_url, 
                                 callback = self.parse_votes)
                request.meta['bill_url'] = bill_url
                yield request;
    def parse_amendment(self, response):
        print("We made it to the bill url.")
        amdt_num = response.meta['amdt_num']
        vote_list = response.meta['vote_list']
        all_amdts = [i.xpath(".//a/text()").extract_first() \
                     for i in response.xpath(""".//ol[@class='basic-search-results-lists expanded-view']/li[@class='expanded']
                     /span[@class='result-heading amendment-heading']""")]
        amdt_next_pg = response.xpath(".//div[@class='pagination']/a[@class='next']/@href").extract_first()
        if (amdt_num >= 100) and (amdt_next_page != None):
            amdt_next_pg = response.urljoin(amdt_next_pg)
            amdt_num = amdt_num - 100
            request = scrapy.Request(url = amdt_next_pg,
                                 callback = parse_amendment,
                                 dont_filter = True)
            request.meta['amdt_num'] = amdt_num
            request.meta['vote_list'] = vote_list
            yield request
#                                  kwargs={'amdt_num': amdt_num})
                # go to next page, subtract 100 from amdt_num, and run this again
                # need to refactor by defining a fxn so I can loop it, ... or use while
        else:
            amdt_id = all_amdts[amdt_num-1]
            amdt_id = clean_bill(amdt_id)
            print(amdt_id)
            for i in vote_list:
                i['amendment_id'] = amdt_id
            print(vote_list[0:4])
            for i in vote_list:
                yield i;
#         print(vote_list); #parse the amendment number at index amdt_num;                                 
    def parse_votes(self, response): #bill_url = 'https://www.congress.gov/bill/115th-congress/house-bill/2851/amendments?pageSort=asc'): #'https://www.congress.gov/bill/115th-congress/house-bill/2851/amendments?pageSort=asc'
        # parses out bill id from xml
        bill_url = response.meta['bill_url']
        try:
            bill_id = clean_bill(response.xpath(".//legis-num/text()").extract_first())
        except Exception:
            bill_id = clean_bill(response.xpath(".//amendment/amendment_to_document_number/text()").extract_first())
#         else:
#             raise ValueError;
        # THE CURRENT SETUP FOR FINDING THE BILL NUM WILL NOT WORK
        # FIRST, PULL THE BILL NUMBER AS IS
        # IF AMENDMENT APPEARS IN THE VOTE QUESTION, THERE IS A DIFFERENT PROCESS
        # PULL THE AMENDMENT AUTHOER (COULD BE THE CRITERIUM)
        # PARSE OUT THE NAME OF THE AUTHOR AND THE STATE REPRESENTED
        # THIS WILL THEN FIND THE PROPER AMENDMENT NUMBER
        # THEN IT WILL COME IN HERE AS THE AMENDMENT NUMBER
        if response.xpath(".//amendment-num/text()").extract_first() == None:
            amdt_num = None
        elif len(response.xpath(".//amendment-num/text()").extract()) == 1:
            amdt_num = int(response.xpath(".//amendment-num/text()").extract_first())  
        else:
            raise ValueError;
        
        amendment_id = None
        # Pull vote date and process it for proper formatting
        vote_date = response.xpath(".//action-date/text()").extract_first() # WILL NEED TO BE CHANGED
        vote_date = datetime.strptime(vote_date, "%d-%b-%Y") # WILL NEED TO BE CHANGED
        vote_date = vote_date.date()
        # See if I need any exceptions or conditions w/ test
        # Pull all info to map pol ids and map them; then upload each line item
        vote_list = []
        for i in response.xpath(".//recorded-vote"):
            # All last names parse
            pol_ln = i.xpath(".//legislator/@unaccented-name").extract_first()
            # All states parse
            pol_state = i.xpath(".//legislator/@state").extract_first()
            # All parties parse
            pol_party = i.xpath(".//legislator/@party").extract_first()
            # Pull all votes cast
            vote_cast = i.xpath(".//vote/text()").extract_first()
            # Repurpose Yea as 1, Nay as 0, anything else as None
            if vote_cast == "Aye":
                vote_cast = 1
            elif vote_cast == "No":
                vote_cast = 0
            else:
                vote_cast = None;
            # Build out an individual dict for each vote and yield that
            vote_dict = {'bill_id': bill_id,
                         'amendment_id': amendment_id,
                         'pol_ln': pol_ln,
                         'pol_party': pol_party,
                         'pol_party': pol_party,
                         'vote_cast': vote_cast,
                         'vote_date': vote_date,
                         'house': 'HR',
                         'state': 'US'}
            #send each dict to the pipeline
            vote_list.append(vote_dict);
        if amdt_num == None:
            for i in vote_list:
                yield i;
            print("There was no amendment")
            print(vote_list[0:4])
        elif amdt_num > 0:
            request = scrapy.Request(url = bill_url,
                                     callback = self.parse_amendment,
                                     dont_filter = True)
            request.meta['amdt_num'] = amdt_num
            request.meta['vote_list'] = vote_list
            print("We found an amendment!")
            yield request
        else:
            raise ValueError;

    
