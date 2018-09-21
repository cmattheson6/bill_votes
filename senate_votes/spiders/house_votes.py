# -*- coding: utf-8 -*-

### -------- Import all of the necessary files -------- ###
import scrapy
from datetime import datetime
from datetime import date
from selenium import webdriver
import psycopg2
import pandas as pd
import re
import unidecode

### -------- Pull all needed tables from database -------- ###
# Connect to the database

hostname = "localhost"
username = "postgres"
password = "postgres"
database = "politics"

conn = psycopg2.connect(host = hostname,
                        user = username,
                        password = password,
                        dbname = database)
cur = conn.cursor()

# Import a DF of politician info for mapping
select_query = """SELECT id, first_name, last_name, party, state from politicians
                  ORDER BY id ASC"""
cur.execute(select_query)
pols_df = pd.DataFrame(list(cur))
pols_df.columns = ['id', 'first_name', 'last_name', 'party', 'state']
# THIS MAY NEED TO CHANGE DUE TO NOT HAVING FIRST NAMES

# Import a DF of nicknames for name mapping
select_query = """SELECT nickname, full_name from nicknames"""
cur.execute(select_query)
names_df = pd.DataFrame(list(cur))
names_df.columns = ["nickname", "full_name"]

# Select the last date that there was a vote in my database
select_query = "select max(vote_date) from all_votes"
cur.execute(select_query)
last_vote_date = list(cur)[0]

# Get all states for mapping states to authors
select_query = """SELECT * from state_map"""
cur.execute(select_query)
states_df = pd.DataFrame(list(cur))
states_df.columns = ["state", "initials"]

# Close connections
cur.close()
conn.close()

### -------- Define all custom fxns here -------- ###

# Need a fxn to pull correct pol IDs
# Input is a dictionary of first_name, last_name, party, and state
def find_pol_id(dic): # THIS FORMULA MAY NEED TO CHANGE DUE TO NO FIRST NAME
    try:
        pol_id = pols_df[(pols_df['last_name'] == dic['last_name']) &
                         (pols_df['party'] == dic['party']) &
                         (pols_df['state'] == dic['state'])].iloc[0,0]
    except Exception: 
        pol_id = ValueError
    return pol_id;

# Need a fxn to clean names
# Normalize policicians' nicknames for database
def fix_nickname(fn):
    if fn in list(names_df['nickname']):
        full_name = names_df[names_df['nickname'] == fn].iloc[0,1]
        return full_name
    else:
        return fn;

# Normalize a politicians' full name to allow for normalized mapping
def scrub_fname(s):
    if len(re.findall(r'[A-Z]\.', s)) == 1:
        u = unidecode.unidecode(s)
        v = re.sub(r' [A-Z]\.', '', u) #remove middle initials
        w = re.sub(r' \".*\"', '', v) #remove nicknames
        x = re.sub(r' (Sr.|Jr.|III|IV)', '', w) #remove suffixes
        y = re.sub(r' \(.*\)', '', x) #remove any parentheses
        z = re.sub(r'\,', '', y) #remove stray commas
        a = z.strip() #remove excess whitespace
        return fix_nickname(a);
    else:
        u = unidecode.unidecode(s)
        v = re.sub(r'\".*\"', '', u) #remove nicknames
        w = re.sub(r' (Sr.|Jr.|III|IV)', '', v) #remove suffixes
        x = re.sub(r' \(.*\)', '', w) #removes any parentheses
        y = re.sub(r'\,', '', x) #remove stray commas
        z = y.strip() #remove excess whitespace
        return fix_nickname(z);
def scrub_lname(s):
    if len(re.findall(r'[A-Z]\.', s)) == 1:
        u = unidecode.unidecode(s)
        v = re.sub(r' [A-Z]\.', '', u) #remove middle initials
        w = re.sub(r' \".*\"', '', v) #remove nicknames
        x = re.sub(r' (Sr.|Jr.|III|IV)', '', w) #remove suffixes
        y = re.sub(r' \(.*\)', '', x) #removes any parentheses
        z = re.sub(r',.*', '', y) #remove anything after a comma
        a = re.sub(r'\,', '', z) #remove stray commas
        b = a.strip() #remove excess whitespace
        return b;
    else:
        u = unidecode.unidecode(s)
        v = re.sub(r'\".*\"', '', u) #remove nicknames
        w = re.sub(r' (Sr.|Jr.|III|IV)', '', v) #remove suffixes
        x = re.sub(r' \(.*\)', '', w) #removes any parentheses
        y = re.sub(r',.*', '', x) #remove anything after a comma
        z = re.sub(r'\,', '', y) #remove stray commas
        a = z.strip() #remove excess whitespace
        return a;
# This is to clean bill names to a normalized state
def clean_bill(x):
    bill = x.replace(" ", "").replace(".", "").upper()
    return bill;
    

class HouseVotesSpider(scrapy.Spider):
    name = 'house_votes'
    
    def start_requests(self):
        start_urls = ["http://clerk.house.gov/evs/2018/index.asp"]
        
        # Start the parsing request
        for u in start_urls:
            yield scrapy.Request(url = u, callback = self.parse_roll_calls)
    def parse_roll_calls(self, response):
        # Pull all links to the votes from this page
        all_roll_urls = response.xpath(".//a/@href").re("^.*ROLL.*$")
        # Pass all links to the next parsing request
        for u in all_roll_urls:
            url = response.urljoin(u)
            yield scrapy.Request(url = url, callback = self.parse_all_bills);
    def parse_all_bills(self, response): # THIS MAY NO LONGER REDIRECT TO XMLS BUT TO VOTE PAGE; EXTRA STEP NEEDED
        # Go to the XML link on this page
        roll_call_votes = response.xpath(".//table/tr")#[0:10] #remove this limiter
        roll_call_votes = roll_call_votes[1:len(roll_call_votes)]
        bill_vote_dicts = []
        date_today = date.today()
        for i in roll_call_votes:
            vote_url = i.xpath(".//a/@href").re_first("^.*rollnumber.*$")
            print(vote_url)
            vote_url = response.urljoin(vote_url)
            bill_url = i.xpath(".//a/@href").re_first("^.*congress.gov.*$");
            bill_url = response.urljoin(bill_url) + "/amendments?pageSort=asc"
            bill_date = i.xpath(".//text()").extract()[2]
            bill_date = bill_date + ", " + str(date.today().year)
            bill_date = datetime.strptime(bill_date, "%d-%b, %Y").date()
            if bill_date == date_today:
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
            yield vote_list
#         print(vote_list); #parse the amendment number at index amdt_num;                                 
    def parse_votes(self, response): #bill_url = 'https://www.congress.gov/bill/115th-congress/house-bill/2851/amendments?pageSort=asc'): #'https://www.congress.gov/bill/115th-congress/house-bill/2851/amendments?pageSort=asc'
        # parses out bill id from xml
        bill_url = response.meta['bill_url']
        try:
            bill_id = clean_bill(response.xpath(".//legis-num/text()").extract_first())
        except Exception:
            bill_id = clean_bill(response.xpath(".//amendment/amendment_to_document_number/text()").extract_first())
        else:
            ValueError;
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
            return ValueError;
        
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
            lname = i.xpath(".//legislator/@unaccented-name").extract_first()
            # All states parse
            state = i.xpath(".//legislator/@state").extract_first()
            # All parties parse
            party = i.xpath(".//legislator/@party").extract_first()
            # Clean first and last names of pol (don't need to keep any removed data)
            lname = scrub_lname(lname)
            # Reorganize above four lists to be politician dictionaries (for next formula)
            pol_dict = {'last_name': lname,
                        'state': state,
                        'party': party}
            # Find pol ID formula (as list comprehension)
            pol_id = find_pol_id(pol_dict) # NEED TO FIX THIS FOR FXN TO WORK
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
                         'pol_id': pol_id,
                         'vote_cast': vote_cast,
                         'vote_date': vote_date,
                         'house': 'HR',
                         'state': 'US'}
            #send each dict to the pipeline
            vote_list.append(vote_dict);
        if amdt_num == None:
            yield vote_list
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
            return ValueError;

    
