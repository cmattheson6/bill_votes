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

# Import a DF of nicknames for name mapping
select_query = """SELECT nickname, full_name from nicknames"""
cur.execute(select_query)
names_df = pd.DataFrame(list(cur))
names_df.columns = ["nickname", "full_name"]

# Select the last date that there was a vote in my database
select_query = "select max(vote_date) from all_votes"
cur.execute(select_query)
last_vote_date = list(cur)[0]

# Close connections
cur.close()
conn.close()

### -------- Define all custom fxns here -------- ###

# Need a fxn to pull correct pol IDs
# Input is a dictionary of first_name, last_name, party, and state
def find_pol_id(dic):
    print(dic) ###formula checkpoint
    try:
        pol_id = pols_df[(pols_df['first_name'] == dic['first_name']) & 
                         (pols_df['last_name'] == dic['last_name']) &
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
        y = re.sub(r'\,', '', x) #remove stray commas
        z = y.strip() #remove excess whitespace
        return fix_nickname(z);
    else:
        u = unidecode.unidecode(s)
        v = re.sub(r'\".*\"', '', u) #remove nicknames
        w = re.sub(r' (Sr.|Jr.|III|IV)', '', v) #remove suffixes
        x = re.sub(r'\,', '', w) #remove stray commas
        y = x.strip() #remove excess whitespace
        return fix_nickname(y);
def scrub_lname(s):
    if len(re.findall(r'[A-Z]\.', s)) == 1:
        u = unidecode.unidecode(s)
        v = re.sub(r' [A-Z]\.', '', u) #remove middle initials
        w = re.sub(r' \".*\"', '', v) #remove nicknames
        x = re.sub(r' (Sr.|Jr.|III|IV)', '', w) #remove suffixes
        y = re.sub(r'\,', '', x) #remove stray commas
        z = y.strip() #remove excess whitespace
        return z;
    else:
        u = unidecode.unidecode(s)
        v = re.sub(r'\".*\"', '', u) #remove nicknames
        w = re.sub(r' (Sr.|Jr.|III|IV)', '', v) #remove suffixes
        x = re.sub(r'\,', '', w) #remove stray commas
        y = x.strip() #remove excess whitespace
        return y;
    

class SenateVotesSpider(scrapy.Spider):
    name = 'senate_votes'
    
    def start_requests(self):
        start_urls = ["https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_115_2.htm",
                      "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_115_1.htm"]
        start_url = ["https://www.senate.gov/legislative/LIS/roll_call_lists/roll_call_vote_cfm.cfm?congress=115&session=2&vote=00178"]
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
            bill_date = bill_date[len(bill_date)-1] + ", " + str(date.today().year)
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
            bill_num = response.xpath(".//document/document_name/text()")\
            .extract_first().replace(" ", "").replace(".", "")
        except Exception:
            bill_num = response.xpath(".//amendment/amendment_to_document_number/text()")\
            .extract_first().replace(" ", "").replace(".", "")
        else:
            return ValueError;
        # Pull vote date and process it for proper formatting
        vote_date = response.xpath(".//vote_date/text()").extract_first()
        vote_date = datetime.strptime(vote_date, "%B %d, %Y, %I:%M %p")
        vote_date = vote_date.date()
        # Parse amendment number
        try:
            amendment_num = response.xpath(".//amendment/amendment_number/text()").\
            extract_first().replace(" ", "").replace(".", "")
        except Exception:
            amendment_num = None
        else:
            return ValueError;
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
            # Clean first and last names of pol (don't need to keep any removed data)
            fname = scrub_fname(fname)
            lname = scrub_lname(lname)
            # Reorganize above four lists to be politician dictionaries (for next formula)
            pol_dict = {'first_name': fname,
                        'last_name': lname,
                        'state': state,
                        'party': party}
            # Find pol ID formula (as list comprehension)
            pol_id = find_pol_id(pol_dict)
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
                         'pol_id': pol_id,
                         'vote_cast': vote_cast,
                         'vote_date': vote_date,
                         'house': 'SN',
                         'state': 'US'}
            #send each dict to the pipeline
            yield vote_dict;
             
    
