# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ListOfVotes(scrapy.item.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    bill_num = scrapy.Field()
    amendment_num = scrapy.Field()
    pol_fn = scrapy.Field()
    pol_ln = scrapy.Field()
    pol_party = scrapy.Field()
    pol_state = scrapy.Field()
    vote_cast = scrapy.Field()
    vote_date = scrapy.Field()
    house = scrapy.Field()
    state = scrapy.Field()
