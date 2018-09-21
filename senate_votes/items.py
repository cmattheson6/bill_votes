# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ListOfVotes(scrapy.item.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    first_name = scrapy.Field()
    last_name = scrapy.Field()
    party = scrapy.Field()
    vote_cast = scrapy.Field()

