# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

### -------- Import all of the necessary files -------- ###
import scrapy
from google.cloud import pubsub
from google.oauth2 import service_account
import subprocess
import scrapy
import scrapy.crawler
from scrapy.utils.project import get_project_settings
import google.auth

### -------- Start of pipeline -------- ###

class SenateVotesPipeline(object):

    def process_item(self, item, spider):
        
        cred_dict = {
                         "auth_provider_x509_cert_url": spider.settings.get('auth_provider_x509_cert_url'),
                         "auth_uri": spider.settings.get('auth_uri'),
                         "client_email": spider.settings.get('client_email'),
                         "client_id": spider.settings.get('client_id'),
                         "client_x509_cert_url": spider.settings.get('client_x509_cert_url'),
                         "private_key": spider.settings.get('private_key'),
                         "private_key_id": spider.settings.get('private_key_id'),
                         "project_id": spider.settings.get('project_id'),
                         "token_uri": spider.settings.get('token_uri'),
                         "type": spider.settings.get('account_type')
             }
        cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
        print(cred_dict)

        credentials = service_account.Credentials.from_service_account_info(cred_dict)
        print(credentials)
        print("I haven't set up the client yet, but I built the credentials!")
        publisher = pubsub.PublisherClient(credentials = credentials)
        print(publisher)
        print("The client was set up!")

        topic = 'projects/{project_id}/topics/{topic}'.format(
             project_id='politics-data-tracker-1',
             topic='bill_votes')
        project_id = 'politics-data-tracker-1'
        topic_name = 'bill_votes'
        topic_path = publisher.topic_path(project_id, topic_name)
        data = u'This is a representative in the House.'
        data = data.encode('utf-8')
        print("The topic was built!")
        publisher.publish(topic_path, data=data,
                          bill_id = str(item['bill_id']),
                          amdt_id = str(item['amdt_id']),
                          sponsor_ln = str(item['pol_ln']),
                          sponsor_state = str(item['pol_state']),
                          sponsor_party = str(item['pol_party']),
                          vote_cast = str(item['vote_cast']),
                          vote_date = str(item['vote_date']),
                          house = str(item['house']),
                          state = str(item['state']))
        
        print("We published! WOOOO!")
        
        return item;
