#!/usr/bin/env python3

import datetime
import os
import sys

import json
import xml.etree.ElementTree as ET
from suds.client import Client
import time
import googleads
from googleads import adwords
from googleads import oauth2

import requests
import singer

from singer import utils


LOGGER = singer.get_logger()
SESSION = requests.Session()
PAGE_SIZE = 100
VERSION = 'v201702'
REPORTING_REQUIRED_FIELDS = frozenset(["adGroupID", "campaignID", "account", "adID", "keywordID", "customerId", "date"])
TYPE_MAPPINGS = {"Boolean" : {"type": "boolean"},
                 "boolean" : {'type': "boolean"},
                 "Date" : {"type": "string",
                           "format": "date-time"},
                 "DateTime" : {"type": "string",
                               "format": "date-time"},
                 "Double" : {"type": "number"},
                 "int" : {"type": "integer"},
                 "Integer": {"type": "integer"},
                 "long": {"type": "integer"},
                 "Long": {"type": "integer"}}


REQUIRED_CONFIG_KEYS = [
    "start_date",
    "access_token",
    "refresh_token",
    "customer_ids",
    "developer_token"
]

# BASE_URL = "https://adwords.google.com/api/adwords/cm/v201607/"
# ACCOUNTS_BASE_URL = "https://adwords.google.com/api/adwords/mcm/v201607/"
# REFRESH_TOKEN_URL = "https://www.googleapis.com/oauth2/v4/token"
CONFIG = {}
STATE = {}

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))

def get_start(key):
    if key not in STATE:
        STATE[key] = CONFIG['start_date']

    return STATE[key]


def get_url(endpoint):
    return BASE_URL.format(CONFIG['account_name']) + endpoint

@utils.ratelimit(100, 15) # TODO
def request_xsd():
    url = 'https://adwords.google.com/api/adwords/reportdownload/v201702/reportDefinition.xsd'
    req = requests.Request("GET", url=url).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)

    return resp.text

#TODO split on commas
def sync_campaigns(client):
    stream_name = 'campaigns'
    campaign_service = client.GetService('CampaignService', version=VERSION)

    offset = 0
    selector = {
        'fields': ['Id', 'Name'],
        'ordering': {
            'field': 'Name',
            'sortOrder': 'ASCENDING'
        },
        'paging': {
            'startIndex': str(offset),
            'numberResults': str(PAGE_SIZE)
        },
        # 'predicates': {
        #     'field': 'Labels',
        #     'operator': 'CONTAINS_ANY',
        #     'values': [label_id]
        # }
    }

    more_pages = True
    while more_pages:
        page = campaign_service.get(selector)

        # Display results.
        if 'entries' in page:
            for campaign in page['entries']:
                print("{}".format(Client.dict(campaign)))


                singer.write_record(stream_name, Client.dict(campaign))
                print ('Campaign found with Id \'%s\', name \'%s\', and labels: %s'
                       % (campaign['id'], campaign['name'], campaign['labels']))
            else:
                print ('No campaigns were found.')

                offset += PAGE_SIZE
                selector['paging']['startIndex'] = str(offset)
                more_pages = offset < int(page['totalNumEntries'])
                time.sleep(1)

def report_definition_service(client, report_type):
    report_definition_service = client.GetService(
        'ReportDefinitionService', version=VERSION)
    fields = report_definition_service.getReportFields(report_type)
    return fields

def inclusion_decision(field):
    name = field['xmlAttributeName']
    if name in REPORTING_REQUIRED_FIELDS:
        return 'always'
    return 'available'

def create_type_map(typ):
    if TYPE_MAPPINGS.get(typ):
        return TYPE_MAPPINGS.get(typ)
    return {'type' : 'string'}

def do_discover(client):
    top_res = request_xsd()
    root = ET.fromstring(top_res)
    path = list(root.find(".//*[@name='ReportDefinition.ReportType']/*"))
    for p in path:
        print(p.attrib['value'])
    streams = [p.attrib['value'] for p in path]
    streams.remove("UNKNOWN")
    result = {'streams': {}}
    for stream in streams:
        LOGGER.info('Loading schmea for %s', stream)
        fields = report_definition_service(client, stream)
        schema = {}
        for field in fields:
            schema[field['xmlAttributeName']] = {'description': field['displayFieldName'],
                                                 'behavior': field['fieldBehavior'],
                                                 'inclusion': inclusion_decision(field)}
            schema[field['xmlAttributeName']].update(create_type_map(field['fieldType']))
        
        final_schema = {"type": "object",
                        "properties": schema}
        result['streams'][stream] = final_schema

    json.dump(result, sys.stdout, indent=4)

    LOGGER.info("Discover complete")

def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    CONFIG.update(args.config)
    STATE.update(args.state)

    # Initialize the GoogleRefreshTokenClient using the credentials you received
    # in the earlier steps.
    oauth2_client = oauth2.GoogleRefreshTokenClient(
        CONFIG['oauth_client_id'], \
        CONFIG['oauth_client_secret'], \
        CONFIG['refresh_token'])

    # Initialize the AdWords client.
    print("AdWordsClient args: {} | {} | {} | {}".format(CONFIG['developer_token'], oauth2_client, CONFIG['user_agent'], CONFIG['customer_ids']))
    adwords_client = adwords.AdWordsClient(CONFIG['developer_token'], oauth2_client, user_agent=CONFIG['user_agent'], client_customer_id=CONFIG["customer_ids"])

    if args.discover:
        do_discover(adwords_client)



if __name__ == "__main__":
    main()
