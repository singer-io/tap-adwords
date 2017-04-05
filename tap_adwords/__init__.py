#!/usr/bin/env python3

import datetime
import os
import sys

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
CAMPAIGNS_FIELD_LIST = ["AdvertisingChannelSubType"
                                      "AdvertisingChannelType"
                                      "Amount"
                                      "BaseCampaignId"
                                      "BidCeiling"
                                      "BidType"
                                      "BiddingStrategyId"
                                      "BiddingStrategyName"
                                      "BiddingStrategyType"
                                      "BudgetId"
                                      "BudgetName"
                                      "BudgetReferenceCount"
                                      "BudgetStatus"
                                      "CampaignTrialType"
                                      "DeliveryMethod"
                                      "Eligible"
                                      "EndDate"
                                      "EnhancedCpcEnabled"
                                      "FrequencyCapMaxImpressions"
                                      "Id"
                                      "IsBudgetExplicitlyShared"
                                      "Labels"
                                      "Level"
                                      "Name"
                                      "PricingMode"
                                      "RejectionReasons"
                                      "ServingStatus"
                                      "Settings"
                                      "StartDate"
                                      "Status"
                                      "TargetContentNetwork"
                                      "TargetCpa"
                                      "TargetCpaMaxCpcBidCeiling"
                                      "TargetCpaMaxCpcBidFloor"
                                      "TargetGoogleSearch"
                                      "TargetPartnerSearchNetwork"
                                      "TargetRoas"
                                      "TargetRoasBidCeiling"
                                      "TargetRoasBidFloor"
                                      "TargetSearchNetwork"
                                      "TargetSpendBidCeiling"
                                      "TargetSpendSpendTarget"
                                      "TimeUnit"
                                      "TrackingUrlTemplate"
                                      "UrlCustomParameters"
                                      "VanityPharmaDisplayUrlMode"
                                      "VanityPharmaText"]

BASE_REQUEST_XML = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
  <soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">
  <soapenv:Header>
    <ns1:RequestHeader soapenv:actor=\"http://schemas.xmlsoap.org/soap/actor/next\" soapenv:mustUnderstand=\"0\" xmlns:ns1=\"https://adwords.google.com/api/adwords/cm/v201607\">
      <clientCustomerId>{customer-id}</clientCustomerId>
      <developerToken>{developer-token}</developerToken>
      <userAgent>RJMetricsAdWordsIntegration</userAgent>
      <validateOnly>false</validateOnly>
      <partialFailure>false</partialFailure>
    </ns1:RequestHeader>
  </soapenv:Header>
  <soapenv:Body>
        <get xmlns=\"https://adwords.google.com/api/adwords/cm/v201607\">
            <serviceSelector>
                {fields}
                {predicates}
                {ordering}
                <paging><startIndex>{start-index}</startIndex><numberResults>{num-results}</numberResults></paging>
            </serviceSelector>
        </get>
    </soapenv:Body>
</soapenv:Envelope>"""

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "access_token",
    "refresh_token",
    "customer_ids",
    "developer_token"
]

BASE_URL = "https://adwords.google.com/api/adwords/cm/v201607/"
ACCOUNTS_BASE_URL = "https://adwords.google.com/api/adwords/mcm/v201607/"
REFRESH_TOKEN_URL = "https://www.googleapis.com/oauth2/v4/token"
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

# def refresh_access_token():
#         refresh_token=CONFIG['refresh_token']
#         refresh_req = request(url=REFRESH_TOKEN_URL,data={'grant_type': "refresh_token", 'refresh_token': refresh_token})
#         refresh_resp = SESSION.send(refresh_req)
#         refresh_json = refresh_resp.json()
#         CONFIG['access_token'] = refresh_json['access_token']
        

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

REPORTING_REQUIRED_FIELDS = frozenset(["adGroupID", "campaignID", "account", "adID", "keywordID", "customerId", "date"])

def inclusion_decision(field):
    name = field['xmlAttributeName']
    if name in REPORTING_REQUIRED_FIELDS:
        return 'always'
    return 'available'

def do_sync(client):
    top_res = request_xsd()
    root = ET.fromstring(top_res)
    path = list(root.find(".//*[@name='ReportDefinition.ReportType']/*"))
    for p in path:
        print(p.attrib['value'])
    report_names = [p.attrib['value'] for p in path]
    print(report_names)
    #print("{}".format(path))
    # fields = report_definition_service(client, "ADGROUP_PERFORMANCE_REPORT")
    # schema = {}
    # for field in fields:
    #     schema[field['xmlAttributeName']] = {'description': field['displayFieldName'],
    #                                          'type': field['fieldType'],
    #                                          'behavior': field['fieldBehavior'],
    #                                          'inclusion': inclusion_decision(field)}

    # final_schema = {"type": "object",
    #                 "properties": schema}
    # print("{}".format(final_schema))
        
    #sync_campaigns(client)
    LOGGER.info("Sync complete")

def main():
    config, state = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(config)
    STATE.update(state)

    # Initialize the GoogleRefreshTokenClient using the credentials you received
    # in the earlier steps.
    oauth2_client = oauth2.GoogleRefreshTokenClient(
        CONFIG['oauth_client_id'], \
        CONFIG['oauth_client_secret'], \
        CONFIG['refresh_token'])

    # Initialize the AdWords client.
    print("AdWordsClient args: {} | {} | {} | {}".format(CONFIG['developer_token'], oauth2_client, CONFIG['user_agent'], CONFIG['customer_ids']))
    adwords_client = adwords.AdWordsClient(CONFIG['developer_token'], oauth2_client, user_agent=CONFIG['user_agent'], client_customer_id=CONFIG["customer_ids"])

    do_sync(adwords_client)


if __name__ == "__main__":
    main()
