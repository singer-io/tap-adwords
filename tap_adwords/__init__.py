#!/usr/bin/env python3

import datetime
import os
import sys

import requests
import singer

from singer import utils


LOGGER = singer.get_logger()
SESSION = requests.Session()

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
def request(url, params=None, body=None):
    params = params or {}
    headers = {"Accept": "application/json",
               "Authorization": "Bearer " + CONFIG["access_token"],
               "Content-Type": "application/soap+xml"}
    req = requests.Request("POST", url=url, params=params, headers=headers, data=body).prepare()
    LOGGER.info("POST {}".format(req.url))
    resp = SESSION.send(req)

    if resp.status_code >= 400:
        LOGGER.error("POST {} [{} - {}]".format(req.url, resp.status_code, resp.content))
        sys.exit(1)

    return resp.json()
 #TODO split on commas
def sync_campaigns():
    stream_name = 'campaigns'
    request_body = BASE_REQUEST_XML.replace('{fields}', '<fields>TimeUnit</fields>') \
                   .replace('{predicates}', '') \
                   .replace('{ordering}', '') \
                   .replace('{start-index}', '0') \
                   .replace('{num-results}', '1') \
                   .replace('{customer-id}', CONFIG['customer_ids']) \
                   .replace('{developer-token}', CONFIG['developer_token'])
                             
    
    
    
    request(BASE_URL + 'CampaignService', body=request_body)
    

def do_sync():
    sync_campaigns()
    LOGGER.info("Sync complete")

def main():
    config, state = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(config)
    STATE.update(state)
    do_sync()


if __name__ == "__main__":
    main()
