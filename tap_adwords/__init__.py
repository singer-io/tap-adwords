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
GENERIC_ENDPOINTS = ['campaigns', 'accounts', 'ad_groups', 'ads']
REPORT_TYPE_MAPPINGS = {"Boolean" : {"type": "boolean"},
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

#TODO find all xsd types
XSD_TYPE_MAPPINGS = {"xsd:long" : {"type" : "integer"},
                     "xsd:boolean" : {"type" : "boolean"}}
CAMPAIGNS_PKS = frozenset(['id'])
ADGROUPS_PKS = frozenset(['id'])
ADGROUP_ADS_PKS = frozenset(['id', 'adGroupId'])
ACCOUNTS_PKS = frozenset(['customerId'])

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "access_token",
    "refresh_token",
    "customer_ids",
    "developer_token"
]
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
def request_xsd(url):
    req = requests.Request("GET", url=url).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)

    return resp.text

def suds_to_dict(obj):
    if not hasattr(obj, '__keylist__'):
        return obj
    data = {}
    fields = obj.__keylist__
    for field in fields:
        val = getattr(obj, field)
        if isinstance(val, list):
            data[field] = []
            for item in val:
                data[field].append(suds_to_dict(item))
        else:
            data[field] = suds_to_dict(val)
    return data

#TODO split on commas
def sync_generic_endpoints(client, stream_name, service_name, field_list):
    service_caller = client.GetService(service_name, version=VERSION)

    field_list = [f[0].upper()+f[1:] for f in field_list]
    offset = 0
    selector = {
        'fields': field_list,
        'paging': {
            'startIndex': str(offset),
            'numberResults': str(PAGE_SIZE)
        },
    }

    more_pages = True
    while more_pages:
        page = service_caller.get(selector)

        # Display results.
        if 'entries' in page:
            for entry in page['entries']:

                singer.write_record(stream_name, suds_to_dict(entry))
            else:

                offset += PAGE_SIZE
                selector['paging']['startIndex'] = str(offset)
                more_pages = offset < int(page['totalNumEntries'])

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
    if REPORT_TYPE_MAPPINGS.get(typ):
        return REPORT_TYPE_MAPPINGS.get(typ)
    return {'type' : 'string'}

def do_discover_reports(client, schema):
    url = 'https://adwords.google.com/api/adwords/reportdownload/v201702/reportDefinition.xsd'
    top_res = request_xsd(url)
    root = ET.fromstring(top_res)
    path = list(root.find(".//*[@name='ReportDefinition.ReportType']/*"))
    streams = [p.attrib['value'] for p in path]
    streams.remove("UNKNOWN")
    result = {'streams': {}}
    for stream in streams:
        LOGGER.info('Loading schmea for %s', stream)
        fields = report_definition_service(client, stream)
        for field in fields:
            schema[field['xmlAttributeName']] = {'description': field['displayFieldName'],
                                                 'behavior': field['fieldBehavior'],
                                                 'field': field['fieldName'],
                                                 'inclusion': inclusion_decision(field)}
            schema[field['xmlAttributeName']].update(create_type_map(field['fieldType']))

        final_schema = {"type": "object",
                        "properties": schema}
        result['streams'][stream] = final_schema

    LOGGER.info("Report discovery complete")
    return result

def xsd_inclusion_decision(field, checking_set):
    name = field.attrib['name']
    if name in checking_set:
        return 'always'
    return 'available'

def create_xsd_type_map(typ):
    if XSD_TYPE_MAPPINGS.get(typ):
        return XSD_TYPE_MAPPINGS.get(typ)
    return {"type": "string"}

def load_schema(stream):
    path = get_abs_path('schemas/{}.json'.format(stream))
    return utils.load_json(path)

def do_discover_generic_endpoints(schema, stream_name):
    stream_schema = load_schema(stream_name)
    schema['streams'].update({stream_name: stream_schema})
    return schema

def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    CONFIG.update(args.config)
    STATE.update(args.state)

    oauth2_client = oauth2.GoogleRefreshTokenClient(
        CONFIG['oauth_client_id'], \
        CONFIG['oauth_client_secret'], \
        CONFIG['refresh_token'])

    adwords_client = adwords.AdWordsClient(CONFIG['developer_token'], \
                                           oauth2_client, user_agent=CONFIG['user_agent'], \
                                           client_customer_id=CONFIG["customer_ids"])

    schema = {'streams': {}}
    if args.discover:
        for endpoint in GENERIC_ENDPOINTS:
            do_discover_generic_endpoints(schema,endpoint)
        schema = do_discover_reports(adwords_client, schema)
        json.dump(schema, sys.stdout, indent=4)
    else:
        #sync_generic_endpoints(adwords_client,'campaigns', 'CampaignService', CAMPAIGNS_FULL_FIELD_LIST)
        #sync_generic_endpoints(adwords_client,'ad_groups', 'AdGroupService', ADGROUP_FULL_FIELD_LIST)
        #sync_generic_endpoints(adwords_client,'ads', 'AdGroupAdService', ADGROUPAD_FULL_FIELD_LIST)
        sync_generic_endpoints(adwords_client,'accounts', 'ManagedCustomerService', ACCOUNTS_FULL_FIELD_LIST)

if __name__ == "__main__":
    main()
