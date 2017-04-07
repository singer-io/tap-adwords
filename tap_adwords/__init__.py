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

SDK_CLIENT = None
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

GENERIC_ENDPOINT_MAPPINGS = {"campaigns": {'primary_keys': ["id"],
                                           'service_name': 'CampaignService'},
                             "ad_groups": {'primary_keys': ["id"],
                                           'service_name': 'AdGroupService'},
                             "ads": {'primary_keys': ["id", "adGroupId"],
                                     'service_name': 'AdGroupAdService'},
                             "accounts": {'primary_keys': ["customerId"],
                                          'service_name': 'ManagedCustomerService'}}

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

def fields(annotated_schema):
    props = annotated_schema['properties'] # pylint: disable=unsubscriptable-object
    return [k for k in props if props[k].get('selected')]

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
def sync_generic_endpoint(stream_name, stream_schema):
    schema = load_schema(stream_name)
    service_name = GENERIC_ENDPOINT_MAPPINGS[stream_name]['service_name']
    primary_keys = GENERIC_ENDPOINT_MAPPINGS[stream_name]['primary_keys']
    singer.write_schema(stream_name, schema, primary_keys)

    service_caller = SDK_CLIENT.GetService(service_name, version=VERSION)

    field_list = primary_keys + fields(stream_schema)
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


def sync_stream(stream, annotated_schema):
    if stream in GENERIC_ENDPOINTS:
        sync_generic_endpoint(stream, annotated_schema)
#TODO reports sync

def do_sync(annotated_schema):
    for stream_name, stream_schema in annotated_schema['streams'].items():
        if stream_schema.get('selected'):
            sync_stream(stream_name, stream_schema)
    LOGGER.info("Sync Completed")

def report_definition_service(report_type):
    report_definition_service = SDK_CLIENT.GetService(
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

def do_discover_reports():
    url = 'https://adwords.google.com/api/adwords/reportdownload/v201702/reportDefinition.xsd'
    top_res = request_xsd(url)
    root = ET.fromstring(top_res)
    path = list(root.find(".//*[@name='ReportDefinition.ReportType']/*"))
    streams = [p.attrib['value'] for p in path]
    streams.remove("UNKNOWN")
    schema = {}
    for stream in streams:
        report_properties = {}
        LOGGER.info('Loading schema for %s', stream)
        fields = report_definition_service(stream)
        for field in fields:
            report_properties[field['xmlAttributeName']] = {'description': field['displayFieldName'],
                                                                'behavior': field['fieldBehavior'],
                                                                'field': field['fieldName'],
                                                                'inclusion': inclusion_decision(field)}
            report_properties[field['xmlAttributeName']].update(create_type_map(field['fieldType']))

        schema[stream] = {"type": "object",
                          "properties": report_properties}

    LOGGER.info("Report discovery complete")
    return schema

def do_discover_generic_endpoints():
    schema = {}
    for stream_name in GENERIC_ENDPOINTS:
        stream_schema = load_schema(stream_name)
        schema.update({stream_name: stream_schema})

    return schema

def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    CONFIG.update(args.config)
    STATE.update(args.state)

    oauth2_client = oauth2.GoogleRefreshTokenClient(
        CONFIG['oauth_client_id'], \
        CONFIG['oauth_client_secret'], \
        CONFIG['refresh_token'])
    global SDK_CLIENT
    SDK_CLIENT = adwords.AdWordsClient(CONFIG['developer_token'], \
                                           oauth2_client, user_agent=CONFIG['user_agent'], \
                                           client_customer_id=CONFIG["customer_ids"])

    if args.discover:
        generic_schema = do_discover_generic_endpoints()
        report_schema = do_discover_reports()
        schema = {}
        schema.update(generic_schema)
        schema.update(report_schema)
        json.dump({"streams": schema}, sys.stdout, indent=4)
    elif args.properties:
        do_sync(args.properties)
    else:
        LOGGER.info("No properties were selected")

if __name__ == "__main__":
    main()
