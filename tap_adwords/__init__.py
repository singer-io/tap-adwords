#!/usr/bin/env python3

import datetime
import os
import sys
import io
import csv

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
from singer import transform

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
    "oauth_client_id",
    "oauth_client_secret",
    "user_agent",
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
    return [k for k in props if props[k].get('selected') or props[k].get('inclusion') == 'always']

@utils.ratelimit(100, 15) # TODO
def request_xsd(url):
    req = requests.Request("GET", url=url).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)

    return resp.text

def sync_report(stream_name, annotated_stream_schema, sdk_client):
    stream_schema = create_schema_for_report(stream_name, sdk_client)
    report_downloader = sdk_client.GetReportDownloader(version=VERSION)
    primary_keys = 'FIXME'
    xml_attribute_list  = fields(stream_schema)
    real_field_list = []

#    singer.write_schema(stream_name, stream_schema, [])
    print("xml list is {}".format(xml_attribute_list))
    
    for field in xml_attribute_list:
        if field != 'customer_id':
            real_field_list.append(stream_schema['properties'][field]['field'])

    print("field list is {}".format(real_field_list))
        
    # Create report definition.
    #TODO add dateRange
    report = {
        'reportName': 'Seems this is required',
        'dateRangeType': 'CUSTOM_DATE',
        'reportType': stream_name,
        'downloadFormat': 'CSV',
        'selector': {
            'fields': real_field_list,
            'dateRange': {'min': '20170101',
                          'max': '20170102'}}}

    # Print out the report as a string
    # Do not get data with 0 impressions, some reporst don't support that
    result = report_downloader.DownloadReportAsString(
        report, skip_report_header=True, skip_column_header=False,
        skip_report_summary=True, include_zero_impressions=True)

    string_buffer = io.StringIO(result)
    reader = csv.reader(string_buffer)
    rows = []
    for row in reader:
        rows.append(row)

    headers = rows[0]
    values = rows[1:]

    description_to_xml_attribute = {}
    for k,v in stream_schema['properties'].items():
        description_to_xml_attribute[v['description']] = k

    xml_attribute_headers = [description_to_xml_attribute[header] for header in headers]
    for val in values:
        obj = dict(zip(xml_attribute_headers, val))
        obj['customer_id'] = sdk_client.client_customer_id
        singer.write_record(stream_name, transform.transform(obj, stream_schema))


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
def sync_generic_endpoint(stream_name, stream_schema, sdk_client):
    schema = load_schema(stream_name)
    service_name = GENERIC_ENDPOINT_MAPPINGS[stream_name]['service_name']
    primary_keys = GENERIC_ENDPOINT_MAPPINGS[stream_name]['primary_keys']
    singer.write_schema(stream_name, schema, primary_keys)

    LOGGER.info("Syncing %s", stream_name)
    service_caller = sdk_client.GetService(service_name, version=VERSION)

    field_list = fields(stream_schema)
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

                singer.write_record(stream_name, transform.transform(suds_to_dict(entry), schema))
            else:

                offset += PAGE_SIZE
                selector['paging']['startIndex'] = str(offset)
                more_pages = offset < int(page['totalNumEntries'])


def sync_stream(stream, annotated_schema, sdk_client):
    if stream in GENERIC_ENDPOINTS:
        sync_generic_endpoint(stream, annotated_schema, sdk_client)
    else:
        sync_report(stream, annotated_schema, sdk_client)

def do_sync(annotated_schema, sdk_client):
    for stream_name, stream_schema in annotated_schema['streams'].items():
        if stream_schema.get('selected'):
            sync_stream(stream_name, stream_schema, sdk_client)

def report_definition_service(report_type, sdk_client):
    report_definition_service = sdk_client.GetService(
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

def create_schema_for_report(stream, sdk_client):
    report_properties = {}
    LOGGER.info('Loading schema for %s', stream)
    fields = report_definition_service(stream, sdk_client)
    for field in fields:
        report_properties[field['xmlAttributeName']] = {'description': field['displayFieldName'],
                                                        'behavior': field['fieldBehavior'],
                                                        'field': field['fieldName'],
                                                        'inclusion': inclusion_decision(field)}
        report_properties[field['xmlAttributeName']].update(create_type_map(field['fieldType']))
    report_properties['customer_id'] = {'description': 'Profile ID',
                                        'behavior': 'ATTRIBUTE',
                                        'type': 'string',
                                        'field': "customer_id",
                                        'inclusion': 'always'}
    return {"type": "object",
            "properties": report_properties}

def do_discover_reports(sdk_client):
    url = 'https://adwords.google.com/api/adwords/reportdownload/v201702/reportDefinition.xsd'
    top_res = request_xsd(url)
    root = ET.fromstring(top_res)
    path = list(root.find(".//*[@name='ReportDefinition.ReportType']/*"))
    streams = [p.attrib['value'] for p in path]
    streams.remove("UNKNOWN")
    schema = {}
    for stream in streams:
        stream_schema = create_schema_for_report(stream, sdk_client)

        schema[stream] = stream_schema

    LOGGER.info("Report discovery complete")
    return schema

def do_discover_generic_endpoints():
    schema = {}
    for stream_name in GENERIC_ENDPOINTS:
        LOGGER.info('Loading schema for %s', stream_name)
        stream_schema = load_schema(stream_name)
        schema.update({stream_name: stream_schema})

    return schema

def do_discover(sdk_client):
    generic_schema = do_discover_generic_endpoints()
    report_schema = do_discover_reports(sdk_client)
    schema = {}
    schema.update(generic_schema)
    schema.update(report_schema)
    json.dump({"streams": schema}, sys.stdout, indent=4)
    LOGGER.info("Discovery complete")

def create_client(customer_id):    
    oauth2_client = oauth2.GoogleRefreshTokenClient(
        CONFIG['oauth_client_id'], \
        CONFIG['oauth_client_secret'], \
        CONFIG['refresh_token'])

    client = adwords.AdWordsClient(CONFIG['developer_token'], \
                                 oauth2_client, user_agent=CONFIG['user_agent'], \
                                 client_customer_id=customer_id)
    return client

def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    CONFIG.update(args.config)
    STATE.update(args.state)
    customer_ids = CONFIG['customer_ids'].split(",")

    if args.discover:
        client = create_client(customer_ids[0])
        do_discover(client)
    elif args.properties:
        for customer_id in customer_ids:
            client = create_client(customer_id)
            do_sync(args.properties, client)

        LOGGER.info("Sync Completed")

    else:
        LOGGER.info("No properties were selected")

if __name__ == "__main__":
    main()
