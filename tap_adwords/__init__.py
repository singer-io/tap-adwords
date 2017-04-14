#!/usr/bin/env python3

import datetime
import os
import sys
import io
import csv
import time
import json

import xml.etree.ElementTree as ET
from suds.client import Client
import pendulum
import googleads
from googleads import adwords
from googleads import oauth2

import requests
import singer

from singer import utils
from singer import transform

LOGGER = singer.get_logger()
SESSION = requests.Session()
PAGE_SIZE = 100
VERSION = 'v201702'

REPORT_TYPE_MAPPINGS = {"Boolean":  {"type": "boolean"},
                        "boolean":  {'type': "boolean"},
                        "Double":   {"type": "number"},
                        "int":      {"type": "integer"},
                        "Integer":  {"type": "integer"},
                        "long":     {"type": "integer"},
                        "Long":     {"type": "integer"},
                        "Date":     {"type": "string",
                                     "format": "date-time"},
                        "DateTime": {"type": "string",
                                     "format": "date-time"}}

GENERIC_ENDPOINT_MAPPINGS = {"campaigns": {'primary_keys': ["id"],
                                           'service_name': 'CampaignService'},
                             "ad_groups": {'primary_keys': ["id"],
                                           'service_name': 'AdGroupService'},
                             "ads":       {'primary_keys': ["id", "adGroupId"],
                                           'service_name': 'AdGroupAdService'},
                             "accounts":  {'primary_keys': ["customerId"],
                                           'service_name': 'ManagedCustomerService'}}

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "oauth_client_id",
    "oauth_client_secret",
    "user_agent",
    "refresh_token",
    "customer_ids",
    "developer_token",
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

def state_key_name(customer_id, report_name):
    return report_name + "_" + customer_id

def get_fields(stream_schema, annotated_schema):
    props = annotated_schema['properties'] # pylint: disable=unsubscriptable-object
    return [k for k in props if props[k].get('selected') or stream_schema['properties'][k].get('inclusion') == 'always']

def get_report_segment_fields(annotated_schema):
    primary_keys = []

    for property_key, property_value in annotated_schema['properties'].items():
        if property_value['behavior'] == 'SEGMENT' and property_value.get('selected'):
            primary_keys.append(property_key)

    return primary_keys

# No rate limit here, since this request is only made once
# per discovery (not sync) job
def request_xsd(url):
    req = requests.Request("GET", url=url).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)

    return resp.text

def sync_report(stream_name, annotated_stream_schema, sdk_client):
    stream_schema = create_schema_for_report(stream_name, sdk_client)
    report_downloader = sdk_client.GetReportDownloader(version=VERSION)
    xml_attribute_list = get_fields(stream_schema, annotated_stream_schema)
    primary_keys = []
    primary_keys += get_report_segment_fields(annotated_stream_schema) + ['customer_id', 'day']
    LOGGER.info("{} primary keys are {}".format(stream_name, primary_keys))

    real_field_list = []
    seen_pk_values = set([])
    singer.write_schema(stream_name, stream_schema, primary_keys)

    for field in xml_attribute_list:
        if field != 'customer_id':
            real_field_list.append(stream_schema['properties'][field]['field'])

    start_date = pendulum.parse(get_start(state_key_name(sdk_client.client_customer_id, stream_name)))
    while start_date <= pendulum.now():
        sync_report_for_day(stream_name, stream_schema, report_downloader, sdk_client.client_customer_id, start_date, real_field_list, primary_keys, seen_pk_values)
        start_date = start_date.add(days=1)

def sync_report_for_day(stream_name, stream_schema, report_downloader, customer_id, start, real_field_list, primary_keys, seen_pk_values):
    LOGGER.info("field list for report is {}".format(real_field_list))
    # Create report definition.
    report = {
        'reportName': 'Seems this is required',
        'dateRangeType': 'CUSTOM_DATE',
        'reportType': stream_name,
        'downloadFormat': 'CSV',
        'selector': {
            'fields': real_field_list,
            'dateRange': {'min': start.strftime('%Y%m%d'),
                          'max': start.strftime('%Y%m%d')}}}

    # Print out the report as a string
    # Do not get data with 0 impressions, some reports don't support that
    result = report_downloader.DownloadReportAsString(
        report, skip_report_header=True, skip_column_header=False,
        skip_report_summary=True, include_zero_impressions=False)

    string_buffer = io.StringIO(result)
    reader = csv.reader(string_buffer)
    rows = []
    for row in reader:
        rows.append(row)

    headers = rows[0]
    values = rows[1:]

    description_to_xml_attribute = {}
    for key, value in stream_schema['properties'].items():
        description_to_xml_attribute[value['description']] = key

    xml_attribute_headers = [description_to_xml_attribute[header] for header in headers]
    for val in values:
        obj = dict(zip(xml_attribute_headers, val))
        obj['customer_id'] = customer_id
        obj = transform.transform(obj, stream_schema)
        print("ojbect is {}".format(obj))
        tup = tuple([obj.get(pk) for pk in primary_keys])
        if tup in seen_pk_values:
            raise Exception("Duplicate PK value found for pk values {}".format(dict(zip(primary_keys, tup))))
        seen_pk_values.add(tup)
        print("about to write record")
        singer.write_record(stream_name, obj)

    utils.update_state(STATE, state_key_name(customer_id, stream_name), start)
    singer.write_state(STATE)

    LOGGER.info("Done syncing for customer id {} report {} for date range {} to {}".format(customer_id, stream_name, start, start))

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

def sync_generic_endpoint(stream_name, annotated_stream_schema, sdk_client):
    discovered_schema = load_schema(stream_name)
    service_name = GENERIC_ENDPOINT_MAPPINGS[stream_name]['service_name']
    primary_keys = GENERIC_ENDPOINT_MAPPINGS[stream_name]['primary_keys']
    singer.write_schema(stream_name, discovered_schema, primary_keys)

    LOGGER.info("Syncing %s", stream_name)
    service_caller = sdk_client.GetService(service_name, version=VERSION)

    field_list = get_fields(discovered_schema, annotated_stream_schema)
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

                singer.write_record(stream_name, transform.transform(suds_to_dict(entry), discovered_schema))
        offset += PAGE_SIZE
        selector['paging']['startIndex'] = str(offset)
        more_pages = offset < int(page['totalNumEntries'])

def sync_stream(stream, annotated_schema, sdk_client):
    if stream in GENERIC_ENDPOINT_MAPPINGS:
        sync_generic_endpoint(stream, annotated_schema, sdk_client)
    else:
        sync_report(stream, annotated_schema, sdk_client)

def do_sync(annotated_schema, sdk_client):
    for stream_name, stream_schema in annotated_schema['streams'].items():
        if stream_schema.get('selected'):
            sync_stream(stream_name, stream_schema, sdk_client)

def get_report_definition_service(report_type, sdk_client):
    report_definition_service = sdk_client.GetService(
        'ReportDefinitionService', version=VERSION)
    fields = report_definition_service.getReportFields(report_type)
    return fields

def create_type_map(typ):
    if REPORT_TYPE_MAPPINGS.get(typ):
        return REPORT_TYPE_MAPPINGS.get(typ)
    return {'type' : 'string'}

def create_schema_for_report(stream, sdk_client):
    report_properties = {}
    LOGGER.info('Loading schema for %s', stream)
    fields = get_report_definition_service(stream, sdk_client)
    for field in fields:
        report_properties[field['xmlAttributeName']] = {'description': field['displayFieldName'],
                                                        'behavior': field['fieldBehavior'],
                                                        'field': field['fieldName'],
                                                        'inclusion': "available"}
        report_properties[field['xmlAttributeName']].update(create_type_map(field['fieldType']))
    report_properties['customer_id'] = {'description': 'Profile ID',
                                        'behavior': 'ATTRIBUTE',
                                        'type': 'string',
                                        'field': "customer_id",
                                        'inclusion': 'always'}
    return {"type": "object",
            "properties": report_properties,
            "inclusion": "available"}

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
    for stream_name in GENERIC_ENDPOINT_MAPPINGS:
        LOGGER.info('Loading schema for %s', stream_name)
        stream_schema = load_schema(stream_name)
        schema.update({stream_name: stream_schema})
    return schema

def do_discover(customer_ids):
    sdk_client = create_sdk_client(customer_ids[0])
    generic_schema = do_discover_generic_endpoints()
    report_schema = do_discover_reports(sdk_client)
    schema = {}
    schema.update(generic_schema)
    schema.update(report_schema)
    json.dump({"streams": schema}, sys.stdout, indent=4)

def create_sdk_client(customer_id):
    oauth2_client = oauth2.GoogleRefreshTokenClient(
        CONFIG['oauth_client_id'], \
        CONFIG['oauth_client_secret'], \
        CONFIG['refresh_token'])

    sdk_client = adwords.AdWordsClient(CONFIG['developer_token'], \
                                 oauth2_client, user_agent=CONFIG['user_agent'], \
                                 client_customer_id=customer_id)
    return sdk_client

def do_sync_all_customers(customer_ids, properties):
    for customer_id in customer_ids:
        sdk_client = create_sdk_client(customer_id)
        do_sync(properties, sdk_client)

def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    CONFIG.update(args.config)
    STATE.update(args.state)
    customer_ids = CONFIG['customer_ids'].split(",")

    if args.discover:
        do_discover(customer_ids)
        LOGGER.info("Discovery complete")
    elif args.properties:
        do_sync_all_customers(customer_ids, args.properties)
        LOGGER.info("Sync Completed")
    else:
        LOGGER.info("No properties were selected")

if __name__ == "__main__":
    main()
