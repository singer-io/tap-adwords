#!/usr/bin/env python3

# pylint: disable=wrong-import-order
import datetime
import os
import sys
import io
import csv
import time
import json
import copy
import xml.etree.ElementTree as ET

import googleads
from googleads import adwords
from googleads import oauth2

import requests
import singer.metrics as metrics
import singer.bookmarks as bookmarks
import singer
from singer import utils
from singer import metadata
from singer import (transform,
                    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                    Transformer)
from dateutil.relativedelta import (relativedelta)
from suds.client import Client

LOGGER = singer.get_logger()
SESSION = requests.Session()
PAGE_SIZE = 1000
VERSION = 'v201708'

REPORT_TYPE_MAPPINGS = {"Boolean":  {"type": ["null", "boolean"]},
                        "boolean":  {'type': ["null", "boolean"]},
                        "Double":   {"type": ["null", "number"]},
                        "int":      {"type": ["null", "integer"]},
                        "Integer":  {"type": ["null", "integer"]},
                        "long":     {"type": ["null", "integer"]},
                        "Long":     {"type": ["null", "integer"]},
                        "Date":     {"type": ["null", "string"],
                                     "format": "date-time"},
                        "DateTime": {"type": ["null", "string"],
                                     "format": "date-time"},
                        "Money":    {"type": ["null", "integer", "string"]}}

GENERIC_ENDPOINT_MAPPINGS = {"campaigns": {'primary_keys': ["id"],
                                           'service_name': 'CampaignService'},
                             "ad_groups": {'primary_keys': ["id"],
                                           'service_name': 'AdGroupService'},
                             "ads":       {'primary_keys': ["adGroupId"],
                                           'service_name': 'AdGroupAdService'},
                             "accounts":  {'primary_keys': ["customerId"],
                                           'service_name': 'ManagedCustomerService'}}

REPORT_RUN_DATETIME = utils.strftime(utils.now())

VERIFIED_REPORTS = frozenset([
    # 'ACCOUNT_PERFORMANCE_REPORT',
    'ADGROUP_PERFORMANCE_REPORT',
    # 'AD_CUSTOMIZERS_FEED_ITEM_REPORT',
    'AD_PERFORMANCE_REPORT',
    'AGE_RANGE_PERFORMANCE_REPORT',
    'AUDIENCE_PERFORMANCE_REPORT',
    # 'AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT',
    # 'BID_GOAL_PERFORMANCE_REPORT',
    #'BUDGET_PERFORMANCE_REPORT',                       -- does NOT allow for querying by date range
    # 'CALL_METRICS_CALL_DETAILS_REPORT',
    #'CAMPAIGN_AD_SCHEDULE_TARGET_REPORT',
    #'CAMPAIGN_CRITERIA_REPORT',
    #'CAMPAIGN_GROUP_PERFORMANCE_REPORT',
    #'CAMPAIGN_LOCATION_TARGET_REPORT',
    #'CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT',   -- does NOT allow for querying by date range
    #'CAMPAIGN_NEGATIVE_LOCATIONS_REPORT',              -- does NOT allow for querying by date range
    #'CAMPAIGN_NEGATIVE_PLACEMENTS_PERFORMANCE_REPORT', -- does NOT allow for querying by date range
    'CAMPAIGN_PERFORMANCE_REPORT',
    #'CAMPAIGN_SHARED_SET_REPORT',                      -- does NOT allow for querying by date range
    'CLICK_PERFORMANCE_REPORT',
    #'CREATIVE_CONVERSION_REPORT',
    'CRITERIA_PERFORMANCE_REPORT',
    #'DESTINATION_URL_REPORT',
    #'DISPLAY_KEYWORD_PERFORMANCE_REPORT',
    #'DISPLAY_TOPICS_PERFORMANCE_REPORT',
    'FINAL_URL_REPORT',
    'GENDER_PERFORMANCE_REPORT',
    'GEO_PERFORMANCE_REPORT',
    #'KEYWORDLESS_CATEGORY_REPORT',
    #'KEYWORDLESS_QUERY_REPORT',
    'KEYWORDS_PERFORMANCE_REPORT',
    #'LABEL_REPORT',                                    -- does NOT allow for querying by date range,
    #'PAID_ORGANIC_QUERY_REPORT',
    #'PARENTAL_STATUS_PERFORMANCE_REPORT',
    #'PLACEHOLDER_FEED_ITEM_REPORT',
    #'PLACEHOLDER_REPORT',
    'PLACEMENT_PERFORMANCE_REPORT',
    #'PRODUCT_PARTITION_REPORT',
    'SEARCH_QUERY_PERFORMANCE_REPORT',
    #'SHARED_SET_CRITERIA_REPORT',                      -- does NOT allow for querying by date range
    #'SHARED_SET_REPORT',                               -- does NOT allow for querying by date range
    #'SHARED_SET_REPORT',
    #'SHOPPING_PERFORMANCE_REPORT',
    #'TOP_CONTENT_PERFORMANCE_REPORT',
    #'URL_PERFORMANCE_REPORT',
    #'USER_AD_DISTANCE_REPORT',
    #'VIDEO_PERFORMANCE_REPORT',
    #'UNKNOWN'
])

REPORTS_WITH_90_DAY_MAX = frozenset([
    'CLICK_PERFORMANCE_REPORT',
])

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


def load_metadata(entity):
    return utils.load_json(get_abs_path("metadata/{}.json".format(entity)))

def get_start_for_stream(customer_id, stream_name):
    bk_value = bookmarks.get_bookmark(STATE,
                                      state_key_name(customer_id, stream_name),
                                      'date')
    bk_start_date = utils.strptime_with_tz(bk_value or CONFIG['start_date'])
    return bk_start_date

def apply_conversion_window(start_date):
    conversion_window_days = int(CONFIG.get('conversion_window_days', '-30'))
    return start_date+relativedelta(days=conversion_window_days)

def get_end_date():
    if CONFIG.get('end_date'):
        return utils.strptime_with_tz(CONFIG.get('end_date'))

    return utils.now()

def state_key_name(customer_id, report_name):
    return report_name + "_" + customer_id

def should_sync(mdata, field):
    if mdata.get(('properties', field), {}).get('selected'):
        return True
    elif mdata.get(('properties', field), {}).get('inclusion') == 'automatic':
        return True

    return False

def get_fields_to_sync(discovered_schema, mdata):
    fields = discovered_schema['properties'] # pylint: disable=unsubscriptable-object
    return [field for field in fields if should_sync(mdata, field)]

def write_schema(stream_name, schema, primary_keys, bookmark_properties=None):
    schema_copy = copy.deepcopy(schema)
    singer.write_schema(stream_name, schema_copy, primary_keys, bookmark_properties=bookmark_properties)

# No rate limit here, since this request is only made once
# per discovery (not sync) job
def request_xsd(url):
    req = requests.Request("GET", url=url).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)

    return resp.text

def add_synthetic_keys_to_stream_schema(stream_schema):
    stream_schema['properties']['_sdc_customer_id'] = {'description': 'Profile ID',
                                                       'type': 'string',
                                                       'field': "customer_id"}
    stream_schema['properties']['_sdc_report_datetime'] = {'description': 'DateTime of Report Run',
                                                           'type': 'string',
                                                           'format' : 'date-time'}
    return stream_schema

def sync_report(stream_name, stream_metadata, sdk_client):
    customer_id = sdk_client.client_customer_id

    stream_schema, _ = create_schema_for_report(stream_name, sdk_client)
    stream_schema = add_synthetic_keys_to_stream_schema(stream_schema)

    xml_attribute_list = get_fields_to_sync(stream_schema, stream_metadata)

    primary_keys = []
    LOGGER.info("{} primary keys are {}".format(stream_name, primary_keys))

    write_schema(stream_name, stream_schema, primary_keys, bookmark_properties=['day'])

    field_list = []
    for field in xml_attribute_list:
        field_list.append(stream_metadata[('properties', field)]['adwords.fieldName'])

    check_selected_fields(stream_name, field_list, sdk_client)
    start_date = apply_conversion_window(get_start_for_stream(customer_id, stream_name))
    if stream_name in REPORTS_WITH_90_DAY_MAX:
        cutoff = utils.now()+relativedelta(days=-90)
        if start_date < cutoff:
            start_date = cutoff

    LOGGER.info('Selected fields: %s', field_list)

    while start_date <= get_end_date():
        sync_report_for_day(stream_name, stream_schema, sdk_client, start_date, field_list)
        start_date = start_date+relativedelta(days=1)
    LOGGER.info("Done syncing the %s report for customer_id %s", stream_name, customer_id)

def parse_csv_string(csv_string):
    string_buffer = io.StringIO(csv_string)
    reader = csv.reader(string_buffer)
    rows = [row for row in reader]

    headers = rows[0]
    values = rows[1:]
    return headers, values

def get_xml_attribute_headers(stream_schema, description_headers):
    description_to_xml_attribute = {}
    for key, value in stream_schema['properties'].items():
        description_to_xml_attribute[value['description']] = key
    description_to_xml_attribute['Ad policies'] = 'policy'

    xml_attribute_headers = [description_to_xml_attribute[header] for header in description_headers]
    return xml_attribute_headers

def transform_pre_hook(data, typ, schema): # pylint: disable=unused-argument
    if isinstance(data, str) and '--' in data:
        data = None

    elif data and typ == "number":
        if data == "> 90%":
            data = "90.01"

        if data == "< 10%":
            data = "9.99"

        if data.endswith(" x"):
            data = data[:-2]

        data = data.replace('%', '')

    return data

RETRY_SLEEP_TIME = 60
MAX_ATTEMPTS = 3
def with_retries_on_exception(sleepy_time, max_attempts):
    def wrap(some_function):
        def wrapped_function(*args):
            attempts = 1
            ex = None
            result = None

            try:
                result = some_function(*args)
            except Exception as our_ex:
                ex = our_ex

            while ex and attempts < max_attempts:
                LOGGER.warning("attempt {} of {} failed".format(attempts, some_function))
                LOGGER.warning("waiting {} seconds before retrying".format(sleepy_time))
                time.sleep(RETRY_SLEEP_TIME)
                try:
                    ex = None
                    result = some_function(*args)
                except Exception as our_ex:
                    ex = our_ex

                attempts = attempts + 1

            if ex:
                LOGGER.critical("Error encountered when contacting Google AdWords API after {} retries".format(MAX_ATTEMPTS))
                raise ex #pylint: disable=raising-bad-type
            else:
                return result
        return wrapped_function
    return wrap

@with_retries_on_exception(RETRY_SLEEP_TIME, MAX_ATTEMPTS)
def attempt_download_report(report_downloader, report):
    result = report_downloader.DownloadReportAsString(
        report, skip_report_header=True, skip_column_header=False,
        skip_report_summary=True,
        # Do not get data with 0 impressions, because some reports don't support that
        include_zero_impressions=False)
    return result

def sync_report_for_day(stream_name, stream_schema, sdk_client, start, field_list): # pylint: disable=too-many-locals
    report_downloader = sdk_client.GetReportDownloader(version=VERSION)
    customer_id = sdk_client.client_customer_id
    report = {
        'reportName': 'Seems this is required',
        'dateRangeType': 'CUSTOM_DATE',
        'reportType': stream_name,
        'downloadFormat': 'CSV',
        'selector': {
            'fields': field_list,
            'dateRange': {'min': start.strftime('%Y%m%d'),
                          'max': start.strftime('%Y%m%d')}}}

     # Fetch the report as a csv string
    with metrics.http_request_timer(stream_name):
        result = attempt_download_report(report_downloader, report)

    headers, values = parse_csv_string(result)
    with metrics.record_counter(stream_name) as counter:
        time_extracted = utils.now()

        for _, val in enumerate(values):
            obj = dict(zip(get_xml_attribute_headers(stream_schema, headers), val))
            obj['_sdc_customer_id'] = customer_id
            obj['_sdc_report_datetime'] = REPORT_RUN_DATETIME
            with Transformer(singer.UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
                bumble_bee.pre_hook = transform_pre_hook
                obj = bumble_bee.transform(obj, stream_schema)

            singer.write_record(stream_name, obj, time_extracted=time_extracted)
            counter.increment()

        if start > get_start_for_stream(sdk_client.client_customer_id, stream_name):
            LOGGER.info('updating bookmark: %s > %s', start, get_start_for_stream(sdk_client.client_customer_id, stream_name))
            bookmarks.write_bookmark(STATE,
                                     state_key_name(sdk_client.client_customer_id, stream_name),
                                     'date',
                                     start.strftime(utils.DATETIME_FMT))
            singer.write_state(STATE)
        else:
            LOGGER.info('not updating bookmark: %s <= %s', start, get_start_for_stream(sdk_client.client_customer_id, stream_name))

        LOGGER.info("Done syncing %s records for the %s report for customer_id %s on %s",
                    counter.value, stream_name, customer_id, start)

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

CAMPAIGNS_BLACK_LISTED_FIELDS = set(['networkSetting', 'conversionOptimizerEligibility',
                                     'frequencyCap'])
AD_GROUPS_BLACK_LISTED_FIELDS = set(['biddingStrategyConfiguration'])

def filter_fields_by_stream_name(stream_name, fields_to_sync):
    if stream_name == 'campaigns':
        return [f for f in fields_to_sync if f not in CAMPAIGNS_BLACK_LISTED_FIELDS]
    elif stream_name == 'ad_groups':
        return [f for f in fields_to_sync if f not in AD_GROUPS_BLACK_LISTED_FIELDS]
    elif stream_name == 'ads':
        return fields_to_sync
    elif stream_name == 'accounts':
        return fields_to_sync
    else:
        raise Exception("unrecognized generic stream_name {}".format(stream_name))

GOOGLE_MAX_RESULTSET_SIZE = 100000

def get_campaign_ids(sdk_client):
    # TODO this could be refactored to reuse some of the other functions
    LOGGER.info("Retrieving campaign ids for customer %s", sdk_client.client_customer_id)
    service_name = GENERIC_ENDPOINT_MAPPINGS['campaigns']['service_name']
    service_caller = sdk_client.GetService(service_name, version=VERSION)
    offset = 0
    selector = {
        'fields': ['Id'],
        'paging': {
            'startIndex': str(offset),
            'numberResults': str(PAGE_SIZE)
        }
    }

    campaign_ids = set()

    while True:
        LOGGER.info("Request %s campaign ids from offset %s for customer %s",
                    PAGE_SIZE,
                    offset,
                    sdk_client.client_customer_id)
        page = service_caller.get(selector)
        if page['totalNumEntries'] > GOOGLE_MAX_RESULTSET_SIZE:
            raise Exception("Too many campaigns (%s > %s) for customer %s",
                            page['totalNumEntries'],
                            GOOGLE_MAX_RESULTSET_SIZE,
                            sdk_client.client_customer_id)
        if 'entries' in page:
            for campaign_id in [entry['id'] for entry in page['entries']]:
                campaign_ids.add(campaign_id)
        offset += PAGE_SIZE
        selector['paging']['startIndex'] = str(offset)
        if offset > int(page['totalNumEntries']):
            break
    LOGGER.info("Retrieved %s campaign ids for customer %s. Expected %s.",
                len(campaign_ids),
                sdk_client.client_customer_id,
                page['totalNumEntries'])
    return campaign_ids

@with_retries_on_exception(RETRY_SLEEP_TIME, MAX_ATTEMPTS)
def attempt_get_from_service(service_caller, selector):
    return service_caller.get(selector)

def get_campaign_ids_filtered_page(sdk_client, fields, campaign_ids, stream, start_index):
    service_name = GENERIC_ENDPOINT_MAPPINGS[stream]['service_name']
    service_caller = sdk_client.GetService(service_name, version=VERSION)
    selector = {
        'fields': fields,
        'predicates': [
            {
                'field': 'BaseCampaignId',
                'operator': 'IN',
                'values': [str(campaign_id) for campaign_id in campaign_ids]
            }
        ],
        'paging': {
            'startIndex': str(start_index),
            'numberResults': str(PAGE_SIZE)
        }
    }
    with metrics.http_request_timer(stream):
        LOGGER.info("Request %s %s from start_index %s for customer %s, campaigns %s",
                    stream,
                    PAGE_SIZE,
                    start_index,
                    sdk_client.client_customer_id,
                    campaign_ids)
        page = attempt_get_from_service(service_caller, selector)
        return page

def get_unfiltered_page(sdk_client, fields, start_index, stream):
    service_name = GENERIC_ENDPOINT_MAPPINGS[stream]['service_name']
    service_caller = sdk_client.GetService(service_name, version=VERSION)
    selector = {
        'fields': fields,
        'paging': {
            'startIndex': str(start_index),
            'numberResults': str(PAGE_SIZE)
        }
    }
    with metrics.http_request_timer(stream):
        LOGGER.info("Request %s %s from start_index %s for customer %s",
                    PAGE_SIZE,
                    stream,
                    start_index,
                    sdk_client.client_customer_id)
        page = attempt_get_from_service(service_caller, selector)
        return page

def is_campaign_ids_selector_safe(sdk_client, campaign_ids, stream):
    LOGGER.info("Ensuring %s selector safety for campaigns %s", stream, campaign_ids)
    page = get_campaign_ids_filtered_page(sdk_client, ['Id'], campaign_ids, stream, 0)
    LOGGER.info("Total entries %s", page['totalNumEntries'])
    return page['totalNumEntries'] < GOOGLE_MAX_RESULTSET_SIZE

# Arbitrary window. Would be smarter to do a binary search rather than
# build up from the bottom.
CAMPAIGN_PARTITION_SIZE = 15

# TODO The strategy here is naive. It assumes that the partition size will
# be small enough to not contain 2 campaign ids that encompass a larger
# than GOOGLE_MAX_RESULTSET_SIZE result set. The only reason we _must_
# fail to retrieve ads or ad_groups is if a single campaign has more than
# GOOGLE_MAX_RESULTSET_SIZE of them. That said, the implementation of a
# smarter approach would be significantly more complicated, so until it
# comes up in production we'll leave the naive approach.
#
# A less naive approach which would also reduce the total number of
# requests for non-pathological cases would be to do a binary search of
# the campaign id space. Splitting each campaign id set until each one has
# a small enough result set would be better.
def get_campaign_ids_safe_selectors(sdk_client,
                                    campaign_ids,
                                    stream):
    LOGGER.info("Discovering safe %s selectors for customer %s",
                stream,
                sdk_client.client_customer_id)
    safe_selectors = []
    current_campaign_ids_window = []
    campaign_ids = [campaign_id for campaign_id in campaign_ids]
    campaign_ids_partitions = [campaign_ids[i:i + CAMPAIGN_PARTITION_SIZE]
                               for i
                               in range(0,
                                        len(campaign_ids),
                                        CAMPAIGN_PARTITION_SIZE)]
    for campaign_ids_partition in campaign_ids_partitions:
        if not is_campaign_ids_selector_safe(
                sdk_client,
                current_campaign_ids_window + campaign_ids_partition,
                stream):
            safe_selectors += [current_campaign_ids_window]
            current_campaign_ids_window = campaign_ids_partition
        else:
            current_campaign_ids_window += campaign_ids_partition
    safe_selectors += [current_campaign_ids_window]
    return safe_selectors

def get_field_list(stream_schema, stream, stream_metadata):
    #NB> add synthetic keys
    field_list = get_fields_to_sync(stream_schema, stream_metadata)
    LOGGER.info("Request fields: %s", field_list)
    field_list = filter_fields_by_stream_name(stream, field_list)
    LOGGER.info("Filtered fields: %s", field_list)
    # These are munged because of the nature of the API. When you pass
    # the field to the API you need to change its first letter to
    # upper case.
    #
    # See:
    # https://developers.google.com/adwords/api/docs/reference/v201708/AdGroupAdService.AdGroupAd
    # for instance
    field_list = [f[0].upper()+f[1:] for f in field_list]
    LOGGER.info("Munged fields: %s", field_list)
    return field_list

def sync_campaign_ids_endpoint(sdk_client,
                               campaign_ids,
                               stream,
                               stream_metadata):
    discovered_schema = load_schema(stream)

    field_list = get_field_list(discovered_schema, stream, stream_metadata)
    discovered_schema['properties']['_sdc_customer_id'] = {
        'description': 'Profile ID',
        'type': 'string',
        'field': "customer_id"
    }
    primary_keys = GENERIC_ENDPOINT_MAPPINGS[stream]['primary_keys']
    write_schema(stream, discovered_schema, primary_keys)

    LOGGER.info("Syncing %s for customer %s", stream, sdk_client.client_customer_id)

    for safe_selector in get_campaign_ids_safe_selectors(
            sdk_client,
            campaign_ids,
            stream):
        start_index = 0
        while True:
            page = get_campaign_ids_filtered_page(sdk_client,
                                                  field_list,
                                                  safe_selector,
                                                  stream,
                                                  start_index)
            if page['totalNumEntries'] > GOOGLE_MAX_RESULTSET_SIZE:
                raise Exception("Too many {} ({} > {}) for customer {}, campaigns {}".format(
                    stream,
                    GOOGLE_MAX_RESULTSET_SIZE,
                    page['totalNumEntries'],
                    sdk_client.client_customer_id,
                    campaign_ids))
            if 'entries' in page:
                with metrics.record_counter(stream) as counter:
                    time_extracted = utils.now()

                    for entry in page['entries']:
                        obj = suds_to_dict(entry)
                        obj['_sdc_customer_id'] = sdk_client.client_customer_id
                        with Transformer(singer.UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee: #pylint: disable=line-too-long
                            bumble_bee.pre_hook = transform_pre_hook
                            record = bumble_bee.transform(obj, discovered_schema)

                            singer.write_record(stream, record, time_extracted=time_extracted)
                            counter.increment()

            start_index += PAGE_SIZE
            if start_index > int(page['totalNumEntries']):
                break
    LOGGER.info("Done syncing %s for customer_id %s", stream, sdk_client.client_customer_id)

def sync_generic_basic_endpoint(sdk_client, stream, stream_metadata):
    discovered_schema = load_schema(stream)
    field_list = get_field_list(discovered_schema, stream, stream_metadata)

    discovered_schema['properties']['_sdc_customer_id'] = {
        'description': 'Profile ID',
        'type': 'string',
        'field': "customer_id"
    }
    primary_keys = GENERIC_ENDPOINT_MAPPINGS[stream]['primary_keys']
    write_schema(stream, discovered_schema, primary_keys)

    LOGGER.info("Syncing %s for customer %s", stream, sdk_client.client_customer_id)


    start_index = 0
    while True:
        page = get_unfiltered_page(sdk_client, field_list, start_index, stream)
        if page['totalNumEntries'] > GOOGLE_MAX_RESULTSET_SIZE:
            raise Exception("Too many %s (%s > %s) for customer %s",
                            stream,
                            GOOGLE_MAX_RESULTSET_SIZE,
                            page['totalNumEntries'],
                            sdk_client.client_customer_id)

        if 'entries' in page:
            with metrics.record_counter(stream) as counter:
                time_extracted = utils.now()

                for entry in page['entries']:
                    obj = suds_to_dict(entry)
                    obj['_sdc_customer_id'] = sdk_client.client_customer_id
                    with Transformer(singer.UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee: #pylint: disable=line-too-long
                        bumble_bee.pre_hook = transform_pre_hook
                        record = bumble_bee.transform(obj, discovered_schema)

                        singer.write_record(stream, record, time_extracted=time_extracted)
                        counter.increment()

        start_index += PAGE_SIZE
        if start_index > int(page['totalNumEntries']):
            break
    LOGGER.info("Done syncing %s for customer_id %s", stream, sdk_client.client_customer_id)

def sync_generic_endpoint(stream_name, stream_metadata, sdk_client):
    campaign_ids = get_campaign_ids(sdk_client)
    if stream_name == 'ads' or stream_name == 'ad_groups':
        if not campaign_ids:
            LOGGER.info("No %s for customer %s", stream_name, sdk_client.client_customer_id)
            return

        sync_campaign_ids_endpoint(sdk_client,
                                   campaign_ids,
                                   stream_name,
                                   stream_metadata)
    elif stream_name == 'campaigns' or stream_name == 'accounts':
        sync_generic_basic_endpoint(sdk_client,
                                    stream_name,
                                    stream_metadata)
    else:
        raise Exception("Undefined generic endpoint %s", stream_name)

def sync_stream(stream_name, stream_metadata, sdk_client):
    if stream_name in GENERIC_ENDPOINT_MAPPINGS:
        sync_generic_endpoint(stream_name, stream_metadata, sdk_client)
    else:
        sync_report(stream_name, stream_metadata, sdk_client)

def do_sync(properties, sdk_client):
    for catalog in properties['streams']:
        stream_name = catalog.get('stream')
        stream_metadata = metadata.to_map(catalog.get('metadata'))

        if stream_metadata.get((), {}).get('selected'):
            LOGGER.info('Syncing stream %s ...', stream_name)
            sync_stream(stream_name, stream_metadata, sdk_client)
        else:
            LOGGER.info('Skipping stream %s.', stream_name)

def get_report_definition_service(report_type, sdk_client):
    report_definition_service = sdk_client.GetService(
        'ReportDefinitionService', version=VERSION)
    fields = report_definition_service.getReportFields(report_type)
    return fields

def create_type_map(typ):
    if REPORT_TYPE_MAPPINGS.get(typ):
        return REPORT_TYPE_MAPPINGS.get(typ)
    return {'type' : ['null', 'string']}

def create_field_metadata_for_report(stream, fields, field_name_lookup):
    mdata = {}
    mdata = metadata.write(mdata, (), 'inclusion', 'available')

    for field in fields:
        breadcrumb = ('properties', str(field['xmlAttributeName']))
        if  hasattr(field, "exclusiveFields"):
            mdata = metadata.write(mdata,
                                   breadcrumb,
                                   'fieldExclusions',
                                   [['properties', field_name_lookup[x]]
                                    for x
                                    in field['exclusiveFields']])
        mdata = metadata.write(mdata, breadcrumb, 'behavior', field['fieldBehavior'])
        mdata = metadata.write(mdata, breadcrumb, 'adwords.fieldName', field['fieldName'])

        #inclusion
        if field['xmlAttributeName'] == 'day':
            # Every report with this attribute errors with an empty
            # 400 if it is not included in the field list.
            mdata = metadata.write(mdata, breadcrumb, 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, breadcrumb, 'inclusion', 'available')


    if stream == 'GEO_PERFORMANCE_REPORT':
        # Requests for this report that don't include countryTerritory
        # fail with an empty 400. There's no evidence for this in the
        # docs but it is what it is.
        mdata = metadata.write(mdata, ('properties', 'countryTerritory'), 'inclusion', 'automatic')

    return mdata

def create_schema_for_report(stream, sdk_client):
    report_properties = {}
    field_name_lookup = {}
    LOGGER.info('Loading schema for %s', stream)
    fields = get_report_definition_service(stream, sdk_client)

    for field in fields:
        field_name_lookup[field['fieldName']] = str(field['xmlAttributeName'])
        report_properties[field['xmlAttributeName']] = {'description': field['displayFieldName']}
        report_properties[field['xmlAttributeName']].update(create_type_map(field['fieldType']))


    if stream == 'AD_PERFORMANCE_REPORT':
        # The data for this field is "image/jpeg" etc. However, the
        # discovered schema from the report description service claims
        # that it should be an integer. This is needed to correct that.
        report_properties['imageMimeType']['type'] = ['null', 'string']

    if stream == 'CALL_METRICS_CALL_DETAILS_REPORT':
        # The data for this field is something like `Jan 1, 2016 1:32:22
        # PM` but the discovered schema is integer.
        report_properties['startTime']['type'] = ['null', 'string']
        report_properties['startTime']['format'] = 'date-time'
        # The data for this field is something like `Jan 1, 2016 1:32:22
        # PM` but the discovered schema is integer
        report_properties['endTime']['type'] = ['null', 'string']
        report_properties['endTime']['format'] = 'date-time'

    mdata = create_field_metadata_for_report(stream, fields, field_name_lookup)

    return ({"type": "object",
             "is_report": 'true',
             "properties": report_properties},
            mdata)

def check_selected_fields(stream, field_list, sdk_client):
    field_set = set(field_list)
    fields = get_report_definition_service(stream, sdk_client)
    field_map = {f.fieldName: f.xmlAttributeName for f in fields}
    errors = []
    for field in fields:
        if field.fieldName not in field_set:
            continue

        if not hasattr(field, "exclusiveFields"):
            continue

        field_errors = []
        for ex_field in field.exclusiveFields:
            if ex_field in field_set:
                field_errors.append(field_map[ex_field])

        if field_errors:
            errors.append("{} cannot be selected with {}".format(
                field.xmlAttributeName, ",".join(field_errors)))

    if errors:
        raise Exception("Fields selection violates Google's exclusion rules:\n\t{}" \
                        .format("\n\t".join(errors)))

def do_discover_reports(sdk_client):
    url = 'https://adwords.google.com/api/adwords/reportdownload/{}/reportDefinition.xsd'.format(VERSION) #pylint: disable=line-too-long
    xsd = request_xsd(url)
    root = ET.fromstring(xsd)
    nodes = list(root.find(".//*[@name='ReportDefinition.ReportType']/*"))

    stream_names = [p.attrib['value'] for p in nodes if p.attrib['value'] in VERIFIED_REPORTS] #pylint: disable=line-too-long
    streams = []
    LOGGER.info("Starting report discovery")
    for stream_name in stream_names:
        schema, mdata = create_schema_for_report(stream_name, sdk_client)
        streams.append({'stream': stream_name,
                        'tap_stream_id': stream_name,
                        'metadata' : metadata.to_list(mdata),
                        'schema': schema})

    LOGGER.info("Report discovery complete")
    return streams


def do_discover_generic_endpoints():
    streams = []
    LOGGER.info("Starting generic discovery")
    for stream_name in GENERIC_ENDPOINT_MAPPINGS:
        LOGGER.info('Loading schema for %s', stream_name)
        schema = load_schema(stream_name)
        md = load_metadata(stream_name)
        streams.append({'stream': stream_name,
                        'tap_stream_id': stream_name,
                        'schema': schema,
                        'metadata': md})
    LOGGER.info("Generic discovery complete")
    return streams

def do_discover(customer_ids):
    sdk_client = create_sdk_client(customer_ids[0])
    generic_streams = do_discover_generic_endpoints()
    report_streams = do_discover_reports(sdk_client)
    streams = []
    streams.extend(generic_streams)
    streams.extend(report_streams)
    json.dump({"streams": streams}, sys.stdout, indent=2)

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
        LOGGER.info('Syncing customer ID %s ...', customer_id)
        sdk_client = create_sdk_client(customer_id)
        do_sync(properties, sdk_client)
        LOGGER.info('Done syncing customer ID %s.', customer_id)

def main_impl():
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

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc

if __name__ == "__main__":
    main()
