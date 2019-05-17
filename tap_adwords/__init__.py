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
import pytz
import xml.etree.ElementTree as ET

import googleads
from googleads import adwords
from googleads import oauth2

import requests
import singer
import zeep
from singer import metrics
from singer import bookmarks
from singer import utils
from singer import metadata
from singer import (transform,
                    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                    Transformer)
from dateutil.relativedelta import (relativedelta)
import math

LOGGER = singer.get_logger()
SESSION = requests.Session()

PAGE_SIZE = 1000

VERSION = 'v201809'

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
    'ACCOUNT_PERFORMANCE_REPORT',
    'ADGROUP_PERFORMANCE_REPORT',
    # 'AD_CUSTOMIZERS_FEED_ITEM_REPORT',
    'AD_PERFORMANCE_REPORT',
    'AGE_RANGE_PERFORMANCE_REPORT',
    'AUDIENCE_PERFORMANCE_REPORT',
    # 'AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT',
    # 'BID_GOAL_PERFORMANCE_REPORT',
    #'BUDGET_PERFORMANCE_REPORT',                       -- does NOT allow for querying by date range
    'CALL_METRICS_CALL_DETAILS_REPORT',
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
    'DISPLAY_KEYWORD_PERFORMANCE_REPORT',
    'DISPLAY_TOPICS_PERFORMANCE_REPORT',
    'FINAL_URL_REPORT',
    'GENDER_PERFORMANCE_REPORT',
    'GEO_PERFORMANCE_REPORT',
    #'KEYWORDLESS_CATEGORY_REPORT',
    'KEYWORDLESS_QUERY_REPORT',
    'KEYWORDS_PERFORMANCE_REPORT',
    #'LABEL_REPORT',                                    -- does NOT allow for querying by date range,
    #'PAID_ORGANIC_QUERY_REPORT',
    #'PARENTAL_STATUS_PERFORMANCE_REPORT',
    'PLACEHOLDER_FEED_ITEM_REPORT',
    'PLACEHOLDER_REPORT',
    'PLACEMENT_PERFORMANCE_REPORT',
    #'PRODUCT_PARTITION_REPORT',
    'SEARCH_QUERY_PERFORMANCE_REPORT',
    #'SHARED_SET_CRITERIA_REPORT',                      -- does NOT allow for querying by date range
    #'SHARED_SET_REPORT',                               -- does NOT allow for querying by date range
    #'SHARED_SET_REPORT',
    'SHOPPING_PERFORMANCE_REPORT',
    #'TOP_CONTENT_PERFORMANCE_REPORT',
    #'URL_PERFORMANCE_REPORT',
    #'USER_AD_DISTANCE_REPORT',
    'VIDEO_PERFORMANCE_REPORT',
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

def get_attribution_window_bookmark(customer_id, stream_name):
    mid_bk_value = bookmarks.get_bookmark(STATE,
                                          state_key_name(customer_id, stream_name),
                                          'last_attribution_window_date')
    return utils.strptime_with_tz(mid_bk_value) if mid_bk_value else None

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
    # If an attribution window sync is interrupted, start where it left off
    start_date = get_attribution_window_bookmark(customer_id, stream_name)
    if start_date is None:
        start_date = apply_conversion_window(get_start_for_stream(customer_id, stream_name))

    if stream_name in REPORTS_WITH_90_DAY_MAX:
        cutoff = utils.now()+relativedelta(days=-90)
        if start_date < cutoff:
            start_date = cutoff

    LOGGER.info('Selected fields: %s', field_list)

    while start_date <= get_end_date():
        sync_report_for_day(stream_name, stream_schema, sdk_client, start_date, field_list)
        start_date = start_date+relativedelta(days=1)
        bookmarks.write_bookmark(STATE,
                                 state_key_name(customer_id, stream_name),
                                 'last_attribution_window_date',
                                 start_date.strftime(utils.DATETIME_FMT))
        singer.write_state(STATE)
    bookmarks.clear_bookmark(STATE,
                             state_key_name(customer_id, stream_name),
                             'last_attribution_window_date')
    singer.write_state(STATE)
    LOGGER.info("Done syncing the %s report for customer_id %s", stream_name, customer_id)

def parse_csv_stream(csv_stream):
    # Wrap the binary stream in a utf-8 stream
    tw = io.TextIOWrapper(csv_stream, encoding='utf-8')

    # Read a single line into a String, and parse the headers as a CSV
    headers = csv.reader(io.StringIO(tw.readline()))
    header_array = [f for f in headers][0]

    # Create another CSV reader for the rest of the data
    csv_reader = csv.reader(tw)
    return header_array, csv_reader

def get_xml_attribute_headers(stream_schema, description_headers):
    description_to_xml_attribute = {}
    for key, value in stream_schema['properties'].items():
        description_to_xml_attribute[value['description']] = key
    description_to_xml_attribute['Ad policies'] = 'policy'

    xml_attribute_headers = [description_to_xml_attribute[header] for header in description_headers]
    return xml_attribute_headers

def transform_pre_hook(data, typ, schema): # pylint: disable=unused-argument
    # A value of two dashes (--) indicates there is no value
    # See https://developers.google.com/adwords/api/docs/guides/reporting#two_dashes
    if isinstance(data, str) and data.strip() == '--':
        data = None

    elif data and typ == "number":
        if data == "> 90%":
            data = "90.01"

        if data == "< 10%":
            data = "9.99"

        if data.endswith(" x"):
            data = data[:-2]

        data = data.replace('%', '')
    elif data and typ == 'object':
        data = zeep.helpers.serialize_object(data, target_cls=dict)

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

            return result
        return wrapped_function
    return wrap

@with_retries_on_exception(RETRY_SLEEP_TIME, MAX_ATTEMPTS)
def attempt_download_report(report_downloader, report):
    result = report_downloader.DownloadReportAsStream(
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

    headers, csv_reader = parse_csv_stream(result)
    with metrics.record_counter(stream_name) as counter:
        time_extracted = utils.now()

        with Transformer(singer.UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
            for row in csv_reader:
                obj = dict(zip(get_xml_attribute_headers(stream_schema, headers), row))
                obj['_sdc_customer_id'] = customer_id
                obj['_sdc_report_datetime'] = REPORT_RUN_DATETIME

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

# You cannot retrieve pages past
# 100k. https://developers.google.com/adwords/api/docs/appendix/limits#general
GOOGLE_MAX_START_INDEX = 100000

# You cannot use more than 10k predicate values for IN or NOT_IN
# operators. http://googleadsdeveloper.blogspot.com/2014/01/ensuring-reliable-performance-with-new.html
GOOGLE_MAX_PREDICATE_SIZE = 10000

@with_retries_on_exception(RETRY_SLEEP_TIME, MAX_ATTEMPTS)
def attempt_get_from_service(service_caller, selector):
    try:
        return service_caller.get(selector)
    except:
        LOGGER.info("An exception was thrown in %s with selector: %s",
                    list(service_caller.zeep_client.wsdl.services.keys())[0],
                    selector)
        raise

def set_index(selector, index):
    selector['paging']['startIndex'] = str(index)
    return selector

def get_service_caller(sdk_client, stream):
    service_name = GENERIC_ENDPOINT_MAPPINGS[stream]['service_name']
    return sdk_client.GetService(service_name, version=VERSION)

def get_page(sdk_client, selector, stream, start_index):
    service_caller = get_service_caller(sdk_client, stream)
    selector = set_index(selector, start_index)
    with metrics.http_request_timer(stream):
        LOGGER.info("Request %s %s for customer %s with startIndex %s using selector %s",
                    PAGE_SIZE,
                    stream,
                    sdk_client.client_customer_id,
                    selector['paging']['startIndex'],
                    hash(str(selector)))
        page = attempt_get_from_service(service_caller, selector)
        return page

#pylint: disable=too-many-return-statements
def binary_search(l, min_high, max_high, kosher_fn):
    mid = math.ceil((min_high + max_high) / 2)
    if min_high == max_high:
        if kosher_fn(l):
            return max_high, True

        return 0, False

    if min_high + 1 == max_high:
        if kosher_fn(l[0:max_high+1]):
            return max_high, True
        elif kosher_fn(l[0:min_high+1]):
            return min_high, True

        return 0, False

    if kosher_fn(l[0:mid+1]):
        return binary_search(l, mid, max_high, kosher_fn)

    return binary_search(l, min_high, mid, kosher_fn)

def set_selector_predicate_values(selector, predicate_field, predicate_values):
    my_selector = copy.deepcopy(selector)
    my_predicate = get_predicate(my_selector, predicate_field)
    my_predicate['values'] = predicate_values
    return my_selector

def iter_safe_selectors(selector, predicate_field, kosher_fn):
    predicate_values = get_predicate_field_values(selector, predicate_field)
    while predicate_values:
        to, success = binary_search(predicate_values, 0, len(predicate_values) - 1, kosher_fn)
        selector = set_selector_predicate_values(selector, predicate_field, predicate_values[0:to+1])
        yield selector, success
        predicate_values = predicate_values[to+1:]

def get_predicate(selector, predicate_field):
    return [p
            for p
            in selector['predicates']
            if p['field'] == predicate_field][0]

def get_predicate_field_values(selector, predicate_field):
    return get_predicate(selector, predicate_field)['values']

def get_campaign_ids_selector(campaign_ids, fields, start_index):
    return {
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

def is_campaign_ids_safe(sdk_client, stream, campaign_ids):
    selector = get_campaign_ids_selector(campaign_ids, ["Id"], 0)
    return is_selector_safe(sdk_client, stream, selector, 'BaseCampaignId')

def get_campaign_ids_safe_selectors(sdk_client,
                                    campaign_ids,
                                    fields,
                                    stream):
    LOGGER.info("Discovering safe %s selectors for customer %s",
                stream,
                sdk_client.client_customer_id)

    is_campaign_ids_safe_lambda = lambda cids: is_campaign_ids_safe(sdk_client,
                                                                    stream,
                                                                    cids)
    start = get_campaign_ids_selector(campaign_ids, fields, 0)
    if is_campaign_ids_safe_lambda(campaign_ids):
        yield start, True

    yield from iter_safe_selectors(start,
                                   'BaseCampaignId',
                                   is_campaign_ids_safe_lambda)

def set_fields(selector, fields):
    selector['fields'] = fields
    return selector

def get_selector_ids(sdk_client, stream, selector):
    LOGGER.info("Retrieving selector ids for customer %s, selector hash %s",
                sdk_client.client_customer_id,
                hash(str(selector)))
    service_name = GENERIC_ENDPOINT_MAPPINGS[stream]['service_name']
    service_caller = sdk_client.GetService(service_name, version=VERSION)
    offset = 0

    selector_ids = set()

    while True:
        selector = set_index(selector, offset)
        LOGGER.info("Request %s selector ids for customer %s, selector hash %s",
                    PAGE_SIZE,
                    sdk_client.client_customer_id,
                    hash(str(selector)))
        page = service_caller.get(set_fields(selector, ["Id"]))
        if page['totalNumEntries'] > GOOGLE_MAX_START_INDEX:
            raise Exception("Too many entries (%s > %s) for customer %s, selector hash %s" % (
                page['totalNumEntries'],
                GOOGLE_MAX_START_INDEX,
                sdk_client.client_customer_id,
                hash(str(selector))))
        if 'entries' in page:
            for selector_id in [entry['id'] for entry in page['entries']]:
                selector_ids.add(selector_id)
        offset += PAGE_SIZE
        if offset > int(page['totalNumEntries']):
            break
    LOGGER.info("Retrieved %s selector ids for customer %s. Expected %s.",
                len(selector_ids),
                sdk_client.client_customer_id,
                page['totalNumEntries'])
    return selector_ids

def get_ad_group_ids_selector(campaign_ids_selector, ad_group_ids):
    campaign_ids_selector_copy = copy.deepcopy(campaign_ids_selector)
    campaign_ids_selector_copy["predicates"].append(
        {
            'field': 'AdGroupId',
            'operator': 'IN',
            'values': ad_group_ids
        })
    return campaign_ids_selector_copy

def is_selector_safe(sdk_client, stream, selector, predicate_field):
    LOGGER.info("Ensuring %s selector safety for %s", stream, predicate_field)

    if len(get_predicate_field_values(selector, predicate_field)) > GOOGLE_MAX_PREDICATE_SIZE:
        LOGGER.info("Selector is unsafe: length of %s values exceeds %s", predicate_field, GOOGLE_MAX_PREDICATE_SIZE)
        return False

    page = get_page(sdk_client, selector, stream, 0)
    LOGGER.info("Total entries %s", page['totalNumEntries'])
    return page['totalNumEntries'] < GOOGLE_MAX_START_INDEX

def is_ad_group_ids_safe(sdk_client, stream, campaign_ids_selector, agids):
    selector = get_ad_group_ids_selector(campaign_ids_selector, agids)
    return is_selector_safe(sdk_client, stream, selector, 'AdGroupId')

total_num_entries_dict = {}

def get_ad_group_ids_safe_selectors(sdk_client, campaign_ids_selector, stream):
    if len(campaign_ids_selector['predicates'][0]['values']) > 1:
        raise Exception("Cannot select ad_group_ids when more than one campaign is used")

    page = get_page(sdk_client, campaign_ids_selector, stream, 0)
    total_num_entries_dict["baseCampaignId"] = page['totalNumEntries']

    ad_group_ids = list(get_selector_ids(sdk_client, 'ad_groups', campaign_ids_selector))

    for selector, success in iter_safe_selectors(get_ad_group_ids_selector(campaign_ids_selector, ad_group_ids),
                                                 'AdGroupId',
                                                 lambda agids: is_ad_group_ids_safe(sdk_client,
                                                                                    stream,
                                                                                    campaign_ids_selector,
                                                                                    agids)):
        if not success:
            raise Exception("Can't fit any partition using ad groups predicate")

        page = get_page(sdk_client, selector, stream, 0)
        total_num_entries_dict["selector_" + str(hash(str(selector)))] = page['totalNumEntries']
        yield selector

# returns starting point selectors (0th page) that need to then be
# paged through but all are safe to page through (<
# GOOGLE_MAX_START_INDEX)
def get_safe_selectors(sdk_client, campaign_ids, fields, stream):
    for campaign_id_selector, success in get_campaign_ids_safe_selectors(sdk_client, campaign_ids, fields, stream):
        if success:
            yield campaign_id_selector
        else:
            for selector in get_ad_group_ids_safe_selectors(sdk_client, campaign_id_selector, stream):
                yield selector

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

    for selector in get_safe_selectors(
            sdk_client,
            list(campaign_ids),
            field_list,
            stream):

        start_index = 0

        while True:
            page = get_page(sdk_client,
                            selector,
                            stream,
                            start_index)
            if page['totalNumEntries'] > GOOGLE_MAX_START_INDEX:
                raise Exception("Too many {} ({} > {}) for customer {}, selector {}".format(
                    stream,
                    GOOGLE_MAX_START_INDEX,
                    page['totalNumEntries'],
                    sdk_client.client_customer_id,
                    selector))
            if 'entries' in page:
                with metrics.record_counter(stream) as counter:
                    time_extracted = utils.now()

                    with Transformer(singer.UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
                        for obj in page['entries']:
                            obj['_sdc_customer_id'] = sdk_client.client_customer_id
                            bumble_bee.pre_hook = transform_pre_hook
                            record = bumble_bee.transform(obj, discovered_schema)

                            singer.write_record(stream, record, time_extracted=time_extracted)
                            counter.increment()

            start_index += PAGE_SIZE
            if start_index > int(page['totalNumEntries']):
                break
    if total_num_entries_dict.get("baseCampaignId") and total_num_entries_dict["baseCampaignId"] != sum([v for k, v in total_num_entries_dict.items() if k.startswith("selector_")]):
        sum_of_selectors = sum([v for k, v in total_num_entries_dict.items() if k.startswith("selector_")])
        LOGGER.warning("A difference was found between totalNumEntries of a search using just BaseCampaignId and one using BaseCampaignId and AdGroupId's.")
        LOGGER.warning("  BaseCampaignId: %s", total_num_entries_dict["baseCampaignId"])
        LOGGER.warning("  BaseCampaignId and AdGroupId's: %s", sum_of_selectors)

    LOGGER.info("Done syncing %s for customer_id %s", stream, sdk_client.client_customer_id)

# Will leak memory. Could provide a flush cache operation if that becomes
# an issue.
parent_account_tz_cache = {}

def get_and_cache_parent_account_tz_str(sdk_client):
    """Retrieve the timezone for the customer id associated with the
    SDK_CLIENT
    """
    if sdk_client.client_customer_id not in parent_account_tz_cache:
        selector = {'fields': ['CustomerId', 'DateTimeZone'],
                    'predicates': [
                        {'field': 'CustomerId',
                         'operator': 'IN',
                         'values': [sdk_client.client_customer_id]}
                    ],
                    # Should only ever have one result
                    'paging': {'startIndex': '0', 'numberResults': '1'}}
        results = get_page(sdk_client, selector, 'accounts', 0)
        assert results['totalNumEntries'] <= 1
        if results['totalNumEntries'] == 1:
            parent_account_tz_cache[sdk_client.client_customer_id] = results['entries'][0]['dateTimeZone']
        elif results['totalNumEntries'] == 0:
            LOGGER.warning(("Google returned an empty managed customer "
                            "result set for %s which should not happen "
                            "based on our understanding of the API. "
                            "Falling back (possibly erroneously) to UTC."),
                           sdk_client.client_customer_id)
            parent_account_tz_cache[sdk_client.client_customer_id] = 'UTC'

    return parent_account_tz_cache[sdk_client.client_customer_id]

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
    selector = {
        'fields': field_list,
        'paging': {
            'startIndex': str(start_index),
            'numberResults': str(PAGE_SIZE)
        }
    }

    while True:
        page = get_page(sdk_client, selector, stream, start_index)
        if page['totalNumEntries'] > GOOGLE_MAX_START_INDEX:
            raise Exception("Too many %s (%s > %s) for customer %s",
                            stream,
                            GOOGLE_MAX_START_INDEX,
                            page['totalNumEntries'],
                            sdk_client.client_customer_id)

        if 'entries' in page:
            with metrics.record_counter(stream) as counter:
                time_extracted = utils.now()

                with Transformer(singer.UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
                    for obj in page['entries']:
                        obj['_sdc_customer_id'] = sdk_client.client_customer_id

                        bumble_bee.pre_hook = transform_pre_hook
                        # At this point the `record` is wrong because of
                        # the comment below.
                        record = bumble_bee.transform(obj, discovered_schema)
                        # retransform `startDate` and `endDate` if this is
                        # campaigns as the transformer doesn't currently
                        # have support for dates
                        #
                        # This will cause a column split in the warehouse
                        # if we ever change to a `date` type so be wary.
                        if stream == 'campaigns':
                            # [API Docs][1] state that these fields are
                            # formatted as dates using `YYYYMMDD` and that
                            # when they're added they default to the
                            # timezone of the parent account. The
                            # description is not super clear so we're
                            # making some assumptions at this point: 1.
                            # The timezone of the parent account is the
                            # timezone that the date string is in and 2.
                            # That there's no way to to create a campaign
                            # is a different time zone (if there is we
                            # don't appear to have a way to retrieve it).
                            #
                            # We grab the parent account's timezone from
                            # the ManagedCustomerService and cast these to
                            # that timezone and then format them as if
                            # they were datetimes which is not quite
                            # accurate but is the best we can currently
                            # do.
                            #
                            # [1]: https://developers.google.com/adwords/api/docs/reference/v201809/CampaignService.Campaign#startdate
                            parent_account_tz_str = get_and_cache_parent_account_tz_str(
                                sdk_client)
                            if record.get('startDate'):
                                naive_date = datetime.datetime.strptime(
                                    obj['startDate'],
                                    '%Y%m%d')
                                utc_date = pytz.timezone(parent_account_tz_str).localize(
                                    naive_date).astimezone(tz=pytz.UTC)
                                record['startDate'] = utils.strftime(utc_date)
                            if record.get('endDate'):
                                naive_date = datetime.datetime.strptime(
                                    obj['endDate'],
                                    '%Y%m%d')
                                utc_date = pytz.timezone(parent_account_tz_str).localize(
                                    naive_date).astimezone(tz=pytz.UTC)
                                record['endDate'] = utils.strftime(utc_date)

                        singer.write_record(stream, record, time_extracted=time_extracted)
                        counter.increment()

        start_index += PAGE_SIZE
        if start_index > int(page['totalNumEntries']):
            break
    LOGGER.info("Done syncing %s for customer_id %s", stream, sdk_client.client_customer_id)

def sync_generic_endpoint(stream_name, stream_metadata, sdk_client):
    if stream_name in ('ads', 'ad_groups'):
        selector = {
            'fields': ['Id'],
            'paging': {
                'startIndex': str(0),
                'numberResults': str(PAGE_SIZE)
            }
        }
        campaign_ids = get_selector_ids(sdk_client, "campaigns", selector)

        if not campaign_ids:
            LOGGER.info("No %s for customer %s", stream_name, sdk_client.client_customer_id)
            return

        sync_campaign_ids_endpoint(sdk_client,
                                   campaign_ids,
                                   stream_name,
                                   stream_metadata)
    elif stream_name in ('campaigns', 'accounts'):
        sync_generic_basic_endpoint(sdk_client,
                                    stream_name,
                                    stream_metadata)
    else:
        raise Exception("Undefined generic endpoint %s", stream_name)

def sync_stream(stream_name, stream_metadata, sdk_client):
    # This bifurcation is real. Generic Endpoints have entirely different
    # performance characteristics and constraints than the Report
    # Endpoints and thus should be kept separate.
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
        if sdk_client.user_agent == "Stitch Tap (+support@stitchdata.com)":
            advice_message = (
                "You should correct this by going to the Stitch "
                "UI and hovering over the question mark of any excluded "
                "field and following the instructions found in the "
                "tooltip."
            )
        else:
            advice_message = (
                "You should correct it in the catalog being passed "
                "to the tap."
            )
        raise Exception(
            "Field selections violate Google's exclusion rules. {}\n\t{}".format(
                advice_message,
                "\n\t".join(errors)))

def do_discover_reports(sdk_client):
    url = 'https://adwords.google.com/api/adwords/reportdownload/{}/reportDefinition.xsd'.format(VERSION)
    xsd = request_xsd(url)
    root = ET.fromstring(xsd)
    nodes = list(root.find(".//*[@name='ReportDefinition.ReportType']/*"))

    stream_names = [p.attrib['value'] for p in nodes if p.attrib['value'] in VERIFIED_REPORTS]
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
