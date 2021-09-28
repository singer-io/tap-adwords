import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import datetime
import unittest
from functools import reduce
from singer import utils
from singer import metadata
import datetime
import json
from dateutil.relativedelta import *
import pdb

class AdwordsBookmarks(unittest.TestCase):
    def setUp(self):
        missing_envs = [x for x in [os.getenv('TAP_ADWORDS_DEVELOPER_TOKEN'),
                                    os.getenv('TAP_ADWORDS_OAUTH_CLIENT_ID'),
                                    os.getenv('TAP_ADWORDS_OAUTH_CLIENT_SECRET'),
                                    os.getenv('TAP_ADWORDS_REFRESH_TOKEN'),
                                    os.getenv('TAP_ADWORDS_CUSTOMER_IDS')] if x == None]
        if len(missing_envs) != 0:
            #pylint: disable=line-too-long
            raise Exception("set TAP_ADWORDS_DEVELOPER_TOKEN, TAP_ADWORDS_OAUTH_CLIENT_ID, TAP_ADWORDS_OAUTH_CLIENT_SECRET, TAP_ADWORDS_REFRESH_TOKEN, TAP_ADWORDS_CUSTOMER_IDS")

    def name(self):
        return "tap_tester_adwords_bookmarks_1"

    def tap_name(self):
        return "tap-adwords"

    def get_type(self):
        return "platform.adwords"

    def get_credentials(self):
        return {'developer_token': os.getenv('TAP_ADWORDS_DEVELOPER_TOKEN'),
                'oauth_client_id': os.getenv('TAP_ADWORDS_OAUTH_CLIENT_ID'),
                'oauth_client_secret': os.getenv('TAP_ADWORDS_OAUTH_CLIENT_SECRET'),
                'refresh_token':     os.getenv('TAP_ADWORDS_REFRESH_TOKEN')}

    def expected_pks(self):
        return {
            'ADGROUP_PERFORMANCE_REPORT' : {},
            'AD_PERFORMANCE_REPORT' : {},
            'CAMPAIGN_PERFORMANCE_REPORT' : {},
            'CLICK_PERFORMANCE_REPORT' : {},
            'CRITERIA_PERFORMANCE_REPORT' : {},
            'GEO_PERFORMANCE_REPORT' : {},
            'KEYWORDS_PERFORMANCE_REPORT' : {},
            'SEARCH_QUERY_PERFORMANCE_REPORT' : {},
            'GENDER_PERFORMANCE_REPORT' : {},
            'AGE_RANGE_PERFORMANCE_REPORT': {},
            'AUDIENCE_PERFORMANCE_REPORT': {},
            'KEYWORDLESS_QUERY_REPORT': {},
            # 'CREATIVE_CONVERSION_REPORT' : {},
            'FINAL_URL_REPORT': {},
            'accounts' : {"customerId"},
            'ads' : {"adGroupId"},
            'ad_groups' : {"id"},
            'campaigns' : {"id"}
        }


    def expected_check_streams(self):
        return {
            'accounts',
            'ads',
            'ad_groups',
            'campaigns',
            'ADGROUP_PERFORMANCE_REPORT',
            'AD_PERFORMANCE_REPORT',
            'CAMPAIGN_PERFORMANCE_REPORT',
            'CLICK_PERFORMANCE_REPORT',
            'CRITERIA_PERFORMANCE_REPORT',
            'GEO_PERFORMANCE_REPORT',
            'KEYWORDS_PERFORMANCE_REPORT',
            'SEARCH_QUERY_PERFORMANCE_REPORT',
            'GENDER_PERFORMANCE_REPORT',
            'AGE_RANGE_PERFORMANCE_REPORT',
            'AUDIENCE_PERFORMANCE_REPORT',
            # 'CREATIVE_CONVERSION_REPORT',
            'FINAL_URL_REPORT',
            'PLACEMENT_PERFORMANCE_REPORT',
            'KEYWORDLESS_QUERY_REPORT',
            'PLACEHOLDER_FEED_ITEM_REPORT',
            'ACCOUNT_PERFORMANCE_REPORT',
            'SHOPPING_PERFORMANCE_REPORT',
            'VIDEO_PERFORMANCE_REPORT',
            'DISPLAY_KEYWORD_PERFORMANCE_REPORT',
            'CALL_METRICS_CALL_DETAILS_REPORT',
            'DISPLAY_TOPICS_PERFORMANCE_REPORT',
            'PLACEHOLDER_REPORT',
            }

    def expected_sync_streams(self):
        return {
            'ADGROUP_PERFORMANCE_REPORT',
            'AD_PERFORMANCE_REPORT',
            'AGE_RANGE_PERFORMANCE_REPORT',
            'CAMPAIGN_PERFORMANCE_REPORT',
            #'CLICK_PERFORMANCE_REPORT',
            'CRITERIA_PERFORMANCE_REPORT',
            'GEO_PERFORMANCE_REPORT',
            'KEYWORDS_PERFORMANCE_REPORT',
            #'SEARCH_QUERY_PERFORMANCE_REPORT',
            'accounts',
            'ads',
            'ad_groups',
            'campaigns',
            'GENDER_PERFORMANCE_REPORT',
            'AUDIENCE_PERFORMANCE_REPORT',
            #'KEYWORDLESS_QUERY_REPORT',
            # 'CREATIVE_CONVERSION_REPORT'
            'FINAL_URL_REPORT'
        }

    def record_to_bk_value(self, stream, record):
        if stream == 'companies':
            properties = record.get('properties')
            if properties is None:
                return None
            bk_value = properties.get('hs_lastmodifieddate', properties.get('createdate'))
            if bk_value is None:
                return None
            return utils.strftime(datetime.datetime.fromtimestamp(float(bk_value['timestamp']) / 1000.0, datetime.timezone.utc))


    def bookmark_name_from_stream_and_customer_id(self, stream, customer_id):
        return "{}_{}".format(stream, customer_id)

    def expected_bookmarks(self):
        return {'ADGROUP_PERFORMANCE_REPORT' : ['date'],
                'AD_PERFORMANCE_REPORT' : ['date'],
                'CAMPAIGN_PERFORMANCE_REPORT' : ['date'],
                'CLICK_PERFORMANCE_REPORT' : ['date'],
                'CRITERIA_PERFORMANCE_REPORT' : ['date'],
                'GEO_PERFORMANCE_REPORT' : ['date'],
                'KEYWORDS_PERFORMANCE_REPORT' : ['date'],
                'SEARCH_QUERY_PERFORMANCE_REPORT' : ['date'],
                'GENDER_PERFORMANCE_REPORT' : ['date'],
                'AGE_RANGE_PERFORMANCE_REPORT' : ['date'],
                'AUDIENCE_PERFORMANCE_REPORT' : ['date'],
                'KEYWORDLESS_QUERY_REPORT' : ['date'],
            # 'CREATIVE_CONVERSION_REPORT'
                # 'CREATIVE_CONVERSION_REPORT' : ['date'],
                'FINAL_URL_REPORT' : ['date'],
                'accounts' : [],
                'ads' : [],
                'ad_groups' : [],
                'campaigns' : []
        }

    def get_properties(self):
        #our test data is on the 9/15
        return {'start_date':   '2018-04-12T00:00:00Z',
                'end_date':     '2018-04-15T00:00:00Z',
                'conversion_window_days' : '-1',
                'user_id':      'not used?',
                'customer_ids': os.getenv('TAP_ADWORDS_CUSTOMER_IDS')}

    def select_all_fields_except(self, blacklisted_fields, schema, md):
        md =  metadata.write(md, (), 'selected', True)
        for p in schema['properties'].keys():
            if p not in blacklisted_fields and p != 'day':
                md =  metadata.write(md, ('properties', p), 'selected', True)

        return md

    def perform_field_select(self, conn_id, catalog):
        annotated_stream = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
        schema = annotated_stream['annotated-schema']
        md = {}

        if catalog['tap_stream_id'] == 'GEO_PERFORMANCE_REPORT':
            md = self.select_all_fields_except({'conversionCategory',
                                                'conversionTrackerId',
                                                'conversionName',
                                                'conversionSource',
                                                'daysToConversion'},
                                               schema,
                                               md)

        if catalog['tap_stream_id'] == 'KEYWORDS_PERFORMANCE_REPORT':
            md = self.select_all_fields_except({'clickType',
                                                'daysToConversion',
                                                'conversionCategory',
                                                'conversionName',
                                                'conversionSource',
                                                'conversionTrackerId',
                                                'device',
                                                'network',
                                                'networkWithSearchPartners',
                                                'topVsOther',
                                                'conversionAdjustment',
                                                'daysToConversionOrAdjustment',},
                                               schema,
                                               md)

        if catalog['tap_stream_id'] == 'accounts':
            md = self.select_all_fields_except({'customerId'}, schema, md)

        if catalog['tap_stream_id'] == 'ads':
            md = self.select_all_fields_except({'adGroupid'}, schema, md)

        if catalog['tap_stream_id'] == 'ad_groups':
            md = self.select_all_fields_except({'id'}, schema, md)

        if catalog['tap_stream_id'] == 'campaigns':
            md = self.select_all_fields_except({'id'}, schema, md)

        if catalog['tap_stream_id'] == 'AD_PERFORMANCE_REPORT':
            md =  self.select_all_fields_except({'conversionCategory',
                                                 'conversionTrackerId',
                                                 'conversionName',
                                                 'conversionSource',
                                                 'clickType',
                                                 'daysToConversion',
                                                 'device',
                                                 'topVsOther',
                                                 'conversionAdjustment',
                                                 'daysToConversionOrAdjustment'},
                                                schema,
                                                md)

        if catalog['tap_stream_id'] == 'ADGROUP_PERFORMANCE_REPORT':
            md = self.select_all_fields_except({'conversionCategory',
                                                'conversionTrackerId',
                                                'conversionName',
                                                'conversionSource',
                                                'clickType',
                                                'daysToConversion',
                                                'device',
                                                'topVsOther',
                                                'hourOfDay',
                                                'conversionAdjustment',
                                                'daysToConversionOrAdjustment'},
                                               schema,
                                               md)

        if catalog['tap_stream_id'] == 'SEARCH_QUERY_PERFORMANCE_REPORT':
            md =  self.select_all_fields_except({'conversionCategory',
                                                 'conversionTrackerId',
                                                 'conversionName',
                                                 'conversionSource',
                                                 'daysToConversion'},
                                                schema,
                                                md)

        if catalog['tap_stream_id'] == 'KEYWORDLESS_QUERY_REPORT':
            md = self.select_all_fields_except({ 'conversionCategory',
                                                 'conversionCategory',
                                                 'conversionTrackerId',
                                                 'conversionName',
                                                 'conversionSource'},
                                               schema,
                                               md)

        if catalog['tap_stream_id'] == 'CAMPAIGN_PERFORMANCE_REPORT':
            md = self.select_all_fields_except({'conversionCategory',
                                                'conversionTrackerId',
                                                'conversionName',
                                                'conversionSource',
                                                'clickType',
                                                'device',
                                                'topVsOther',
                                                'hourOfDay',
                                                'avgImprFreqPerCookie',
                                                'uniqueCookies',
                                                'daysToConversion',
                                                'conversionAdjustment',
                                                'daysToConversionOrAdjustment',
                                                'adEventType'},
                                               schema,
                                               md)

        if catalog['tap_stream_id'] == 'CLICK_PERFORMANCE_REPORT':
            md = self.select_all_fields_except({},
                                               schema,
                                               md)

        if catalog['tap_stream_id'] == 'CRITERIA_PERFORMANCE_REPORT':
            md = self.select_all_fields_except({'conversionCategory',
                                                'conversionTrackerId',
                                                'conversionName',
                                                'conversionSource',
                                                'clickType',
                                                'topVsOther',
                                                'daysToConversion',
                                                'conversionAdjustment',
                                                'daysToConversionOrAdjustment'},
                                               schema,
                                               md)

        if catalog['tap_stream_id'] == 'GENDER_PERFORMANCE_REPORT':
            md =  self.select_all_fields_except({'conversionCategory',
                                                 'conversionTrackerId',
                                                 'conversionName',
                                                 'conversionSource',
                                                 'clickType',
                                                 'daysToConversion'},
                                                schema,
                                                md)

        if catalog['tap_stream_id'] == 'AGE_RANGE_PERFORMANCE_REPORT':
            md =  self.select_all_fields_except({'conversionCategory',
                                                 'conversionTrackerId',
                                                 'conversionName',
                                                 'conversionSource',
                                                 'clickType',
                                                 'daysToConversion' },
                                                schema,
                                                md)

        if catalog['tap_stream_id'] == 'AUDIENCE_PERFORMANCE_REPORT':
            md =  self.select_all_fields_except({'topVsOther',
                                                 'conversionCategory',
                                                 'conversionTrackerId',
                                                 'conversionName',
                                                 'conversionSource',
                                                 'clickType',
                                                 'daysToConversion'},
                                                schema,
                                                md)

        if catalog['tap_stream_id'] == 'FINAL_URL_REPORT':
            md =  self.select_all_fields_except({'topVsOther',
                                                 'conversionCategory',
                                                 'conversionTrackerId',
                                                 'conversionName',
                                                 'conversionSource',
                                                 'clickType',
                                                 'daysToConversion'},
                                                schema,
                                                md)

        # if catalog['tap_stream_id'] == 'CREATIVE_CONVERSION_REPORT':
        #     md =  self.select_all_fields_except({'topVsOther',
        #                                          'conversionCategory',
        #                                          'conversionTrackerId',
        #                                          'conversionName',
        #                                          'conversionSource',
        #                                          'clickType'},
        #                                         schema,
        #                                         md)

        return {'key_properties' :     catalog.get('key_properties'),
                'schema' :             schema,
                'tap_stream_id':       catalog.get('tap_stream_id'),
                'replication_method' : catalog.get('replication_method'),
                'replication_key'    : catalog.get('replication_key'),
                'metadata'           : metadata.to_list(md)}

    def verify_day_column(self):
        synced_records = runner.get_records_from_target_output()
        for stream in self.expected_sync_streams():
            for message in synced_records[stream]['messages']:
                if message['action'] == 'upsert' and stream not in {'accounts', 'ads', 'campaigns', 'ad_groups'}:
                    self.assertIsNotNone(message['data'].get('day'))

    def verify_synthetic_columns(self):
        our_ccids = set(os.getenv('TAP_ADWORDS_CUSTOMER_IDS').split(","))
        synced_records = runner.get_records_from_target_output()
        for stream in self.expected_sync_streams():
            for message in synced_records[stream]['messages']:
                if message['action'] == 'upsert':
                    self.assertIn(message.get('data').get('_sdc_customer_id'), our_ccids)
                    if stream in {'accounts', 'ads', 'campaigns', 'ad_groups'}:
                        self.assertIsNone(message.get('data').get('_sdc_report_datetime'))
                    else:
                        self.assertIsNotNone(message.get('data').get('_sdc_report_datetime'))


    def test_run(self):
        conn_id = connections.ensure_connection(self)

        #run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)

        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))

        diff = self.expected_check_streams().symmetric_difference( found_catalog_names )

        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are kosher")

        #select all catalogs
        #NB> only selected support catalogs
        menagerie.set_state(conn_id, {})
        found_catalogs = [x for x in found_catalogs if x['stream_name'] in self.expected_sync_streams()]
        selected_catalogs = list(map( lambda catalog: self.perform_field_select(conn_id, catalog), found_catalogs))

        # verify a few of the metadata for the
        # KEYWORDS_PERFORMANCE_REPORT
        keyword_performance_report_catalog = [fc for
                                              fc in found_catalogs
                                              if fc['stream_name'] == 'KEYWORDS_PERFORMANCE_REPORT'][0]
        annotated_schema = menagerie.get_annotated_schema(conn_id, keyword_performance_report_catalog['stream_id'])

        mdata = metadata.to_map(annotated_schema['metadata'])

        # ATTRIBUTE breadcrumb
        self.assertEqual('ATTRIBUTE',
                         metadata.get(mdata, ('properties', 'adGroup'), 'behavior'))
        self.assertEqual('AdGroupName',
                         metadata.get(mdata, ('properties', 'adGroup'), 'adwords.fieldName'))
        # SEGMENT breadcrumb
        self.assertEqual('SEGMENT',
                         metadata.get(mdata, ('properties', 'conversionSource'), 'behavior'))
        # METRIC breadcrumb
        self.assertEqual('METRIC',
                         metadata.get(mdata, ('properties', 'expectedClickthroughRateHist'), 'behavior'))

        #automatic day inclusion
        self.assertEqual('automatic',
                         metadata.get(mdata, ('properties', 'day'), 'inclusion'))

        # fieldExclusions
        self.assertEqual(
            [['properties', 'conversionAdjustment'],
             ['properties', 'daysToConversionOrAdjustment'],
             ['properties', 'conversionCategory'],
             ['properties', 'daysToConversion'],
             ['properties', 'conversionTrackerId'],
             ['properties', 'conversionName'],
             ['properties', 'conversionSource']],
            metadata.get(mdata, ('properties', 'interactionTypes'), 'fieldExclusions'))

        stream_metadata = [{'tap_stream_id': x['tap_stream_id'], 'metadata': x['metadata']} for x in selected_catalogs]

        menagerie.put_non_discoverable_metadata(conn_id, stream_metadata)

        #clear state
        sync_job_name = runner.run_sync_mode(self, conn_id)

        #verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))


        self.verify_synthetic_columns()
        self.verify_day_column()

        state = menagerie.get_state(conn_id)
        bookmarks = state.get('bookmarks')
        bookmark_streams = set(state.get('bookmarks').keys())

        start_of_end_day =  '2018-04-15 00:00:00'

        #verify bookmarks
        for k,v in sorted(list(self.expected_bookmarks().items())):
            if k not in self.expected_sync_streams() or record_count_by_stream.get(k) is None:
                continue

            for w in v:
                for cid in os.getenv('TAP_ADWORDS_CUSTOMER_IDS').split(","):
                    bk_key = self.bookmark_name_from_stream_and_customer_id(k, cid)
                    bk_value = bookmarks.get(bk_key,{}).get(w)
                    self.assertIsNotNone(bk_value)
                    self.assertEqual(utils.strptime_with_tz(bk_value), utils.strptime_with_tz(start_of_end_day), "Bookmark {} for stream {} should have been updated to {}".format(bk_value, k, start_of_end_day))
                    print("bookmark {}({}) updated to {} from max record value {}".format(bk_key, w, bk_value,  start_of_end_day))


        acceptable_bookmarks = [self.bookmark_name_from_stream_and_customer_id(s,c) for s in self.expected_sync_streams() for c in  os.getenv('TAP_ADWORDS_CUSTOMER_IDS').split(",")]

        diff = bookmark_streams.difference(set(acceptable_bookmarks))
        self.assertEqual(len(diff), 0, msg="Unexpected bookmarks: {} Expected: {} Actual: {}".format(diff, acceptable_bookmarks, bookmarks))

        self.assertEqual(state.get('currently_syncing'), None,"Unexpected `currently_syncing` bookmark value: {} Expected: None".format(state.get('currently_syncing')))
