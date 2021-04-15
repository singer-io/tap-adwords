import unittest
from unittest.mock import Mock, patch
from tap_adwords import GoogleAdsServerFault, AdWordsReportBadRequestError
from tap_adwords import get_report_definition_service
from tap_adwords import attempt_get_from_service
from tap_adwords import attempt_download_report

class TestCommonExceptionHandling(unittest.TestCase):
    """A set of unit tests to ensure that we handle the most common exceptions correctly."""
    def test_get_report_definition_service_raise(self):
        """When GoogleAdsServerFault is returned in get_report_definition_service, we expect the following custom error message"""
        report_definition_service = Mock()
        mocked_sdk_client = Mock()
        mocked_sdk_client.GetService = Mock(return_value=report_definition_service)
        report_definition_service.getReportFields = Mock(side_effect=GoogleAdsServerFault(
            "",
            errors=[],
            message="[AuthorizationError.CUSTOMER_NOT_ACTIVE @ ; trigger:'<null>']"
        ))

        with self.assertRaisesRegexp(Exception, "Customer Not Active AuthorizationError. This usually means that you haven't been active on your account for 15 months.") as e:
            get_report_definition_service('', mocked_sdk_client)
        self.assertEquals(1, report_definition_service.getReportFields.call_count)

    def test_get_report_definition_service_raise_for_other_GoogleAdsServerFault(self):
        """When GoogleAdsServerFault is returned in report_definition_service without a 'customer not active' message, we expect to not see a custom error message."""
        report_definition_service = Mock()
        mocked_sdk_client = Mock()
        mocked_sdk_client.GetService = Mock(return_value=report_definition_service)
        report_definition_service.getReportFields = Mock(side_effect=GoogleAdsServerFault(
            "",
            errors=[],
            message="this is a non 'customer not active' authorization GoogleAdsServerFault"
        ))

        with self.assertRaisesRegexp(Exception, "this is a non 'customer not active' authorization GoogleAdsServerFault") as e:
            get_report_definition_service('', mocked_sdk_client)
        self.assertEquals(1, report_definition_service.getReportFields.call_count)

    def test_get_report_definition_service_normal_exception(self):
        """When we see an exception other than GoogleAdsServerFault in get_report_definition_service, we expect that error to be raised with no special handling"""
        report_definition_service = Mock()
        mocked_sdk_client = Mock()
        mocked_sdk_client.GetService = Mock(return_value=report_definition_service)
        report_definition_service.getReportFields = Mock(side_effect=Exception(
            "this is a normal exception"
        ))

        with self.assertRaisesRegexp(Exception, "this is a normal exception") as e:
            get_report_definition_service('', mocked_sdk_client)
        self.assertEquals(1, report_definition_service.getReportFields.call_count)



    def test_attempt_get_from_service_raise(self):
        """When GoogleAdsServerFault is returned in attempt_get_from_service, we expect the following custom error message.
        We also expect that it will not retry"""
        mocked_service_caller = Mock()
        mocked_service_caller.get = Mock(side_effect=GoogleAdsServerFault(
            "",
            errors=[],
            message="[AuthorizationError.CUSTOMER_NOT_ACTIVE @ ; trigger:'<null>']"
        ))
        mocked_service_caller.zeep_client.wsdl.services = {"key1": "a", "key2": "b"}

        with self.assertRaisesRegexp(Exception, "Customer Not Active AuthorizationError. This usually means that you haven't been active on your account for 15 months.") as e:
            attempt_get_from_service(mocked_service_caller, "")
        self.assertEquals(1, mocked_service_caller.get.call_count)

    def test_attempt_get_from_service_raise_for_other_GoogleAdsServerFault(self):
        """When GoogleAdsServerFault is returned in attempt_get_from_service without a 'customer not active' message, we expect to not see a custom error message.
        We also expect that it will retry 3 times"""
        mocked_service_caller = Mock()
        mocked_service_caller.get = Mock(side_effect=GoogleAdsServerFault(
            "",
            errors=[],
            message="this is a non 'customer not active' authorization GoogleAdsServerFault"
        ))
        mocked_service_caller.zeep_client.wsdl.services = {"key1": "a", "key2": "b"}

        with self.assertRaisesRegexp(Exception, "this is a non 'customer not active' authorization GoogleAdsServerFault") as e:
            attempt_get_from_service(mocked_service_caller, "")
        self.assertEquals(3, mocked_service_caller.get.call_count)

    def test_attempt_get_from_service_normal_exception_raise_with_retry(self):
        """When we see an exception other than GoogleAdsServerFault in attempt_get_from_service, we expect that error to be raised with no special handling.
        We also expect that it will retry 3 times"""
        mocked_service_caller = Mock()
        mocked_service_caller.get = Mock(side_effect=Exception(
            "this exception type should be retried"
        ))
        mocked_service_caller.zeep_client.wsdl.services = {"key1": "a", "key2": "b"}

        with self.assertRaisesRegexp(Exception, "this exception type should be retried") as e:
            attempt_get_from_service(mocked_service_caller, "")
        self.assertEquals(3, mocked_service_caller.get.call_count)


    def test_attempt_download_report_raise(self):
        """When AdWordsReportBadRequestError is returned in attempt_download_report, we expect the following custom error message.
        We also expect that it will retry 3 times"""
        mocked_report_downloader = Mock()
        mocked_report_downloader.DownloadReportAsStream = Mock(side_effect=AdWordsReportBadRequestError(
            "RateExceededError.RATE_EXCEEDED",
            'Basic Access Daily Reporting Quota',
            'None'"",
            "",
            "",
            ""
        ))

        with self.assertRaisesRegexp(Exception, "Rate Exceeded Error. Too many requests were made to the API in a short period of time.") as e:
            attempt_download_report(mocked_report_downloader, "")
        self.assertEquals(3, mocked_report_downloader.DownloadReportAsStream.call_count)

    def test_attempt_download_report_raise_for_other_AdWordsReportBadRequestError(self):
        """When AdWordsReportBadRequestError is returned without 'rate exceeded' message in attempt_download_report, we expect to not see a custom error message.
        We also expect that it will retry 3 times"""
        mocked_report_downloader = Mock()
        mocked_report_downloader.DownloadReportAsStream = Mock(side_effect=AdWordsReportBadRequestError(
            "this is a non 'rate exceeded' AdWordsReportBadRequestError",
            'Basic Access Daily Reporting Quota',
            'None'"",
            "",
            "",
            ""
        ))

        with self.assertRaisesRegexp(Exception, "this is a non 'rate exceeded' AdWordsReportBadRequestError") as e:
            attempt_download_report(mocked_report_downloader, "")
        self.assertEquals(3, mocked_report_downloader.DownloadReportAsStream.call_count)

    def test_attempt_download_report_raise_normal_exception(self):
        """When we see an exception other than AdwordsReportBadRequestError in attempt_download_report, we expect that error to be raised with no special handling
        We also expect that it will retry 3 times"""
        mocked_report_downloader = Mock()
        mocked_report_downloader.DownloadReportAsStream = Mock(side_effect=Exception(
            "this is a normal exception"
        ))

        with self.assertRaisesRegexp(Exception, "this is a normal exception") as e:
            attempt_download_report(mocked_report_downloader, "")
        self.assertEquals(3, mocked_report_downloader.DownloadReportAsStream.call_count)
