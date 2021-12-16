import unittest
from unittest.mock import Mock, patch
import tap_adwords
from requests.exceptions import Timeout, ConnectionError

@patch("time.sleep")
class TestRequestTimeoutBackoff(unittest.TestCase):
    """A set of unit tests to ensure that tap retry 5 times on timeout exception."""
    
    def test_backoff_on_get_report_definition_service(self, mock_sleep):
        """When Timeout error is returned in get_report_definition_service, we expect that it retry 5 times."""
        report_definition_service = Mock()
        mocked_sdk_client = Mock()
        mocked_sdk_client.GetService = Mock(return_value=report_definition_service)
        report_definition_service.getReportFields = Mock(side_effect=Timeout)

        try:
            tap_adwords.get_report_definition_service('', mocked_sdk_client)
        except Timeout:
            pass

        # Verify that report_definition_service.getReportFields is called 5 times
        self.assertEquals(5, report_definition_service.getReportFields.call_count)

    def test_backof_on_attempt_get_from_service(self, mock_sleep):  
        """When Timeout error is returned in attempt_get_from_service, we expect that it retry 5 times."""
        mocked_service_caller = Mock()
        mocked_service_caller.get = Mock(side_effect=Timeout)
        mocked_service_caller.zeep_client.wsdl.services = {"key1": "a", "key2": "b"}

        try:
            tap_adwords.attempt_get_from_service(mocked_service_caller, "")
        except Timeout:
            pass

        # Verify that report_definition_service.getReportFields is called 5 times
        self.assertEquals(5, mocked_service_caller.get.call_count)

    def test_backof_on_attempt_download_report(self, mock_sleep):
        """When Timeout error is returned in attempt_download_report, we expect that it retry 5 times."""
        mocked_report_downloader = Mock()
        mocked_report_downloader.DownloadReportAsStream = Mock(side_effect=Timeout)

        try:
            tap_adwords.attempt_download_report(mocked_report_downloader, "")
        except Timeout:
            pass

        self.assertEquals(5, mocked_report_downloader.DownloadReportAsStream.call_count)

    @patch('requests.Session.send', side_effect = Timeout)
    @patch("requests.Request.prepare")
    def test_backof_on_request_xsd(self, mocked_prepare, mocked_send, mock_sleep):
        """When Timeout error is returned in request_xsd, we expect that it retry 5 times."""

        try:
            tap_adwords.request_xsd("")
        except Timeout:
            pass

        self.assertEquals(5, mocked_send.call_count)

@patch("time.sleep")
class TestConnectionErrorBackoff(unittest.TestCase):
    """A set of unit tests to ensure that tap backoff 5 times on ConnectionError exception."""
    
    def test_backoff_on_get_report_definition_service(self, mock_sleep):
        """When ConnectionError error is returned in get_report_definition_service, we expect that it retry 5 times."""
        report_definition_service = Mock()
        mocked_sdk_client = Mock()
        mocked_sdk_client.GetService = Mock(return_value=report_definition_service)
        report_definition_service.getReportFields = Mock(side_effect=ConnectionError)

        try:
            tap_adwords.get_report_definition_service('', mocked_sdk_client)
        except ConnectionError:
            pass

        # Verify that report_definition_service.getReportFields is called 5 times
        self.assertEquals(5, report_definition_service.getReportFields.call_count)

    def test_backof_on_attempt_get_from_service(self, mock_sleep):  
        """When ConnectionError error is returned in attempt_get_from_service, we expect that it retry 5 times."""
        mocked_service_caller = Mock()
        mocked_service_caller.get = Mock(side_effect=ConnectionError)
        mocked_service_caller.zeep_client.wsdl.services = {"key1": "a", "key2": "b"}

        try:
            tap_adwords.attempt_get_from_service(mocked_service_caller, "")
        except ConnectionError:
            pass

        # Verify that report_definition_service.getReportFields is called 5 times
        self.assertEquals(5, mocked_service_caller.get.call_count)

    def test_backof_on_attempt_download_report(self, mock_sleep):
        """When ConnectionError error is returned in attempt_download_report, we expect that it retry 5 times."""
        mocked_report_downloader = Mock()
        mocked_report_downloader.DownloadReportAsStream = Mock(side_effect=ConnectionError)

        try:
            tap_adwords.attempt_download_report(mocked_report_downloader, "")
        except ConnectionError:
            pass

        self.assertEquals(5, mocked_report_downloader.DownloadReportAsStream.call_count)

    @patch('requests.Session.send', side_effect = ConnectionError)
    @patch("requests.Request.prepare")
    def test_backof_on_request_xsd(self, mocked_prepare, mocked_send, mock_sleep):
        """When ConnectionError error is returned in request_xsd, we expect that it retry 5 times."""

        try:
            tap_adwords.request_xsd("")
        except ConnectionError:
            pass

        self.assertEquals(5, mocked_send.call_count)
        
class TestRequestTimeoutValue(unittest.TestCase):
    def test_integer_request_timeout_in_config(self):
        """
            Verify that if request_timeout is provided in config(integer value) then it should be use
        """
        tap_adwords.CONFIG.update({"request_timeout": 100}) # integer timeout in config

        request_timeout = tap_adwords.get_request_timeout()

        # If none zero positive integer or string value passed in the config then it converted to float value. So, here we are verifying the same.
        self.assertEqual(request_timeout, 100.0) # Verify timeout value

    def test_float_request_timeout_in_config(self):
        """
            Verify that if request_timeout is provided in config(float value) then it should be use
        """
        tap_adwords.CONFIG.update({"request_timeout": 100.5}) # float timeout in config

        request_timeout = tap_adwords.get_request_timeout()

        self.assertEqual(request_timeout, 100.5) # Verify timeout value

    def test_string_request_timeout_in_config(self):
        """
            Verify that if request_timeout is provided in config(string value) then it should be use
        """
        tap_adwords.CONFIG.update({"request_timeout": "100"}) # string format timeout in config

        request_timeout = tap_adwords.get_request_timeout()

        # If none zero positive integer or string value passed in the config then it converted to float value. So, here we are verifying the same.
        self.assertEqual(request_timeout, 100.0) # Verify timeout value

    def test_empty_string_request_timeout_in_config(self):
        """
            Verify that if request_timeout is provided in config with empty string then default value is used
        """
        tap_adwords.CONFIG.update({"request_timeout": ""}) # empty string in config

        request_timeout = tap_adwords.get_request_timeout()

        self.assertEqual(request_timeout, 300) # Verify timeout value

    def test_zero_request_timeout_in_config(self):
        """
            Verify that if request_timeout is provided in config with zero value then default value is used
        """
        tap_adwords.CONFIG.update({"request_timeout": 0}) # zero value in config

        request_timeout = tap_adwords.get_request_timeout()

        self.assertEqual(request_timeout, 300) # Verify timeout value

    def test_zero_string_request_timeout_in_config(self):
        """
            Verify that if request_timeout is provided in config with zero in string format then default value is used
        """
        tap_adwords.CONFIG.update({"request_timeout": '0'}) # zero value in config

        request_timeout = tap_adwords.get_request_timeout()

        self.assertEqual(request_timeout, 300) # Verify timeout value

    def test_no_request_timeout_in_config(self):
        """
            Verify that if request_timeout is not provided in config then default value is used
        """
        tap_adwords.CONFIG = {}
        request_timeout = tap_adwords.get_request_timeout()

        self.assertEqual(request_timeout, 300) # Verify timeout value
