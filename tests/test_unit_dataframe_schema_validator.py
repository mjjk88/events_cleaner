import unittest
from unittest.mock import MagicMock
import pandas as pd
import pandas.testing
import logging

import events_cleaner.dataframe_schema_validator as dfv


class TestDataframeSchemaValidator(unittest.TestCase):

    def setUp(self):
        self.error_logger = logging.getLogger('NOOP')
        self.error_logger.info = MagicMock()

    def test_should_return_correct_record(self):
        test_data = [{"id": "9ba671d6-5154-41c1-93b1-41dd647b2d55", "created_at": "2020-01-13 05:10:12",
                      "user_email": "john@xyz.com", "ip": "204.116.116.31", "event_name": "message_saved",
                      "metadata": {"message_id": 99979}}]
        expected_df = pd.DataFrame(columns=['id', 'created_at', 'user_email', 'ip', 'event_name', 'metadata'],
                                   data=test_data)

        df = dfv.DataframeSchemaValidator(self.error_logger).validated(expected_df)

        pandas.testing.assert_frame_equal(expected_df, df)
        self.error_logger.info.assert_not_called()

    def test_should_drop_record_with_missing_id_column(self):
        test_data = [{"created_at": "2020-01-13 05:10:12",
                      "user_email": "john@xyz.com", "ip": "204.116.116.31", "event_name": "message_saved",
                      "metadata": {"message_id": 99979}}]
        test_df = pd.DataFrame(columns=['id', 'created_at', 'user_email', 'ip', 'event_name', 'metadata'],
                               data=test_data)

        df = dfv.DataframeSchemaValidator(self.error_logger).validated(test_df)

        self.assertTrue(df.empty)
        self.error_logger.info.assert_called()

    def test_should_drop_record_with_invalid_id_column(self):
        test_data = [{"id": "08700", "created_at": "2020-01-13 05:10:12",
                      "user_email": "john@xyz.com", "ip": "204.116.116.31", "event_name": "message_saved",
                      "metadata": {"message_id": 99979}}]
        test_df = pd.DataFrame(columns=['id', 'created_at', 'user_email', 'ip', 'event_name', 'metadata'],
                               data=test_data)

        df = dfv.DataframeSchemaValidator(self.error_logger).validated(test_df)

        self.assertTrue(df.empty)
        self.error_logger.info.assert_called()

    def test_should_drop_record_with_invalid_created_at_column(self):
        test_data = [{"id": "9ba671d6-5154-41c1-93b1-41dd647b2d55", "created_at": "yesterday",
                      "user_email": "john@xyz.com", "ip": "204.116.116.31", "event_name": "message_saved",
                      "metadata": {"message_id": 99979}}]
        test_df = pd.DataFrame(columns=['id', 'created_at', 'user_email', 'ip', 'event_name', 'metadata'],
                               data=test_data)

        df = dfv.DataframeSchemaValidator(self.error_logger).validated(test_df)

        self.assertTrue(df.empty)
        self.error_logger.info.assert_called()

    def test_should_drop_record_with_invalid_user_email_column(self):
        test_data = [{"id": "9ba671d6-5154-41c1-93b1-41dd647b2d55", "created_at": "2020-01-13 05:10:12",
                      "user_email": "johny_bravo", "ip": "204.116.116.31", "event_name": "message_saved",
                      "metadata": {"message_id": 99979}}]
        test_df = pd.DataFrame(columns=['id', 'created_at', 'user_email', 'ip', 'event_name', 'metadata'],
                               data=test_data)

        df = dfv.DataframeSchemaValidator(self.error_logger).validated(test_df)

        self.assertTrue(df.empty)
        self.error_logger.info.assert_called()

    def test_should_drop_record_with_invalid_ip_column(self):
        test_data = [{"id": "9ba671d6-5154-41c1-93b1-41dd647b2d55", "created_at": "2020-01-13 05:10:12",
                      "user_email": "john@xyz.com", "ip": "0-0-0", "event_name": "message_saved",
                      "metadata": {"message_id": 99979}}]
        test_df = pd.DataFrame(columns=['id', 'created_at', 'user_email', 'ip', 'event_name', 'metadata'],
                               data=test_data)

        df = dfv.DataframeSchemaValidator(self.error_logger).validated(test_df)

        self.assertTrue(df.empty)
        self.error_logger.info.assert_called()

    def test_should_drop_record_with_invalid_event_name_column(self):
        test_data = [{"id": "9ba671d6-5154-41c1-93b1-41dd647b2d55", "created_at": "2020-01-13 05:10:12",
                      "user_email": "john@xyz.com", "ip": "204.116.116.31", "event_name": ' ',
                      "metadata": {"message_id": 99979}}]
        test_df = pd.DataFrame(columns=['id', 'created_at', 'user_email', 'ip', 'event_name', 'metadata'],
                               data=test_data)

        df = dfv.DataframeSchemaValidator(self.error_logger).validated(test_df)

        self.assertTrue(df.empty)
        self.error_logger.info.assert_called()

    def test_should_drop_record_with_invalid_metadata_column(self):
        test_data = [{"id": "9ba671d6-5154-41c1-93b1-41dd647b2d55", "created_at": "2020-01-13 05:10:12",
                      "user_email": "john@xyz.com", "ip": "204.116.116.31", "event_name": "message_saved",
                      "metadata": "   "}]
        test_df = pd.DataFrame(columns=['id', 'created_at', 'user_email', 'ip', 'event_name', 'metadata'],
                               data=test_data)

        df = dfv.DataframeSchemaValidator(self.error_logger).validated(test_df)

        self.assertTrue(df.empty)
        self.error_logger.info.assert_called()


if __name__ == '__main__':
    unittest.main()
