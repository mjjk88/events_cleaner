import logging
import os
import unittest
from unittest.mock import MagicMock

import pandas as pd
import pandas.testing
from testfixtures import TempDirectory

import events_cleaner.dataframe_loader as dfl


class TestDataframeLoader(unittest.TestCase):

    def setUp(self):
        self.error_logger = logging.getLogger('NOOP')
        self.error_logger.info = MagicMock()

    def test_should_load_correct_row(self):
        test_file_name = 'test_correct_event.json'
        with TempDirectory() as d:
            file_to_test = os.path.join(d.path, test_file_name)
            d.write(test_file_name, b"""{"id": "1da671d6-5154-41c1-93b1-41dd647b2d67", "created_at": "2021-01-18 05:10:12", "user_email": "nkowalsky@kmt.com", "ip": "105.189.122.52", "event_name": "message_saved", "metadata": {"message_id": 45674567}}""")
            expected_data = [{"id": "1da671d6-5154-41c1-93b1-41dd647b2d67", "created_at": "2021-01-18 05:10:12",
                              "user_email": "nkowalsky@kmt.com", "ip": "105.189.122.52",
                              "event_name": "message_saved",
                              "metadata": {"message_id": 45674567}}]
            expected_df = pd.DataFrame(columns=['id', 'created_at', 'user_email', 'ip', 'event_name', 'metadata'],
                                       data=expected_data)

            df = dfl.DataframeLoader(file_to_test, self.error_logger).load_dataframe()

            pandas.testing.assert_frame_equal(expected_df, df)
            self.error_logger.info.assert_not_called()

    def test_should_delete_incorrect_row(self):
        test_file_name = 'test_incorrect_event.json'
        with TempDirectory() as d:
            file_to_test = os.path.join(d.path, test_file_name)
            d.write(test_file_name, b"""{"id": "572279cc-4b5d-4a43-a3ec-aa52a8f87e69", "created_at": "2021-01-18 02:31:30", {"id": "19cd6e40-cab8-532d-9g62-a8466761e307", "created_at": "2020-01-12 13:28:31", "user_email": "bbryan@mklkp.net", "ip": "177.77.22.188", "event_name": "meeting_scheduled", "metadata": {"meeting_id": "eafb8c22-66ed-44aa-99fb-7389330fbff95", "meeting_time": "2021-02-20 11:00:00"}}""")
            df = dfl.DataframeLoader(file_to_test, self.error_logger).load_dataframe()

            self.assertTrue(df.empty)
            self.error_logger.info.assert_called()


if __name__ == '__main__':
    unittest.main()
