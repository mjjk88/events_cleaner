import os
import unittest
import boto3
from testfixtures import TempDirectory

import events_cleaner.files_handler as fh
import events_cleaner.s3_downloader as s3d


class TestS3Downloader(unittest.TestCase):
    """
    This is an example of integration tests we could write to test integration with S3.
    It will NOT work now as it requires proper configuration (e.g. having test bucket).
    I decided to leave it as the example of what should be included in real testing flow.
    """

    def setUp(self):
        self.s3 = boto3.resource('s3')
        self.test_bucket = 'test_bucket'
        self.test_vents_dir = 'test_vents_dir'

    def test_should_download_file_from_test_bucket(self):
        test_file_name_input = 'test_file.json'
        with TempDirectory() as d:
            file_discovery = fh.FilesHandler(d.path)
            file_discovery.clean_up_old_data()

            file_to_test = os.path.join(file_discovery.input_dir, test_file_name_input)
            d.write(file_to_test, b"""{"id": "1da671d6-5154-41c1-93b1-41dd647b2d67", "created_at": "2021-01-18 05:10:12", "user_email": "nkowalsky@kmt.com", "ip": "105.189.122.52", "event_name": "message_saved", "metadata": {"message_id": 45674567}}
{"id": "572279cc-4b5d-4a43-a3ec-aa52a8f87e69", "created_at": "2021-01-18 02:31:30", {"id": "19cd6e40-cab8-532d-9g62-a8466761e307", "created_at": "2020-01-12 13:28:31", "user_email": "bbryan@mklkp.net", "ip": "177.77.22.188", "event_name": "meeting_scheduled", "metadata": {"meeting_id": "eafb8c22-66ed-44aa-99fb-7389330fbff95", "meeting_time": "2021-02-20 11:00:00"}}
{"id": " ", "created_at": " ", "user_email": "kucma.com", "ip": "", "event_name": "   ", "metadata": {   }}
{"id": "9ba671d6-5154-41c1-93b1-41dd647b2d55", "created_at": "dadaadaddd", "user_email": "johnywalker@zsad.com", "ip": "809.128.189.33", "event_name": "message_saved", "metadata": {"message_id": 45678}}"""
                    )

            self.s3.upload_file(file_to_test, self.test_bucket, self.test_vents_dir)

            s3d.S3Downloader(bucket=self.test_bucket, current_events_dir=self.test_vents_dir,
                             local_dir=file_discovery.input_dir).download_files()
            downloaded_file = os.path.join(file_discovery.input_dir, test_file_name_input)

            self.assertEqual(downloaded_file, file_to_test)


if __name__ == '__main__':
    unittest.main()
