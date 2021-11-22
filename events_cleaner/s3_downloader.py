import logging
import os
import time

import boto3


class S3Downloader:
    """
    Downloads all files from directory in the bucket
    """

    def __init__(self, bucket, current_events_dir, local_dir):

        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(bucket)

        self.local_dir = local_dir
        self.s3_directory = current_events_dir
        self.logger = logging.getLogger('EVENTS CLEANER')

    def download_files(self):
        start_of_download = time.time()

        files_objects = self.bucket.objects.filter(Prefix=self.s3_directory)
        files_objects = filter(lambda x: x.key != self.s3_directory, files_objects)

        for obj in files_objects:
            local_file = os.path.join(self.local_dir, os.path.basename(obj.key))
            self.bucket.download_file(obj.key, local_file)
            self.logger.info('%s downloaded. File size: %.2f MB', os.path.basename(obj.key),
                             os.stat(local_file).st_size/1024.0/1024.0)

        end_of_download = time.time()
        self.logger.info('Downloading events files finished in: %.2f s', end_of_download - start_of_download)
