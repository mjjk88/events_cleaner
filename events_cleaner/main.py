import logging
import os
import time
from optparse import OptionParser

import events_cleaner.events_processor as ep
import events_cleaner.files_handler as efh
import events_cleaner.s3_downloader as s3d


def setup_logger(name, file_name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(file_name)
    fh.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def parse_args():
    parser = OptionParser()
    parser.add_option("-b", "--bucket", dest="bucket",
                      help="s3 source bucket")
    parser.add_option("--remote_dir", dest="remote_dir",
                      help="remote dir in s3 bucket")
    parser.add_option("--local_dir", dest="local_dir",
                      help="local directory root")

    (options, args) = parser.parse_args()

    if not options.bucket:
        parser.error("bucket is required")
    if not options.local_dir:
        parser.error("local_dir is required")
    if not options.remote_dir:
        parser.error("remote_dir is required")

    return options


if __name__ == '__main__':
    options = parse_args()

    logger_app = setup_logger('EVENTS CLEANER', os.path.join(options.local_dir, 'events_cleaner.log'))
    start = time.time()
    logger_app.info('Event cleaner started')

    try:
        file_discovery = efh.FilesHandler(options.local_dir)
        logger_app.info('Cleaning old directory structure')
        file_discovery.clean_up_old_data()

        s3d.S3Downloader(bucket=options.bucket, current_events_dir=options.remote_dir,
                         local_dir=file_discovery.input_dir).download_files()
        ep.EventsProcessor(setup_logger, file_discovery).clean_all_files()
    except:
        logger_app.exeption("Error occurs")

    end = time.time()
    logger_app.info('Total time: %.2f s', end - start)
