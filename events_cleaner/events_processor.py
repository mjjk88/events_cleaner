import logging
import time

import events_cleaner.dataframe_loader as dfl
import events_cleaner.dataframe_schema_validator as dfv
import events_cleaner.datframe_dumper as dfd


class EventsProcessor:
    """
    Cleans events in each of downloaded files.
    For each file:
        - prepares dataframe according to the schema
        - validates the data
        - logs errors to separate error log
    """

    def __init__(self, setup_logger, file_discovery):
        self.logger_app = logging.getLogger('EVENTS CLEANER')
        self.setup_logger = setup_logger
        self.file_discovery = file_discovery

    def clean_all_files(self):
        for file_definition in self.file_discovery.traversal_files():
            self._clean_file(file_definition)

    def _clean_file(self, file_definition):
        logger_events_errors = self.setup_logger('EVENTS ERRORS - ' + file_definition.filename,
                                                 file_definition.errors_file)
        start_of_load = time.time()
        self.logger_app.info('Loading events started for file: %s', file_definition.input_file)
        df = dfl.DataframeLoader(file_definition.input_file, logger_events_errors).load_dataframe()
        end_of_load = time.time()
        self.logger_app.info('Loading events finished in: %.2f s', end_of_load - start_of_load)
        df = dfv.DataframeSchemaValidator(logger_events_errors).validated(df)
        end_of_valid = time.time()
        self.logger_app.info('Validation events finished in: %.2f s', end_of_valid - end_of_load)
        self.logger_app.info('Saving cleaned %d events started for file: %s into %s', len(df),
                             file_definition.input_file,
                             file_definition.output_file)
        dfd.DataframeDumper(file_definition.output_file).dump_dataframe(df)
        end_of_dump = time.time()
        self.logger_app.info('Saving cleaned events finished in: %.2f s', end_of_dump - end_of_valid)
