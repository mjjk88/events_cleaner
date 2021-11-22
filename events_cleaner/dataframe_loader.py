import json
import logging
import pandas as pd


class DataframeLoader:
    """
    Prepares dataframe:
        - loads json events from input_file
        - drops and logs unpassable lines
        - returns cleaned dataframe
    """
    def __init__(self, input_file, logger_events_errors):
        self.input_file = input_file
        self.logger_events_errors = logger_events_errors
        self.logger = logging.getLogger('EVENTS CLEANER')

    def load_dataframe(self):
        with open(self.input_file, mode='r') as file:
            errors = []
            data = list(DataframeLoader._read_json_lines(file, errors))
            df = pd.DataFrame(columns=['id', 'created_at', 'user_email', 'ip', 'event_name', 'metadata'], data=data)
            for error in errors:
                self.logger_events_errors.info(*error)

            self.logger.info("Loaded %s. Dropped %d lines, saved %d lines", self.input_file, len(errors), len(df))

        return df

    @staticmethod
    def _read_json_lines(file, errors):
        for line in file.readlines():
            try:
                event = json.loads(line)
                yield event
            except Exception as e:
                errors.append(("JSON parsing error: %s line: %s", e, line))
