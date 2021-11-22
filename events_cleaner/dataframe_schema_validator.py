import logging
from pandas_schema import Column, Schema
from pandas_schema.validation import DateFormatValidation, LeadingWhitespaceValidation, TrailingWhitespaceValidation, \
    MatchesPatternValidation


class DataframeSchemaValidator:
    """
    Validates dataframe schema:
        - drops and logs records which do not pass schema validation
        - return cleaned dataframe
    """

    EVENT_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    IP_REGEX = r'(?:[0-9]{1,3}\.){3}[0-9]{1,3}'
    EMAIL_REGEX = r'^[a-zA-Z0-9.!#$%&’*+/=?^_`{|}~-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*$'
    UUID_REGEX = r'[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}'

    def __init__(self, logger_events_errors):
        self.logger_events_errors = logger_events_errors
        self.logger = logging.getLogger('EVENTS CLEANER')
        self.schema = Schema([
            Column('id', [MatchesPatternValidation(DataframeSchemaValidator.UUID_REGEX)]),
            Column('created_at', [DateFormatValidation(DataframeSchemaValidator.EVENT_DATE_FORMAT)]),
            Column('user_email', [MatchesPatternValidation(DataframeSchemaValidator.EMAIL_REGEX)]),
            Column('ip', [MatchesPatternValidation(DataframeSchemaValidator.IP_REGEX)]),
            Column('event_name', [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()]),
            Column('metadata', [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()])
        ])

    def validated(self, df):
        errors = self.schema.validate(df)
        for error in errors:
            self.logger_events_errors.info("Schema validation error: %s record:\n%s", str(error), df.loc[error.row, :])

        df = df.drop(index=[e.row for e in errors])
        self.logger.info("Validated dataframe. Dropped %d records, saved %d records", len(errors), len(df))

        return df
