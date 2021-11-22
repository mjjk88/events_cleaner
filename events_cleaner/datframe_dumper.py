class DataframeDumper:
    """
    Saves dataframe as json lines
    """

    def __init__(self, output_file):
        self.output_file = output_file

    def dump_dataframe(self, df):
        df.to_json(self.output_file, orient='records', lines=True)
