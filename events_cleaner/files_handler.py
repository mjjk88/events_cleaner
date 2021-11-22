import os
import shutil
from dataclasses import dataclass


@dataclass
class FilesLocation:
    filename: str
    input_file: str
    output_file: str
    errors_file: str


class FilesHandler:
    """
    Cleans and prepares files directory structure:
    ${ROOT}/input/${FILENAME} <-- input file
    ${ROOT}/output/${FILENAME} <-- cleaned up input file
    ${ROOT}/errors/${FILE_NAME}-errors.log <-- error log
    """

    def __init__(self, root):
        self.root = root
        self.input_dir = os.path.join(self.root, 'input')
        self.output_dir = os.path.join(self.root, 'output')
        self.errors_dir = os.path.join(self.root, 'errors')

    def clean_up_old_data(self):
        shutil.rmtree(self.input_dir, ignore_errors=True, onerror=None)
        shutil.rmtree(self.output_dir, ignore_errors=True, onerror=None)
        shutil.rmtree(self.errors_dir, ignore_errors=True, onerror=None)
        self._prepare_file_structure()

    def _prepare_file_structure(self):
        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.errors_dir, exist_ok=True)

    def traversal_files(self):
        for filename in os.listdir(self.input_dir):
            yield FilesLocation(filename,
                                os.path.join(self.input_dir, filename),
                                os.path.join(self.output_dir, filename),
                                os.path.join(self.errors_dir, filename + '-errors.log')
                                )
