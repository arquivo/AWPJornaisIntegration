from warc import ARCFile
from warc.gzip2 import GzipFile

__author__ = "Daniel Bicho"
__email__ = "daniel.bicho@fccn.pt"


# Extend ARCFile functionality to write gzipped arc files
class ARCFile2(ARCFile):
    def __init__(self, filename=None, mode=None, fileobj=None, version=None, file_headers={}):
        super(ARCFile2, self).__init__(filename, mode, fileobj, version, file_headers)

    def write(self, arc_record):
        "Writes out the given arc record to the file"
        if not self.version:
            self.version = 2
        if not self.header_written:
            self.header_written = True
            self._write_header()

            # gzip member arc header
            self.fileobj.close_member()
        arc_record.write_to(self.fileobj, self.version)
        self.fileobj.write("\n")  # Record separator

        if isinstance(self.fileobj, GzipFile):
            self.fileobj.close_member()
