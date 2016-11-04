import datetime
import os

from warc.gzip2 import GzipFile
from arcfile2 import ARCFile2

__author__ = "Daniel Bicho"
__email__ = "daniel.bicho@fccn.pt"


class Write2Arc(object):

    def __create_arc(self):
        self.arc_filename = '{}-{}-{}.arc.gz'.format(self.filename, datetime.datetime.utcnow().strftime(
            '%Y%m%d%H%M%S'), str(self._number_arcs).zfill(5))

        self.f = GzipFile(self.arc_filename, mode='wb')
        self.arc_file = ARCFile2(file_headers={'ip_address': '0.0.0.0',
                                              'date': datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S'),
                                              'org': 'Arquivo.pt'}, fileobj = self.f, version = 1)

    def __init__(self, filename, max_size):
        self._number_arcs = 0
        self.filename = filename
        self.max_size = max_size
        self.__create_arc()


    def write_record(self, arc_record):
        self.arc_file.write(arc_record)
        size = os.path.getsize(self.arc_filename)
        if size >= self.max_size:
            self.arc_file.close()
            self._number_arcs += 1

            # rodar arc
            self.__create_arc()