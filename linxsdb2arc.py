import logging
import re
import os

import cx_Oracle
import warc

from write2arc import Write2Arc

__author__ = "Daniel Bicho"
__email__ = "daniel.bicho@fccn.pt"

os.environ["NLS_LANG"] = ".UTF8"

def compute_content_length(payload):
    content_length = 0
    count = False
    for text in payload.split('\r\n\r\n'):
        if count:
            # Splitting method deletes the \r\n\r\n that must be accounted on the content-lenght total.
            # So we need to add them to the content-lenght sum.
            # # TODO reforge with better code
            content_length += len(text.encode('utf-8')) + len('\r\n\r\n'.encode('utf-8'))
        count = True
    return content_length - len('\r\n\r\n'.encode('utf-8'))


def main():
    # set logging level
    logging.basicConfig(filename='linxdb2arc.log', levelname=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.setLevel('DEBUG')

    # Connection to Oracle Database where LinxsDB is located
    connection = cx_Oracle.connect('linxs/linxs@p24.arquivo.pt:1522/linxsdb')
    cursor = connection.cursor()

    # Init object responsable to write to arc file
    arc = Write2Arc('AWP-JornaisPT', 100000000)

    # Execute Query to extract needed information from LinxsDB
    cursor.execute("SELECT URL,DATA_INDEX,ORIGINAL,ID FROM ARTIGO ORDER BY DATA_INDEX")
    for row in cursor:
        record = {'URL': row[0], 'TIME': row[1].strftime('%Y%m%d%H%M%S'), 'PAYLOAD': row[2].read()}

        # If payload is empty dont write this record
        if record['PAYLOAD'] == '':
            continue

        logger.info("RECORD WRITE {} {}".format(row[3], record['URL']))
        logger.debug("PAYLOAD {}".format(record['PAYLOAD'][0:20]))

        # Validate PAYLOAD
        valid_payload = re.search(r"^HTTP/\d.\d \d{3}", record['PAYLOAD'])
        if valid_payload:
            # Set content - type
            search_type = re.search(r"[cC]ontent-[tT]ype: (\w+/\w+)", record['PAYLOAD'])
            if search_type:
                content_type = search_type.group(1)
            else:
                content_type = 'text/html'

            # Put together in a string the ARC header record and ARC record payload
            record_header = '{} {} {} {} {}'.format(record['URL'], '1.1.1.1', record['TIME'], content_type,
                                                len(record['PAYLOAD']))

            # Fix bad formed headers record (multiple white spaces, beginning white spaces)
            record_header = re.sub('\s+', ' ', record_header).strip()

            logger.info('RECORD HEADER - {}'.format(record_header))

            # Generate a ARCRecord object and write to ARC File
            arc_record = warc.ARCRecord.from_string(record_header + '\n' + record['PAYLOAD'], 1)
            arc.write_record(arc_record)
        else:
            logger.info("INVALID PAYLOAD {} {}".format(row[3], record['URL']))
    # Clean up DB connection
    cursor.close()
    connection.close()


if __name__ == '__main__':
    main()
