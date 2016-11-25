import os

import chardet
import cx_Oracle

os.environ["NLS_LANG"] = ".UTF8"

__author__ = "Daniel Bicho"
__email__ = "daniel.bicho@fccn.pt"

connection = cx_Oracle.connect('<connection string>')
cursor = connection.cursor()

# Aux SQL queries
# "SELECT original FROM ARTIGO WHERE ROWNUM <= 10"
# "SELECT * FROM User_Tables"
# "SELECT * FROM NLS_DATABASE_PARAMETERS"
# "SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME='ARTIGO'
# "SELECT * FROM (SELECT * FROM ARTIGO ORDER BY ID DESC) WHERE ROWNUM = 1"
# Execute Query to extract needed information from LinxsDB

cursor.execute("SELECT URL,ORIGINAL,DATA_INDEX,ID FROM ARTIGO WHERE ID = '10901411' ORDER BY DATA_INDEX DESC")
for row in cursor:
    content = row[1].read()
    a = chardet.detect(content)
    print row[2].strftime('%Y%m%d%H%M%S')
    print content
