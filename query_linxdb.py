import cx_Oracle

__author__ = "Daniel Bicho"
__email__ = "daniel.bicho@fccn.pt"

connection = cx_Oracle.connect('linxs/linxs@p24.arquivo.pt:1522/linxsdb')
cursor = connection.cursor()

# Aux SQL queries
# "SELECT original FROM ARTIGO WHERE ROWNUM <= 10"
# "SELECT * FROM User_Tables"
# "SELECT * FROM NLS_DATABASE_PARAMETERS"
# "SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME='ARTIGO'
# "SELECT * FROM (SELECT * FROM ARTIGO ORDER BY ID DESC) WHERE ROWNUM = 1"
# Execute Query to extract needed information from LinxsDB

cursor.execute("SELECT URL,DATA_INDEX,ID FROM ARTIGO ORDER BY DATA_INDEX DESC")
for row in cursor:
    print row
