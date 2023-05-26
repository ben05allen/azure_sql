import os
from azure.identity import AzureCliCredential
import struct
import pyodbc
from dotenv import load_dotenv


load_dotenv()
SQL_COPT_SS_ACCESS_TOKEN = 1256 # https://learn.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute?view=sql-server-ver16#sql_copt_ss_access_token
driver = 'ODBC Driver 18 for SQL Server'
server = f'tcp:{os.getenv("SERVER")}.database.windows.net,1433'
database = os.getenv('DATABASE')

credential = AzureCliCredential()

# db_token is a named tuple
db_token = credential.get_token('https://database.windows.net/.default')

# first element is the access token
token_bytes = bytes(db_token[0], 'utf-8')

# fit msodbcsql types to pyodbc by inserting extra bytes, see the following repo;
# https://github.com/felipefandrade/azuresqlspn
exp_token = b''
for i in token_bytes:
    exp_token += bytes({i}) + bytes(1)
token_struct = struct.pack("=i", len(exp_token)) + exp_token

odbc_cxn = f"Driver={driver};Server={server};Database={database}"
conn = pyodbc.connect(odbc_cxn, attrs_before = {SQL_COPT_SS_ACCESS_TOKEN:token_struct})

query_string = "SELECT * FROM dbo.fruits"
cursor = conn.cursor()
cursor.execute(query_string)
for row in cursor.fetchall():
    print(*row)
