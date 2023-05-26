import os
from azure.identity import AzureCliCredential
import struct
from typing_extensions import Annotated
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Mapped, mapped_column
from urllib.parse import quote_plus
from dotenv import load_dotenv


load_dotenv()
# magic numbers
SQL_COPT_SS_ACCESS_TOKEN = 1256 
driver = 'ODBC Driver 18 for SQL Server'
server = f'tcp:{os.getenv("SERVER")}.database.windows.net,1433'
database = os.getenv('DATABASE')

class Base(DeclarativeBase):
    pass


int_pk = Annotated[int, mapped_column(primary_key=True)]


class Fruit(Base):
    __tablename__ = 'fruits'

    id: Mapped[int_pk]
    name: Mapped[str]
    count: Mapped[int]


fruit_data = [
        # {'name':'Apple', 'count':10},
        # {'name':'Banana', 'count':15},
        # {'name':'Orange', 'count':12},
        {'name':'Kiwifruit', 'count':2},
        ]



credential = AzureCliCredential()
db_token = credential.get_token('https://database.windows.net/.default')

# fit msodbcsql types to pyodbc by inserting extra bytes, see the following repo;
# https://github.com/felipefandrade/azuresqlspn
token_bytes = bytes(db_token[0], 'utf-8')
exp_token = b''
for i in token_bytes:
    exp_token += bytes({i}) + bytes(1)
token_struct = struct.pack("=i", len(exp_token)) + exp_token

odbc_cxn = f'Driver={driver};Server={server};Database={database}'
params = quote_plus(odbc_cxn)

engine = create_engine(f'mssql+pyodbc:///?odbc_connect={quote_plus(odbc_cxn)}', connect_args={'attrs_before': {SQL_COPT_SS_ACCESS_TOKEN:token_struct}})
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
with Session.begin() as local_session:
    for fruit in fruit_data:
        fruit_obj = Fruit(**fruit)
        local_session.add(fruit_obj)