import os
from typing_extensions import Annotated
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Mapped, mapped_column
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()
driver = 'ODBC Driver 18 for SQL Server'
server = f'tcp:{os.getenv("SERVER")}.database.windows.net,1433'
database = os.getenv('DATABASE')
authentication = 'ActiveDirectoryMsi'

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


odbc_cxn = f"Driver={driver};Server={server};Database={database};Authentication={authentication}"
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={quote_plus(odbc_cxn)}')
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
with Session.begin() as local_session:
    for fruit in fruit_data:
        fruit_obj = Fruit(**fruit)
        local_session.add(fruit_obj)