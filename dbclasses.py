from sqlalchemy import Column, Integer, Date, String, ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


'''
defined tables for the database: slr
'''

Base = declarative_base()

class ScopusEntry(Base):
    # Tell SQLAlchemy what the table name is and if there's any table-specific arguments it should know about
    __tablename__ = 'literature'
    __table_args__ = {'extend_existing': True}
    # tell SQLAlchemy the name of column and its attributes:
    eid = Column(String, primary_key=True, nullable=False)
    doi = Column(String)
    title = Column(String)
    abstract = Column(String, default=None)
    keywords = Column(String, default=None)
    subtype = Column(String)
    date = Column(Date)
    author = Column(String)
    openaccess = Column(Boolean)
    publicationname = Column(String)
    paperurl = Column(String)
    abstracturl = Column(String)
    request = Column(String)
    source = Column(String)
    searchfield = Column(String)
    query = Column(String)
    sdg = Column(String)
    decision = Column(String, default=None)

    def __init__(self, eid, doi, title, subtype, date, author, openaccess, publicationname, paperurl, abstracturl, request, source, searchfield, query, sdg, abstract=None, keywords=None, decision=None):
        self.eid = eid
        self.doi = doi
        self.title = title
        self.abstract = abstract
        self.keywords = keywords
        self.subtype = subtype
        self.date = date
        self.author = author
        self.openaccess = openaccess
        self.publicationname = publicationname
        self.paperurl = paperurl
        self.abstracturl = abstracturl
        self.request = request
        self.source = source
        self.searchfield = searchfield
        self.query = query
        self.sdg = sdg
        self.decision = decision


def connect_to_db():
    # Create the database
    with open('./database.txt', 'rt') as f:
        conn_string = f.readline()
    engine = create_engine(conn_string)
    Base.metadata.create_all(engine)

    # Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    return s