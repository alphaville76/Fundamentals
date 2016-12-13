from contextlib import contextmanager
import csv
import datetime
from model import *
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import dateutil.relativedelta
from model import Base, fundamentals, Equity


@contextmanager
def db_session():
    #engine = create_engine('sqlite:///resource/fundamentals.sqlite')
    engine = create_engine('mysql://root@localhost/fundamentals')
    db_session = sessionmaker()
    db_session.configure(bind=engine)
    Base.metadata.create_all(engine)
    s = db_session()
    try:
        yield s
        s.commit()
    except:
        s.rollback()
        raise
    finally:
        s.close()


def query(*args):
    with db_session() as s:
        q = s.query(fundamentals.symbol).join(fundamentals.balance_sheet, fundamentals.income_statement,
                                              fundamentals.asset_classification, \
                                              fundamentals.cash_flow_statement, fundamentals.company_reference,
                                              fundamentals.earnings_ratios, \
                                              fundamentals.earnings_report, fundamentals.financial_statement_filing,
                                              fundamentals.general_profile, \
                                              fundamentals.operation_ratios, fundamentals.share_class_reference,
                                              fundamentals.valuation, fundamentals.valuation_ratios).add_columns(
            *args)
    return q

def get_fundamentals(query, date = datetime.date.today()):
    with db_session() as s:
        dmin = date - dateutil.relativedelta.relativedelta(months=3)
        dmax = date - dateutil.relativedelta.relativedelta(days=1)
        q = query.filter(fundamentals.date >= dmin).filter(fundamentals.date <= dmax)
        df = pd.read_sql(q.statement, s.bind)
        df.index = df['symbol']
        del df['symbol']
    return df.T


def _to_date(str):
        if str == 'None':
            return None

            return datetime.strptime(str, "%Y-%m-%d").date()


def insert_equities():
    with open('resource/tickers.txt', 'rb') as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t')
        with db_session() as s:
            s.query(TickerChangeDate).delete()
            s.query(RelatedTicker).delete()
            s.query(Equity).delete()
            for row in reader:
                e = Equity()
                e.id = row['Perma Ticker']
                e.ticker = row['Ticker']
                e.name = row['Name']
                e.cusip = row['CUSIP']
                e.fama_industry = row['Fama Industry']
                e.currency = row['Currency']
                e.sector = row['Sector']
                e.industry = row['Industry']
                e.last_updated = _to_date(row['Last Updated'])
                e.prior_tickers = row['Prior Tickers']

                for date in row['Ticker Change Date'].split(','):
                    if date == 'None':
                        break
                    cd = TickerChangeDate()
                    cd.date = _to_date(row['Ticker Change Date'])
                    e.ticker_change_date.append(cd)

                for related_ticker in row['Related Tickers'].split(','):
                    if related_ticker == 'None':
                        break
                    rt = RelatedTicker()
                    rt.ticker = related_ticker
                    e.related_tickers.append(rt)

                e.exchange = row['Exchange']
                e.sic = row['SIC']
                e.location = row['Location']
                e.delisted_from = row['Delisted From']
                e.is_foreign = row['Is Foreign'] == 'Y'

                s.add(e)