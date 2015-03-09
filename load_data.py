from datetime import datetime, time

import pandas as pd
import numpy as np

import psycopg2 as pg

CRIME_DATE_FORMAT = '%m/%d/%Y %H:%M:%S %p'

create_schemas_query = """
CREATE TABLE crimes (
  IncidntNum int,
  Category varchar,
  Descript varchar,
  DayOfWeek varchar,
  Date timestamp,
  Time time,
  PdDistrict varchar,
  Resolution varchar,
  Address varchar,
  X double precision,
  Y double precision,
  Location varchar
);

CREATE TABLE investments (
  "company_permalink" VARCHAR,
  "company_name" VARCHAR,
  "company_category_list" VARCHAR,
  "company_market" VARCHAR,
  "company_country_code" VARCHAR,
  "company_state_code" VARCHAR,
  "company_region" VARCHAR,
  "company_city" VARCHAR,
  "investor_permalink" VARCHAR,
  "investor_name" VARCHAR,
  "investor_category_list" VARCHAR,
  "investor_market" VARCHAR,
  "investor_country_code" VARCHAR,
  "investor_state_code" VARCHAR,
  "investor_region" VARCHAR,
  "investor_city" VARCHAR,
  "funding_round_permalink" VARCHAR,
  "funding_round_type" VARCHAR,
  "funding_round_code" VARCHAR,
  "funded_at" DATE,
  "funded_month" VARCHAR,
  "funded_quarter" VARCHAR,
  "funded_year" INTEGER,
  "raised_amount_usd" DOUBLE PRECISION
);

CREATE TABLE companies (
  "permalink" VARCHAR,
  "name" VARCHAR,
  "homepage_url" VARCHAR,
  "category_list" VARCHAR,
  "market" VARCHAR,
  "funding_total_usd" DOUBLE PRECISION,
  "status" VARCHAR,
  "country_code" VARCHAR,
  "state_code" VARCHAR,
  "region" VARCHAR,
  "city" VARCHAR,
  "funding_rounds" INTEGER,
  "founded_at" DATE,
  "founded_month" VARCHAR,
  "founded_quarter" VARCHAR,
  "founded_year" REAL,
  "first_funding_at" VARCHAR,
  "last_funding_at" VARCHAR
);

CREATE TABLE rounds (
  "company_permalink" VARCHAR,
  "company_name" VARCHAR,
  "company_category_list" VARCHAR,
  "company_market" VARCHAR,
  "company_country_code" VARCHAR,
  "company_state_code" VARCHAR,
  "company_region" VARCHAR,
  "company_city" VARCHAR,
  "funding_round_permalink" VARCHAR,
  "funding_round_type" VARCHAR,
  "funding_round_code" VARCHAR,
  "funded_at" DATE,
  "funded_month" VARCHAR,
  "funded_quarter" VARCHAR,
  "funded_year" INTEGER,
  "raised_amount_usd" DOUBLE PRECISION
);

CREATE TABLE acquisitions (
  "company_permalink" VARCHAR,
  "company_name" VARCHAR,
  "company_category_list" VARCHAR,
  "company_market" VARCHAR,
  "company_country_code" VARCHAR,
  "company_state_code" VARCHAR,
  "company_region" VARCHAR,
  "company_city" VARCHAR,
  "acquirer_permalink" VARCHAR,
  "acquirer_name" VARCHAR,
  "acquirer_category_list" VARCHAR,
  "acquirer_market" VARCHAR,
  "acquirer_country_code" VARCHAR,
  "acquirer_state_code" VARCHAR,
  "acquirer_region" VARCHAR,
  "acquirer_city" VARCHAR,
  "acquired_at" DATE,
  "acquired_month" VARCHAR,
  "acquired_quarter" VARCHAR,
  "acquired_year" INTEGER,
  "price_amount" DOUBLE PRECISION,
  "price_currency_code" VARCHAR
);
"""

def ingest_csv(con, tablename, filename, ncols, batch_preparer,
               batchsize=1000, header=True):
    import csv
    f = open(filename, 'rb')
    reader = csv.reader(f)

    insert_stmt = ('INSERT INTO {} VALUES ({});'
                   .format(tablename, ', '.join(['%s'] * ncols)))
    batch = []
    def _write_batch():
        tuples = batch_preparer(batch)
        cursor = con.cursor()
        try:
            cursor.executemany(insert_stmt, tuples)
            con.commit()
        except Exception:
            con.rollback()
            raise

    total = 0
    for i, line in enumerate(reader):
        if header and i == 0:
            continue
        batch.append(line[:ncols])
        if len(batch) == batchsize:
            _write_batch()
            batch = []
            total += batchsize
            print 'wrote %d rows' % total

    # last batch
    _write_batch()


def pg_connect():
    return pg.connect(host='localhost', port=5432,
                      user='wesm', password='foo',
                      database='strata_2015')


def drop_schemas(con):
    query = """
DROP TABLE IF EXISTS companies;
DROP TABLE IF EXISTS acquisitions;
DROP TABLE IF EXISTS investments;
DROP TABLE IF EXISTS rounds;
DROP TABLE IF EXISTS crimes;
    """
    cur = con.cursor()
    cur.execute(query)
    con.commit()

def create_schemas(con):
    cur = con.cursor()
    cur.execute(create_schemas_query)
    con.commit()

con = pg_connect()
drop_schemas(con)
create_schemas(con)

def try_int(x):
    try:
        return int(x)
    except (ValueError, TypeError):
        return None

def try_(f):
    def g(x):
        try:
            return f(x)
        except (ValueError, TypeError):
            return None
    return g

def parse_datetime(fmt):
    def f(x):
        try:
            return datetime.strptime(x, fmt)
        except (ValueError, TypeError):
            return None
    return f

def number_with_comma(x):
    try:
        x = x.replace(',', '')
        return float(x)
    except (ValueError, AttributeError):
        return None


def empty_as_null(x):
    return x if len(x) > 0 else None


def generic_prepare(converters):
    def prepare(rows):
        prepared = []

        for row in rows:
            row = [empty_as_null(x) for x in row]
            for i, converter in converters:
                row[i] = converter(row[i])
            prepared.append(row)
        return prepared
    return prepare

crime_converters = [
    (0, try_(int)),
    (4, parse_datetime(CRIME_DATE_FORMAT)),
    (9, try_(float)),
    (10, try_(float))
]
ingest_csv(con, 'crimes', 'sf_crimes.csv', 12,
           generic_prepare(crime_converters))

CRUNCHBASE_DATE_FORMAT = '%Y-%m-%d'
parse_cb_date = parse_datetime(CRUNCHBASE_DATE_FORMAT)


investments_converters = [
    (19, parse_cb_date),
    (23, number_with_comma)
]
ingest_csv(con, 'investments', 'investments.csv', 24,
           generic_prepare(investments_converters))

companies_converters = [
    (5, number_with_comma), # funding_total_usd
    (12, parse_cb_date), # founded_at
]
ingest_csv(con, 'companies', 'companies.csv', 18,
           generic_prepare(companies_converters))

rounds_converters = [
    (11, parse_cb_date),
    (15, number_with_comma)
]
ingest_csv(con, 'rounds', 'rounds.csv', 16,
           generic_prepare(rounds_converters))

acquisitions_converters = [
    (16, parse_cb_date),
    (20, number_with_comma)
]
ingest_csv(con, 'acquisitions', 'acquisitions.csv', 22,
           generic_prepare(acquisitions_converters))


def bad_values(series, converter):
    bad_data = []
    for x in series:
        try:
            converter(x)
        except (TypeError, ValueError):
            bad_data.append(x)
    return bad_data

def null_on_error(series, converter):
    def f(x):
        try:
            return converter(x)
        except (TypeError, ValueError):
            return np.nan
    return series.map(f)


def to_time(x):
    h, m = x.split(':')
    return time(int(h), int(m))

def cleaning_pandas_some():
    crimes = pd.read_csv('sf_crimes.csv')
    crimes.Date = pd.to_datetime(crimes.Date, format=CRIME_DATE_FORMAT)
    crimes.Time = crimes.Time.map(to_time)

    companies = pd.read_csv('companies.csv',
                            parse_dates=['founded_at', 'first_funding_at'])
    companies['funding_total_usd'] = null_on_error(
        companies.pop(' funding_total_usd '), number_with_comma)

    investments = pd.read_csv('investments.csv')
    acquisitions = pd.read_csv('acquisitions.csv')
    rounds = pd.read_csv('rounds.csv')

    return investments, acquisitions, rounds
