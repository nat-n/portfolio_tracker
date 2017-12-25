import math
from datetime import datetime, date, timedelta
import pandas as pd
from pymongo import UpdateOne
from .helpers import log_client_fetch_error, asset_record_keys, \
    asset_columns_map


class CacheClient:
    CURRENCY_SIG_FIG = 5

    def __init__(self, db):
        self.asset_prices = db.asset_prices
        self.currency_rates = db.currency_rates

    def put_asset_prices(
            self, source, symbol, price_data, start_date, end_date):
        records = self.asset_prices_to_cache_records(
            source, symbol, price_data, start_date, end_date)
        self.upsert_records(self.asset_prices, records)

    def get_asset_prices(self, symbol, start_date=None, end_date=None):
        query = {'symbol': symbol}
        try:
            if start_date is not None or end_date is not None:
                query['date'] = {}
                if start_date is not None:
                    query['date']['$gte'] = start_date
                if end_date is not None:
                    query['date']['$lte'] = end_date
            return self.create_asset_prices_data_frame(
                self.asset_prices.find(query, {'_id': 0})
            )
        except Exception:
            log_client_fetch_error('cache', symbol, start_date, end_date)
            return self.create_asset_prices_data_frame([])

    def put_currency_rates(self, source, base_currency, rate_data):
        records = self.currency_rates_to_cache_records(
            source, base_currency, rate_data)
        self.upsert_records(self.currency_rates, records)

    def get_currency_rates(
            self, base_currency, other_currency, start_date, end_date):
        symbols = sorted([base_currency, other_currency])
        query = {'symbols': symbols}
        try:
            if start_date is not None or end_date is not None:
                query['date'] = {}
                if start_date is not None:
                    query['date']['$gte'] = start_date
                if end_date is not None:
                    query['date']['$lte'] = end_date
            return self.create_currency_rates_data_frame(
                other_currency,
                self.currency_rates.find(query, {'_id': 0})
            )
        except Exception:
            log_client_fetch_error('cache', symbols, start_date, end_date)
            return self.create_currency_rates_data_frame(other_currency, [])

    @classmethod
    def upsert_records(self, collection, records):
        # TODO: put this in a background thread
        operations = []
        for record in records:
            query = {'date': record['date']}
            if 'symbol' in record:
                query['symbol'] = record['symbol']
            if 'symbols' in record:
                query['symbols'] = record['symbols']
            # avoid overwritting a day that isn't missing with one that is
            if record['_placeholer']:
                query['_placeholer'] = True
            operations.append(UpdateOne(query, {'$set': record}, upsert=True))
        collection.bulk_write(operations, ordered=False)

    @classmethod
    def format_rate_and_inverse(self, rate):
        float_parse_template = '%.' + str(self.CURRENCY_SIG_FIG) + 'g'
        return [float(float_parse_template % n) for n in [rate, 1 / rate]]

    @classmethod
    def currency_rates_to_cache_records(
            self, source, base_currency, rate_data):
        records = []
        for other_currency, rates in rate_data.iteritems():
            if base_currency < other_currency:
                symbols = [base_currency, other_currency]
                reverse_order = False
            else:
                symbols = [other_currency, base_currency]
                reverse_order = True
            for exchange_date, rate in rates.iteritems():
                if not math.isnan(rate):
                    [rate, inverse] = self.format_rate_and_inverse(rate)
                    if reverse_order:
                        [rate, inverse] = [inverse, rate]
                    records.append({
                        'date': exchange_date,
                        'symbols': symbols,
                        'rate': rate,
                        'inverse': inverse,
                        "_source": source,
                        "_placeholer": False
                    })
        return records

    @staticmethod
    def create_asset_prices_data_frame(records):
        result = pd.DataFrame(list(records), columns=asset_record_keys)
        result.rename(columns=asset_columns_map, inplace=True)
        result.set_index('Date', inplace=True)
        return result

    @staticmethod
    def create_currency_rates_data_frame(symbol, records):
        rows = [{
            'Date': r['date'],
            symbol: r['rate'] if r['symbols'][1] == symbol else r['inverse']
        } for r in records]
        result = pd.DataFrame(rows, columns=['Date', symbol])
        result.set_index('Date', inplace=True)
        return result

    @staticmethod
    def asset_prices_to_cache_records(
            source, symbol, price_data, start_date, end_date):
        today = datetime(date.today().year,
                         date.today().month,
                         date.today().day)
        if start_date is None:
            start_date = price_data.index[0]
        if end_date is None:
            end_date = price_data.index[-1]
        if end_date >= today:
            end_date = today - timedelta(days=1)
        num_days = (end_date - start_date).days + 1

        def sanitize_float(value):
            value = float(value)
            if math.isnan(value):
                return None
            return value

        def sanitize_int(value):
            try:
                return int(value)
            except TypeError:
                return None

        def create_record(row_date):
            try:
                row = price_data.loc[row_date]
                return {
                    'symbol': symbol,
                    'date': row_date,
                    'open': sanitize_float(row['Open']),
                    'high': sanitize_float(row['High']),
                    'low': sanitize_float(row['Low']),
                    'close': sanitize_float(row['Close']),
                    'volume': sanitize_int(row['Volume']),
                    '_source': source,
                    '_placeholer': False
                }
            except (KeyError, AttributeError):
                return {
                    'symbol': symbol,
                    'date': row_date,
                    'open': None,
                    'high': None,
                    'low': None,
                    'close': None,
                    'volume': None,
                    '_source': source,
                    '_placeholer': True
                }

        return (
            create_record(row_date) for row_date in (
                start_date + timedelta(days=i) for i in range(0, num_days)
            )
        )
