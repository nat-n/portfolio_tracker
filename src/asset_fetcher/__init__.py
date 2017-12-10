import math
from datetime import datetime, date, timedelta
from .clients import cache_client, quandl_client, tradier_client, ft_client
import pandas as pd
from config import config


class AssetFetcher:
    def parse_date(self, d):
        if isinstance(d, datetime):
            return d
        return datetime.strptime(d, "%Y-%m-%d")

    def strip_empty_rows(self, df):
        return df.dropna(how='all')

    def fetch_history(self, symbol, start_date, end_date=date.today()):
        start_date = self.parse_date(start_date)
        end_date = self.parse_date(end_date)
        num_days = (end_date - start_date).days + 1
        required_range = pd.date_range(start_date, periods=num_days)

        from_cache = cache_client.get(symbol, start_date, end_date)
        missing_dates = required_range.difference(from_cache.index)
        today = datetime(date.today().year,
                         date.today().month,
                         date.today().day)

        if missing_dates[missing_dates < today].empty:
            return self.strip_empty_rows(from_cache)

        missing_start = missing_dates.min()
        missing_end = missing_dates.max()

        return pd.concat([
            self.strip_empty_rows(from_cache.loc[:missing_start]),
            self.fetch_history_from_sources_and_cache(
                symbol, missing_start, missing_end),
            self.strip_empty_rows(from_cache.loc[missing_end:])
        ])

    def fetch_history_from_sources_and_cache(self, symbol, start_date, end_date=date.today().isoformat()):
        # TODO: make this smarter
        result = quandl_client.fetch_history(symbol, start_date, end_date)
        if result is not None:
            self.cache_history('quandl', symbol, result, start_date, end_date)
        if result is None:
            result = tradier_client.fetch_history(symbol, start_date, end_date)
            if result is not None:
                self.cache_history('tradier', symbol, result,
                                   start_date, end_date)
        if result is None:
            result = ft_client.fetch_history(symbol, start_date, end_date)
            if result is not None:
                self.cache_history('ft', symbol, result, start_date, end_date)
        if result is None:
            print('Couldn\'t find ' + symbol + 'anywhere!')
        return result

    def cache_history(self, source, symbol, df, start_date=None, end_date=None):
        records = self.as_cache_records(
            source, symbol, df, start_date, end_date)
        cache_client.put_many(records)

    def as_cache_records(self, source, symbol, df, start_date=None, end_date=None):
        today = datetime(date.today().year,
                         date.today().month,
                         date.today().day)
        if start_date is None:
            start_date = df.index[0]
        if end_date is None:
            end_date = df.index[-1]
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
                row = df.loc[row_date]
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
