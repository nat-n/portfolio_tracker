from datetime import datetime, date
from .clients import ClientProxy, cache_client
import pandas as pd
from config import config


class PriceFetcher:
    def parse_date(self, d):
        if isinstance(d, datetime):
            return d
        if isinstance(d, date):
            return datetime.fromordinal(d.toordinal())
        return datetime.strptime(d, "%Y-%m-%d")

    def strip_empty_rows(self, df):
        return df.dropna(how='all')

    def fetch_asset_prices(self, symbol, start_date, end_date=date.today()):
        start_date = self.parse_date(start_date)
        end_date = self.parse_date(end_date)
        num_days = (end_date - start_date).days + 1
        required_range = pd.date_range(start_date, periods=num_days)

        from_cache = cache_client.get_asset_prices(
            symbol, start_date, end_date)
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
            ClientProxy.get_asset_price_history(symbol, start_date, end_date),
            self.strip_empty_rows(from_cache.loc[missing_end:])
        ])

    def fetch_currency_rates(self, base_currency, other_currency, start_date, end_date):
        start_date = self.parse_date(start_date)
        end_date = self.parse_date(end_date)
        num_days = (end_date - start_date).days + 1
        required_range = pd.date_range(start_date, periods=num_days)

        from_cache = cache_client.get_currency_rates(
            base_currency, other_currency, start_date, end_date)
        missing_dates = required_range.difference(from_cache.index)
        today = datetime(date.today().year,
                         date.today().month,
                         date.today().day)

        if missing_dates[missing_dates < today].empty:
            return from_cache

        skip_dates = required_range.difference(missing_dates)
        results = pd.concat([
            from_cache,
            ClientProxy.get_currency_price_history(
                base_currency, other_currency, start_date, end_date, skip_dates).dropna()
        ])
        results = results[~results.index.duplicated(keep='first')].sort_index()
        
        return results
