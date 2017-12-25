from .cache_db import db
from .cache_client import CacheClient
from .quandl_client import QuandlCleint
from .tradier_client import TradierClient
from .ft_client import FtClient
from .fixer_client import FixerClient
from config import config


cache_client = CacheClient(db)
quandl_client = QuandlCleint(api_key=config['quandl.api_key'])
tradier_client = TradierClient(access_token=config['tradier.access_token'])
ft_client = FtClient()
fixer_client = FixerClient()


class ClientProxy:
    cache_client = cache_client

    asset_clients = {
        "quandl": quandl_client,
        "tradier": tradier_client,
        "ft": ft_client
    }

    currency_clients = {
        "fixer": fixer_client
    }

    @classmethod
    def get_asset_price_history(self, symbol, start_date, end_date):
        for client_name, client in self.asset_clients.items():
            result = client.fetch_history(symbol, start_date, end_date)
            if result is not None:
                self.cache_client.put_asset_prices(
                    client_name, symbol, result, start_date, end_date)
                return result

    @classmethod
    def get_currency_price_history(
            self, base_currency, other_currency, start_date, end_date,
            skip_dates=None):
        for client_name, client in self.currency_clients.items():
            result = client.fetch_history(
                base_currency,
                other_currency,
                start_date,
                end_date,
                skip_dates)
            if result is not None:
                self.cache_client.put_currency_rates(
                    client_name, base_currency, result)
                return result[[other_currency]]
