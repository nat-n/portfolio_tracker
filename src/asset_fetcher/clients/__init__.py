from .cache_db import db
from .cache_client import CacheClient
from .quandl_client import QuandlCleint
from .tradier_client import TradierClient
from .ft_client import FtClient
from config import config

cache_client = CacheClient(db)
quandl_client = QuandlCleint(api_key=config['quandl.api_key'])
tradier_client = TradierClient(access_token=config['tradier.access_token'])
ft_client = FtClient()
