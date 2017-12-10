from datetime import date
import quandl
from .helpers import log_client_fetch_error


class QuandlCleint:
    default_api_version = '2015-04-09'

    def __init__(self, api_key, api_version=default_api_version):
        self.set_api_config(api_key=api_key, api_version=api_version)

    def set_api_config(self, api_key=None, api_version=default_api_version):
        if api_key is not None:
            quandl.ApiConfig.api_key = api_key
        quandl.ApiConfig.api_version = api_version

    def fetch_history(self, symbol, start_date, end_date=date.today().isoformat()):
        qsymbol = "WIKI/" + symbol
        try:
            return self._normalize_results(
                quandl.get(qsymbol, start_date=start_date, end_date=end_date)
            )
        except Exception:
            log_client_fetch_error('quandl', symbol, start_date, end_date)
            return

    def _normalize_results(self, df):
        return df.loc[:, 'Open':'Volume']
