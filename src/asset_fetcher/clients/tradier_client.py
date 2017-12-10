import json
import http.client
import pandas as pd
from datetime import datetime
from .helpers import log_client_fetch_error, record_keys, columns_map


class TradierClient:
    query_format = (
        '/v1/markets/history'
        '?symbol={symbol}'
        '&start={start:%Y-%m-%d}'
        '&end={end:%Y-%m-%d}'
    )

    def __init__(self, access_token):
        self.access_token = access_token

    def fetch_history(self, symbol, start_date, end_date):
        connection = http.client.HTTPSConnection(
            'sandbox.tradier.com', 443, timeout=30)
        headers = {'Accept': 'application/json',
                   'Authorization': 'Bearer ' + self.access_token}
        try:
            url = self.query_format.format(
                symbol=symbol, start=start_date, end=end_date)
            connection.request('GET', url, None, headers)
            response = connection.getresponse()
            history = json.loads(
                response.read().decode("utf-8"))['history']

            if history is None:
                raise LookupError

            days = history['day']
            if isinstance(days, dict):
                days = [days]
            for day in days:
                day['date'] = datetime.strptime(day['date'], "%Y-%m-%d")

            result = pd.DataFrame(
                days, columns=record_keys)
            result.rename(columns=columns_map, inplace=True)
            result['Date'] = pd.to_datetime(result['Date'], format='%Y-%m-%d')
            result.set_index('Date', inplace=True)

            return result

        except (http.client.HTTPException, LookupError):
            log_client_fetch_error('tradier', symbol, start_date, end_date)
