from json.decoder import JSONDecodeError
import requests
import time
import pandas as pd
from datetime import datetime, date

class FixerClient:
    query_url = (
        'http://api.fixer.io/{date}'
        '?base={base}'
        # '&symbols={symbols}' # actually just grab and cache everything cos why not
    )

    earliest_date = datetime(1999, 1, 4)
    max_retry_budget = 20

    def request_url(self, target_date, base_currency, currencies):
        return self.query_url.format(
            date=target_date.strftime('%Y-%m-%d'),
            base=base_currency,
            symbols=','.join(currencies)
        )

    def fetch_history(self, base_currency, currencies, start_date, end_date, skip_dates=None, remaining_retries=10):
        if isinstance(currencies, str):
            currencies = [currencies]
        dates = pd.date_range(start_date, end_date)
        result = pd.DataFrame(index=dates, columns=currencies)

        if skip_dates is not None:
            dates = dates.difference(skip_dates)

        for target_date in dates:
            # gradually increase remaining retry budget with successful requests
            remaining_retries = min(self.max_retry_budget, remaining_retries + 0.075)
            # throttle harder if fewer retries remaining
            time.sleep(
                max(0.15, 0.6 * (1 - (remaining_retries / self.max_retry_budget))))
            req = requests.get(self.request_url(
                target_date, base_currency, currencies))
            try:
                res = req.json()
            except JSONDecodeError as req_error:
                if remaining_retries > 0:
                    # wait longer if fewer retries remaining
                    time.sleep(max(2, self.max_retry_budget - remaining_retries))
                    return self.fetch_history(base_currency, currencies, target_date, end_date, skip_dates, remaining_retries - 1)
                else:
                    raise req_error
            
            for currency in res['rates']:
                result.at[target_date, currency] = res['rates'][currency]

        return result
