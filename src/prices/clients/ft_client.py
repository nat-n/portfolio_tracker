import pandas as pd
from datetime import date
from dateutil.parser import parse
from lxml import html
import requests
from .helpers import log_client_fetch_error, asset_column_names


class FtClient:
    query_url = (
        'https://markets.ft.com/data/equities/ajax/get-historical-prices'
        '?startDate={start_date}'
        '&endDate={end_date}'
        '&symbol={symbol}'
    )

    def fetch_history(
            self, symbol, start_date,
            end_date=date.today().isoformat(), query_url=query_url):
        try:
            url = (query_url).format(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml'}
            request = requests.get(url, headers=headers)
            response = request.json()
            html_data = html.fromstring(response['html'])
            history = []
            html_rows = html_data.xpath('//tr')
            for html_row in html_rows:
                html_cells = html_row.getchildren()
                history.append({
                    'Date': parse(html_cells[0].getchildren()[0].text),
                    'Open': self._parseFloatCell(html_cells[1]),
                    'High': self._parseFloatCell(html_cells[2]),
                    'Low': self._parseFloatCell(html_cells[3]),
                    'Close': self._parseFloatCell(html_cells[4]),
                    'Volume': self._parseFloatCell(
                        html_cells[5].getchildren()[0])
                })
            result = pd.DataFrame(
                history, columns=asset_column_names)
            result.set_index('Date', inplace=True)
            return result

        except Exception:
            log_client_fetch_error('ft', symbol, start_date, end_date)

    def _parseFloatCell(self, cell):
        try:
            return float(cell.text.replace(',', ''))
        except ValueError:
            return None
