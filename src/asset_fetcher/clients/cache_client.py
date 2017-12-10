import pandas as pd
from pymongo import UpdateOne
from .helpers import log_client_fetch_error, record_keys, columns_map


class CacheClient:
    def __init__(self, db):
        self.historical_prices = db.historical_prices

    def put_one(self, record):
        self.historical_prices.update(
            {'symbol': record['symbol'], 'date': record['date']}, record, True)

    def put_many(self, items):
        operations = []
        for item in items:
            record = {'symbol': item['symbol'], 'date': item['date']}
            # avoid overwritting a day that isn't missing with one that is
            if item['_placeholer']:
                record['_placeholer']: True
            operations.append(UpdateOne(record, {'$set': item}, upsert=True))
        self.historical_prices.bulk_write(operations, ordered=False)

    def get(self, symbol, start_date=None, end_date=None):
        try:
            query = {'symbol': symbol}
            if start_date is not None or end_date is not None:
                query['date'] = {}
                if start_date is not None:
                    query['date']['$gte'] = start_date
                if end_date is not None:
                    query['date']['$lte'] = end_date
            return self.create_history_data_frame(
                self.historical_prices.find(query, {'_id': 0})
            )
        except Exception:
            log_client_fetch_error('cache', symbol, start_date, end_date)
            return self.create_history_data_frame([])

    def create_history_data_frame(self, records):
        result = pd.DataFrame(list(records), columns=record_keys)
        result.rename(columns=columns_map, inplace=True)
        result.set_index('Date', inplace=True)
        return result
