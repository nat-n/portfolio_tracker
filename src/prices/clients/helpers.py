
def log_client_fetch_error(client, symbol, start, end):
    msg = (
        'Error fetching data from {client} for {symbol} '
        'in range {start:%Y-%m-%d} : {end:%Y-%m-%d}.'
    ).format(
        client=client,
        symbol=symbol,
        start=start,
        end=end
    )
    print(msg)

asset_record_keys = ['date', 'open', 'high', 'low', 'close', 'volume']
asset_column_names = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
asset_columns_map = {
    'date': 'Date',
    'open': 'Open',
    'high': 'High',
    'low': 'Low',
    'close': 'Close',
    'volume': 'Volume'
}
