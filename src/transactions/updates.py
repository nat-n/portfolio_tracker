import pandas as pd


class Updates:
    updates_columns = ['Date', 'Quantity', 'Price',
                       'Price Factor', 'Exchange Rate', 'Base Currency']
    updates_column_defaults = [None, None, 1, 1, 1, None]
    cash_prefix = 'Cash:'
    cash_trasaction_types = ['Deposit', 'Withdrawal', 'Payment', 'Fee']

    holdings = {}
    asset_currencies = {}

    def __init__(self, transactions):
        for _, transaction in transactions.iterrows():
            self._updates_from_transaction(transaction)
        self.updates = dict([
            [asset_symbol, {
                'asset_currency': self.asset_currencies[asset_symbol],
                'updates': self._create_upates_df(asset_updates)
            }]
            for asset_symbol, asset_updates in self.holdings.items()
        ])

    def _fill_missing_columns(self, row):
        row += self.updates_column_defaults[len(row):]
        if (row[2] is None):
            row[2] = self.updates_column_defaults[2]
        return row

    def _create_upates_df(self, transactions):
        result = pd.DataFrame([
            self._fill_missing_columns(t) for t in transactions
        ], columns=self.updates_columns)
        result.set_index('Date', inplace=True)
        return result

    def get_asset_symbol(self, transaction):
        if transaction['Type'] in self.cash_trasaction_types:
            return self.cash_prefix + transaction['Asset']
        return transaction['Asset']

    def get_fee_symbol(self, transaction):
        if transaction['Fee'] > 0:
            return self.cash_prefix + transaction['Fee Currency']

    def get_base_symbol(self, transaction):
        if transaction['Base Currency'] is not None:
            return self.cash_prefix + transaction['Base Currency']

    def get_exchange_rate(self, transaction):
        conversion_required = \
            transaction['Base Currency'] != transaction['Asset Currency'] and \
            transaction['Base Currency'] is not None
        if conversion_required:
            return transaction['Exchange Rate']
        return 1

    def _updates_from_transaction(self, transaction):
        asset_symbol = self.get_asset_symbol(transaction)
        self.asset_currencies[asset_symbol] = transaction['Asset Currency']
        if asset_symbol not in self.holdings:
            self.holdings[asset_symbol] = []

        if transaction['Type'] in ['Deposit', 'Payment']:
            updates = self._updates_from_deposit(transaction)
        elif transaction['Type'] in ['Withdrawal', 'Fee']:
            updates = self._updates_from_withdrawal(transaction)
        elif transaction['Type'] == 'Purchase':
            updates = self._updates_from_purchase(transaction)
        elif transaction['Type'] == 'Sale':
            updates = self._updates_from_sale(transaction)
        else:
            raise Exception('Unknown transaction type', transaction['Type'])

        for symbol, *update in updates:
            self.holdings[symbol].append(update)

    def _latest_quantity(self, symbol):
        if symbol in self.holdings and self.holdings[symbol]:
            return self.holdings[symbol][-1][1]
        else:
            return 0

    def _updates_from_deposit(self, transaction):
        asset_symbol = self.get_asset_symbol(transaction)
        fee_symbol = self.get_fee_symbol(transaction)
        tdate = transaction['Date']
        tfee = transaction['Fee']
        tquantity = transaction['Quantity']
        asset_fee = 0

        if fee_symbol == asset_symbol:
            asset_fee = tfee
        elif fee_symbol is not None:
            yield (
                fee_symbol,
                tdate,
                self._latest_quantity(fee_symbol) - tfee
            )

        yield (
            asset_symbol,
            tdate,
            self._latest_quantity(asset_symbol) + tquantity - asset_fee
        )

    def _updates_from_withdrawal(self, transaction):
        # WARNING: if a fee is set on a fee it will be deducted twice
        asset_symbol = self.get_asset_symbol(transaction)
        fee_symbol = self.get_fee_symbol(transaction)
        tfee = transaction['Fee']
        tdate = transaction['Date']
        tquantity = transaction['Quantity']
        asset_fee = 0

        if fee_symbol == asset_symbol:
            asset_fee = tfee
        elif fee_symbol is not None:
            yield (
                fee_symbol,
                tdate,
                self._latest_quantity(fee_symbol) - tfee
            )

        yield (
            asset_symbol,
            tdate,
            self._latest_quantity(asset_symbol) - tquantity - asset_fee
        )

    def _updates_from_purchase(self, transaction):
        # WARNING: omiting Base Currency causes asset to appear without being
        #          paid for from the portfolio cash (i.e. as a grant)
        asset_symbol = self.get_asset_symbol(transaction)
        fee_symbol = self.get_fee_symbol(transaction)
        base_symbol = self.get_base_symbol(transaction)
        exchange_rate = self.get_exchange_rate(transaction)
        tfee = transaction['Fee']
        tdate = transaction['Date']
        tprice = transaction['Price']
        tpricefactor = transaction['Price Factor']
        tquantity = transaction['Quantity']
        tbase = transaction['Base Currency']

        yield (
            asset_symbol,
            tdate,
            self._latest_quantity(asset_symbol) + tquantity,
            tprice,
            tpricefactor,
            exchange_rate,
            tbase,
        )

        if base_symbol == fee_symbol and base_symbol is not None:
            yield (
                base_symbol,
                tdate,
                (
                    self._latest_quantity(base_symbol) -
                    (tquantity * tprice * tpricefactor * exchange_rate) -
                    tfee
                ),
                None, tpricefactor
            )
        else:
            if base_symbol is not None:
                yield (
                    base_symbol,
                    tdate,
                    self._latest_quantity(base_symbol) -
                    (tquantity * tprice * tpricefactor * exchange_rate),
                    None, tpricefactor
                )
            if fee_symbol is not None:
                yield (
                    fee_symbol,
                    tdate,
                    self._latest_quantity(fee_symbol) - tfee,
                    None, tpricefactor
                )

    def _updates_from_sale(self, transaction):
        # Omitting Base Currency causes asset to appear without being paid for
        # from the portfolio cash
        asset_symbol = self.get_asset_symbol(transaction)
        fee_symbol = self.get_fee_symbol(transaction)
        base_symbol = self.get_base_symbol(transaction)
        exchange_rate = self.get_exchange_rate(transaction)
        tfee = transaction['Fee']
        tdate = transaction['Date']
        tprice = transaction['Price']
        tpricefactor = transaction['Price Factor']
        tquantity = transaction['Quantity']
        tbase = transaction['Base Currency']

        yield (
            asset_symbol,
            tdate,
            self._latest_quantity(asset_symbol) - tquantity,
            tprice,
            tpricefactor,
            exchange_rate,
            tbase,
        )

        if base_symbol == fee_symbol and base_symbol is not None:
            yield (
                base_symbol,
                tdate,
                (
                    self._latest_quantity(base_symbol) +
                    (tquantity * tprice * tpricefactor * exchange_rate) -
                    tfee
                ),
                None, tpricefactor
            )
        else:
            if base_symbol is not None:
                yield (
                    base_symbol,
                    tdate,
                    (
                        self._latest_quantity(base_symbol) +
                        (tquantity * tprice * tpricefactor * exchange_rate)
                    ),
                    None, tpricefactor
                )
            if fee_symbol is not None:
                yield (
                    fee_symbol,
                    tdate,
                    self._latest_quantity(fee_symbol) - tfee,
                    None, tpricefactor
                )
