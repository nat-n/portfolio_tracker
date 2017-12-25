import math
import json
from datetime import date
import numpy as np
import pandas as pd
from updates import Updates


class Transactions:
    transaction_columns = [
        'Type', 'Date', 'Asset', 'Quantity', 'Price', 'Price Factor',
        'Asset Currency', 'Exchange Rate', 'Base Currency', 'Fee',
        'Fee Currency', 'Notes']
    transactions = None
    transaction_defaults = {
        'Price Factor': 1,
        'Exchange Rate': 1,
        'Fee': 0,
        'Notes': ''
    }

    def load_file(self, path):
        data = json.load(open(path))
        self.transactions = pd.DataFrame(
            data, columns=self.transaction_columns
        ).fillna(value=self.transaction_defaults)

    def compute_updates(self):
        self.updates = Updates(self.transactions)
