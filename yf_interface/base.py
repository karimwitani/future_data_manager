import yfinance as yf
import time
import datetime
import pandas as pd
import sqlite3
import logging

month_symbol_mapper = {
    'jul': 'N',
    'aug': 'Q',
    'sep': 'U',
    'oct': 'V',
    'nov': 'X',
    'dec': 'Z',
    'jan': 'F',
    'feb': 'G',
    'mar': 'H',
    'apr': 'J',
    'may': 'K',
    'jun': 'M',
}


class yahoo_futures_interface:
    def __init__(self, year='22', month='jan'):
        self.year = year
        self.month = month

    def _oil_futures_ticker_generator(self):
        self.ticker = f"CL{month_symbol_mapper[self.month]}{self.year}.NYM"
        logging.debug(self.ticker)

    def get_yahoo_data(self, start=None, end=None):
        self._oil_futures_ticker_generator()
        yahoo_ticker_interface = yf.Ticker(self.ticker)
        if start is None or end is None:
            self.price_data = yahoo_ticker_interface.history(period='max')
        else:
            self.price_data = yahoo_ticker_interface.history(
                start=start, end=end)
        self.price_data.drop(
            labels=['Dividends', 'Stock Splits'], axis=1, inplace=True)
        self.price_data['Ticker'] = self.ticker
