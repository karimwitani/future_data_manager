import yfinance as yf
import sqlite3
from configparser import ConfigParser
import datetime
from pandas.tseries.offsets import BusinessDay
from dateutil.relativedelta import relativedelta
import logging

#SETUP LOGGING
level = logging.INFO
logging.basicConfig(
    filename='futures_interface.log', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=level)

#SETUP CONFIG OBJECT
config_object = ConfigParser()
config_object.read("config.ini")
server_config = config_object["SERVERINFO"]



month_symbol_mapper = {
    '1':{'string':'jan','code':'F'},
    '2':{'string':'feb','code':'G'},
    '3':{'string':'mar','code':'H'},
    '4':{'string':'apr','code':'J'},
    '5':{'string':'may','code':'K'},
    '6':{'string':'jun','code':'M'},
    '7':{'string':'jul','code':'N'},
    '8':{'string':'aug','code':'Q'},
    '9':{'string':'sep','code':'U'},
    '10':{'string':'oct','code':'V'},
    '11':{'string':'nov','code':'X'},
    '12':{'string':'dev','code':'Z'},
}

class yahoo_futures_interface:
    def __init__(self, year='22', month='1'):
        self.year = year
        self.month = month


    def _oil_futures_ticker_generator(self):
        self.ticker = f"CL{month_symbol_mapper[self.month]['code']}{self.year}.NYM"
        logging.debug(f"Generated ticker: {self.ticker}")


    def get_yahoo_data(self, start=None):
        self._oil_futures_ticker_generator()
        self._get_start_date()
        self._get_settlement_date()
        logging.info(f"Getting yahoo data for {self.ticker} from start date {self.start_date}")
        yahoo_ticker_interface = yf.Ticker(self.ticker)
        self.price_data = yahoo_ticker_interface.history(
                start=self.start_date, end=datetime.datetime.now())
        try:
            self.price_data.drop(
            labels=['Dividends', 'Stock Splits'], axis=1, inplace=True)
        except Exception as e:
            logging.error(f"Facing error in getting data from yahoo: {e}")
        self.price_data['Ticker'] = self.ticker
        self.price_data['Settlement Date'] = self.contract_expiration 

    
    def _get_start_date(self):
        self._create_connection()
        self.cur.execute(f"SELECT MAX(DATE) FROM OIL_FUTURES_YAHOO WHERE TICKER = '{self.ticker}';")
        res = self.cur.fetchone()
        self.conn.close()
        logging.debug(f"Most recent reading for {self.ticker} is {res[0]}")
        if res[0] is not None :
            self.start_date = datetime.datetime.strptime(res[0], '%Y-%m-%d')
        else:
            self.start_date = datetime.datetime.now() - datetime.timedelta(45)
        logging.debug(f"Start date for fetching yahoo data for ticker {self.ticker} is {self.start_date}")


    def _create_connection(self, db_file=server_config['database']):
        """ create a database connection to the SQLite database
            specified by db_file
        :param db_file: database file
        :return: Connection object or None
        """
        self.conn = None
        try:
            self.conn = sqlite3.connect(db_file)
            self.cur = self.conn.cursor()
        except Exception as e:
            logging.error(f"Problem with connecting to database {db_file}, {e}")


    def _get_settlement_date(self):
        year = int(self.year)+2000
        month = int(self.month)

        previous_month_25th_day = datetime.date(year,month,25) - relativedelta(months=1)
        logging.debug(f'Previous_month_25th_day; {previous_month_25th_day}')
        if previous_month_25th_day.weekday() < 5:
            contract_expiration = previous_month_25th_day-3*BusinessDay()
        else:
            contract_expiration = previous_month_25th_day-4*BusinessDay()
        self.contract_expiration = contract_expiration.date()
        logging.debug(f"Expiry date for the contract {self.ticker} is {self.contract_expiration}")
    

    def _create_execute_entry(self, row):
        """
        Create a new futures price entry in the database's OIL_FUTURES_YAHOO table
        :param conn:
        :param row:
        :return: void
        """
        
        sql = '''INSERT INTO OIL_FUTURES_YAHOO(DATE,TICKER,OPEN, HIGH, LOW, CLOSE, VOLUME,SETTLEMENT_DATE)
                VALUES(?,?,?,?,?,?,?,?)'''
        try:
            self._create_connection()
            self.cur.execute(sql,row)
            self.conn.commit()
            self.conn.close()
        except Exception as e:
            logging.error(f"Error in inserting sql into the database: {e}")
    

    def create_entries_from_dataframe(self):
        #self._create_connection()
        for index,row in self.price_data.iterrows():
            logging.debug(f"Data to insert into the database: {index.date(),row[5],row[0],row[1],row[2],row[3],row[4],row[6]}")
            row = (index.date(),row[5],row[0],row[1],row[2],row[3],row[4],row[6])
            self._create_execute_entry(row)
        
def get_next_12_contract_months(month_symbol_mapper):
    year  = datetime.date.today().year-2000
    month = datetime.date.today().month +2
    next_12_contracts = []
    step=0
    while step <= 36:
        next_12_contracts.append({'year':str(year), 'month':str(month)})
        if month <12:
            month +=1
        else:
            year+=1
            month = 1
        step+=1
    return next_12_contracts


if __name__ == '__main__':
    next_12_contracts = get_next_12_contract_months(month_symbol_mapper)

    for contract in next_12_contracts:
        #print(f"year: {contract['year']}, month: {contract['month']}")
        interface = yahoo_futures_interface(year=contract['year'], month=contract['month'])
        interface.get_yahoo_data()
        interface.create_entries_from_dataframe()
