import yfinance as yf
import sqlite3
#from yf_interface.base import yahoo_futures_interface
from configparser import ConfigParser
import datetime

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
        print(self.ticker)

    def get_yahoo_data(self, start=None):
        self._oil_futures_ticker_generator()
        self._get_start_date()
        yahoo_ticker_interface = yf.Ticker(self.ticker)
        self.price_data = yahoo_ticker_interface.history(
                start=self.start_date, end=datetime.datetime.now())
        try:
            self.price_data.drop(
            labels=['Dividends', 'Stock Splits'], axis=1, inplace=True)
        except Exception as e:
            print(e)
        self.price_data['Ticker'] = self.ticker
    
    def _get_start_date(self):
        self._create_connection()
        self.cur.execute(f"SELECT MAX(DATE) FROM OIL_FUTURES_YAHOO WHERE TICKER = '{self.ticker+'i'}';")
        res = self.cur.fetchone()
        self.conn.close()
        print(res)
        if res[0] is not None :
            self.start_date = datetime.datetime.strptime(res[0], '%Y-%m-%d')
        else:
            self.start_date = datetime.datetime.now() - datetime.timedelta(45)
        print(self.start_date)


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
            print(e)
    
    
    def _create_execute_entry(self, row):
        """
        Create a new futures price entry in the database's OIL_FUTURES_YAHOO table
        :param conn:
        :param row:
        :return: void
        """
        
        sql = '''INSERT INTO OIL_FUTURES_YAHOO(TICKER, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME)
                VALUES(?,?,?,?,?,?,?)'''
        try:
            self._create_connection()
            self.cur.execute(sql,row)
            self.conn.commit()
            self.conn.close()
        except Exception as e:
            print(e)
    
    def create_entries_from_dataframe(self):
        #self._create_connection()
        for index,row in self.price_data.iterrows():
            print((row[5],index.date(),row[0],row[1],row[2],row[3],row[4]))
            row = (row[5],index.date(),row[0],row[1],row[2],row[3],row[4])
            self._create_execute_entry(row)
        
def get_next_12_contract_months(month_symbol_mapper):
    year  = datetime.date.today().year-2000
    month = datetime.date.today().month +2
    next_12_contracts = []
    step=0
    while step < 12:
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
        print(f"year: {contract['year']}, month: {contract['month']}")
        interface = yahoo_futures_interface(year=contract['year'], month=contract['month'])
        interface.get_yahoo_data()
        interface.create_entries_from_dataframe()