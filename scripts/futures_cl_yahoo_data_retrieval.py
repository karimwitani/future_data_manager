"""
Boiler plate for importing the modules
"""
import os
import sys
from pprint import pprint
from pathlib import Path

# Setup the rootpath
root_path = str(Path(os.path.dirname(os.path.realpath(__file__))).parent)
sys.path.insert(0, root_path)
from lib.base import *

# Setup the config dictionary
config = AttrDict(
    yaml.load(Loader=yaml.BaseLoader, stream=open(root_path + "/config/config.yaml"))
)
config = config[ENV]

########################
# SCRIPT START
########################


today = datetime.date.today()
database = root_path + config["db"]
print(database)

table = "OIL_FUTURES_YAHOO"
contracts = []
for step in range(0, 24):
    if step == 0:
        ticker, front_settelment_date = get_contract_code_time_spread()
        contracts.append((ticker, front_settelment_date))
    else:
        ticker, next_settelment_date = get_contract_code_time_spread(
            date=front_settelment_date, time_spread=step
        )
        contracts.append((ticker, next_settelment_date))


for contract in contracts:
    ticker = contract[0].strip()
    print(f"ticker: {ticker}")
    expiry = contract[1]
    latest_date = get_latest_date(database=database, table=table, ticker=ticker)
    print(f"The latest date available is {latest_date}")
    if latest_date < today:
        print(
            f"Latest date in futures data is {latest_date} and it's behind today's date, {today}"
        )
        interface = yahoo_futures_interface(
            ticker=ticker,
            contract_expiration=expiry,
            start_date=latest_date + relativedelta(days=1),
            database=database,
        )
        interface.get_yahoo_data()
        interface.create_entries_from_dataframe()
    else:
        print("data is up to date")
