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

# variable setup
today = datetime.date.today()
database = root_path + config["db"]
spreads_table = "OIL_FUTURES_CL_TIME_SPREADS"
historical_table = "OIL_FUTURES_YAHOO"

# latest date from the spreads table
max_spread_date = get_latest_date(database, spreads_table)
max_historical_date = get_latest_date(database, historical_table)
print(
    f"Latest historical date: {max_historical_date}, latest spreads date is: {max_spread_date}"
)

if max_historical_date > max_spread_date:
    print("spreads data is behind")

    # get appropriate tickers and settlement dates
    ticker_0, next_settelment_date_0 = get_contract_code_time_spread()
    ticker_3, next_settelment_date_3 = get_contract_code_time_spread(
        date=next_settelment_date_0, time_spread=3
    )
    ticker_6, next_settelment_date_6 = get_contract_code_time_spread(
        date=next_settelment_date_3, time_spread=3
    )
    ticker_9, next_settelment_date_9 = get_contract_code_time_spread(
        date=next_settelment_date_6, time_spread=3
    )
    ticker_12, next_settelment_date_12 = get_contract_code_time_spread(
        date=next_settelment_date_9, time_spread=3
    )

    # setup the database connection and get the historical data
    conn, cur = create_databse_connection(db_file=database)
    sql = f"""
    WITH TEMP_0 AS (
        SELECT DATE, TICKER, CLOSE
        FROM OIL_FUTURES_YAHOO
        WHERE TICKER = '{ticker_0}'

    ),
    TEMP_3 AS (
        SELECT DATE, TICKER, CLOSE
        FROM OIL_FUTURES_YAHOO
        WHERE TICKER = '{ticker_3}'

    ),
    TEMP_6 AS (
        SELECT DATE, TICKER, CLOSE
        FROM OIL_FUTURES_YAHOO
        WHERE TICKER = '{ticker_6}'

    ),
    TEMP_9 AS (
        SELECT DATE, TICKER, CLOSE
        FROM OIL_FUTURES_YAHOO
        WHERE TICKER = '{ticker_9}'

    ),
    TEMP_12 AS (
        SELECT DATE, TICKER, CLOSE
        FROM OIL_FUTURES_YAHOO
        WHERE TICKER = '{ticker_12}'

    )
    SELECT
        TEMP_0.DATE, TEMP_0.TICKER SPR_FRONT_CONTRACT, TEMP_0.CLOSE SPR_FRONT_CLOSING,
        TEMP_3.TICKER SPR_3MO_CONTRACT, TEMP_3.CLOSE SPR_3MO_CLOSING,
        TEMP_6.TICKER SPR_6MO_CONTRACT, TEMP_6.CLOSE SPR_6MO_CLOSING,
        TEMP_9.TICKER SPR_9MO_CONTRACT, TEMP_9.CLOSE SPR_9MO_CLOSING,
        TEMP_12.TICKER SPR_12MO_CONTRACT, TEMP_12.CLOSE SPR_12MO_CLOSING
    FROM TEMP_0
    INNER JOIN TEMP_3
        ON TEMP_0.DATE = TEMP_3.DATE
    INNER JOIN TEMP_6
        ON TEMP_0.DATE = TEMP_6.DATE
    INNER JOIN TEMP_9
        ON TEMP_0.DATE = TEMP_9.DATE
    INNER JOIN TEMP_12
        ON TEMP_0.DATE = TEMP_12.DATE
    WHERE TEMP_0.DATE > '{max_spread_date}'
    """

    df = pd.read_sql_query(sql, conn)
    conn.close()

    # remove from prod
    print(df.head())

    # values into the spreads table
    conn, cur = create_databse_connection(db_file=database)

    try:
        df.to_sql(
            "OIL_FUTURES_CL_TIME_SPREADS", index=False, con=conn, if_exists="append"
        )
    except Exception as e:
        print(e)
    conn.close()
else:
    print("spreads data is up to date")
