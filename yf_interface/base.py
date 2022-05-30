from imports import * 



def oil_futures_ticker_generator(date):
    """ Builds a ticker for WTI contract expiring in that month/year combination.
    
    Uses a dictionary to get the mapping of months to ticker codes and builds a ticker code.

    Parameters
    ----------
    date : datetime.date
        A datetime.date object of that day/month/year of the desired ticker

    Returns
    ----------
    ticker : string
        The string of the ticker that will be used by the data interface
    """
    #Map months to ticker codes
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

    ticker = f"CL{month_symbol_mapper[str(date.month)]['code']}{date.year-2000}.NYM"
    return ticker


def get_business_date_offset(date):
    """ Get the business date offset WTI oil futures using the specs from ICE.
    
    The contracts settle 3 business days prior to the 25th of each month. If the 25th is not a business date itself
    the offset is 4 business days.

    Parameters
    ----------
    date : datetime.date
        The 25th day of the month in which we're looking for an expiry date

    Returns
    ----------
    offset : int
        The number of days to offset from the 25th day of the month
    """

    #builds a dictionary of US holidays
    us_holidays = holidays.US()

    offset = 3 if date.weekday() < 5 and date not in us_holidays else 4
    return offset


def get_25th_day(date):
    """ Get the 25th day of a given month.
    
    The 25th day of a month is the anchor from which ICE determines the offset for contract expiry. If 25th is a weekday
    then the offset is 3 business days otherwise it is 4. 

    Parameters
    ----------
    date : datetime.date
        The 25th day of the month in which we're looking for an expiry date

    Returns
    ----------
    offset : int
        The number of days to offset from the 25th day of the month
    """

    assert  isinstance(date, datetime.date), 'date passed is not a datetime object'
    return datetime.date(date.year, date.month, 25)


def get_contract_code_time_spread(date=datetime.date.today(), time_spread=0):
    """ Get the next settlement contract from a given date.
    
    Contracts usualy expire on 20-22nd day of a given month prior to contract delivery. Depending on which day it is the
    contract expiry can be determined for this month or any given number of months into the future. To get the current 
    (next) expiry, use the default values. Otherwise use the next contract expiry as an anchor and the time_spread steps
    as the offset for months into the future.

    Parameters
    ----------
    date : datetime.date
        The date from which you want to get the expiry dates.
    
    time_spread : int
        the number of months into the future that you want to ge the expiry for.

    Returns
    ----------
    ticker : string
        The ticker for the returned contract.

    next_settelment_date : datetime.date
        The settlment date for the returned contract.
    """

    #get the 25th day of the month, the anchor
    day_25th = get_25th_day(date) + relativedelta(months=time_spread)
    
    #get the offset depending the which weekday the 25th is on
    business_day_offset = get_business_date_offset(day_25th)

    #The settlement date logic
    settlement_date_current_month = day_25th -  business_day_offset*BusinessDay()
    settlement_date_current_month = settlement_date_current_month.to_pydatetime().date()
    
    #If already passed the settlment date for the month, then use the next months
    if date < settlement_date_current_month:
        next_settelment_date = settlement_date_current_month
    else:
        day_25th = day_25th + relativedelta(months=1)
        business_day_offset = get_business_date_offset(day_25th)
        next_settelment_date = day_25th - business_day_offset*BusinessDay()
        next_settelment_date = next_settelment_date.to_pydatetime().date()

    #Ticker is generated using the expiry date
    ticker = oil_futures_ticker_generator(next_settelment_date+relativedelta(months=1))

    logging.debug(f"2 next_settelment_date : {next_settelment_date}, ticker: {ticker}")
    
    return ticker, next_settelment_date