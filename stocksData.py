"""This modules gathers data from Alpha Vantage API and calculates
 daily, weekly, monthly, quarterly, half-yearly and annual time-series
 data for stocks
"""

import os
import sys
import datetime
import time
from alpha_vantage.timeseries import TimeSeries
import numpy as np
import pandas as pd

with open('apiKey.txt', 'r', encoding='UTF-8') as f:
    apiKey = f.readline()


def load_symbols(filename):
    """Loads a .csv file as dataFrame


       Arguments:
       filename -> A csv files containing all symbols. Each symbol
       should be on a different line and contains no headers.
    """
    # filename with csv extension
    df = pd.read_csv(filename, header=None, names=['symbols'])
    df['symbols'] = df['symbols'].str.strip()
    df['symbols'] = df['symbols'].str.upper()
    return df['symbols'].to_list()


def execute_symbols(symbols, output_file=None):
    """Runs script
    """
    # print("In execute symbols")
    if isinstance(symbols, str):
        symbols = [symbols]
    num_symbols = len(symbols)
    dates = getDates()
    num_requests = 0
    for idx, symbol in enumerate(symbols):
        if num_requests >= 4:
            num_requests = 0
            time.sleep(60)
        # print(f"{symbol} Start")
        print(f"Getting data for {symbol} ({idx+1}/{num_symbols})")
        try:
            start_time = time.time()
            (weekly, monthly) = get_data(symbol, dates)
            end_time = time.time()
            # print(f"Duration to fetch data: {end_time - start_time} s")
            if (end_time - start_time) <= 60:
                num_requests += 2
            else:
                num_requests = 0
            # print(f"Running API request count:{num_requests}")
            df = computeData(symbol, weekly, monthly)
            # print(df)
            # print(df)
            # print(f"Done")
        except:
            print(f"Error occurred: {symbol}")
        df.insert(0, 'symbol', symbol)
        write_master_csv(df, output_file)


# def execute_symbols(symbols):Runs script
#     if isinstance(symbols, str):
#         symbols = [symbols]
#     num_symbols = len(symbols)
#     dates = getDates()
#     num_requests=0
#     for idx, symbol in enumerate(symbols):
#         # if num_requests >= 4:
#         #     num_requests = 0
#         #     time.sleep(60)
#         # print(f"{symbol} Start")
#         print(f"Getting data for {symbol} ({idx+1}/{num_symbols})")
#         try:
#             start_time = time.time()
#             (weekly, monthly, num_requests) = get_data(symbol, dates,
#                                                       num_requests)
#             end_time = time.time()
#             # print(f"Duration to fetch data: {end_time - start_time} s")
#             if (end_time - start_time) > 60:
#                 num_requests = 0
#             # else:
#             #     num_requests = 0
#             # print(f"Running API request count:{num_requests}")
#             df = computeData(symbol, weekly, monthly)
#             # print(f"Done")
#         except:
#             print(f"Error occurred: {symbol}")
#         try:
#             df.insert(0, 'symbol', symbol)
#         except:
#             continue
#         write_master_csv(df)


def write_master_csv(df, output_file=None):
    """Creates output file
    """
    if not output_file:
        output_file = (
            f"masterData-{pd.to_datetime('today').strftime('%Y-%m-%d')}.csv")
    df.to_csv(output_file, mode='a', index=False,
              header=not os.path.exists(output_file))


def run_script(filename, output_file=None):
    """Runs script
    """
    symbols = load_symbols(filename)
    execute_symbols(symbols, output_file)

    print(f"Script execution complete")


def get_data(symbol, dates, BSE=True):
    """Get data from Alpha_Vantage
    """
    ts = TimeSeries(key=apiKey, output_format='pandas')
    if BSE:
        symbol = symbol+'.BSE'
    try:
        # print(f"Data fetch start")
        weekly, meta_data_w = ts.get_weekly_adjusted(symbol=symbol)
        monthly, meta_data_m = ts.get_monthly_adjusted(symbol=symbol)
        # print(f"Data fetch complete")
    except Exception as e:
        print(e)
        print(f"Error Raised")
    return (weekly, monthly)


# def get_data(symbol, dates, num_requests= 0, BSE=True):

#     ts = TimeSeries(key=apiKey, output_format='pandas')
#     if BSE:
#         symbol = symbol+'.BSE'
#     try:
#         # print(f"Data fetch start")
#         if num_requests >= 5 :
#             time.sleep(60)
#             num_requests = 0
#         weekly, meta_data_w  = ts.get_weekly_adjusted(symbol=symbol)
#         num_requests += 1

#         if num_requests >= 5 :
#             time.sleep(60)
#             num_requests = 0
#         monthly, meta_data_m = ts.get_monthly_adjusted(symbol=symbol)
#         num_requests += 1

#         # print(f"Data fetch complete")
#     except Exception as e:
#         print(e)
#         print(f"Error Raised")
#     return (weekly, monthly, num_requests)


def computeData(symbol, weekly, monthly):
    (weekEnd, monthEnd,
     quarterBegin, quarterEnd,
     halfBegin, halfEnd,
     yearBegin, yearEnd) = getDates()
    # print("Dates retrived")
    weeklyData = getDictStandard(weekly, weekEnd, 'weekly')
    monthlyData = getDictStandard(monthly, monthEnd, 'monthly')
    quarterlyData = getDictCustom(monthly, quarterBegin, quarterEnd,
                                  'quarterly')
    halflyData = getDictCustom(monthly, halfBegin, halfEnd, 'halfly')
    yearlyData = getDictCustom(monthly, yearBegin, yearEnd, 'yearly')
    # print(weeklyData, monthlyData, quarterlyData, halflyData, yearlyData)

    rename = {
        '1. open': 'open',
        '2. high': 'high',
        '3. low': 'low',
        '4. close': 'close',
        '5. adjusted close': 'adjclose',
    }

    df = pd.DataFrame([weeklyData, monthlyData, quarterlyData, halflyData,
                       yearlyData])
    # print(f"DF Ready")
    df1 = df.drop(columns=['6. volume', '7. dividend amount'])
    df2 = df1.rename(columns=rename)
    # df2.to_csv(f"{symbol}.csv", index=False)
    # print(f"Export Complete")
    return df2
    # df2.to_excel(f"{symbol[:-4]}.xslx")


def getDates():
    today = pd.Timestamp.today()

    dayOfTheWeek = today.day_of_week
    # End of week date
    weekEnd = (today + pd.DateOffset(days=-abs(dayOfTheWeek - 4))).normalize()

    # End of Month date
    if (today.normalize() < (today + pd.offsets.BMonthEnd(n=0)).normalize()):
        monthEnd = (today + pd.offsets.MonthEnd(n=-1)).normalize()
    else:
        monthEnd = today.normalize()

    # Quarter Dates
    if (today.normalize() < (today + pd.offsets.BQuarterEnd(n=0)).normalize()):
        quarterEnd = (today + pd.offsets.QuarterEnd(n=-1)).normalize()
    else:
        quarterEnd = today.normalize()
    quarterBegin = quarterEnd + pd.offsets.QuarterBegin(n=-1, startingMonth=1)

    # Semiannual Dates

    if not today.is_year_end:
        if (today.month > 6) or (today.month == 6 and today.day == 30):
            halfEnd = (
                (today + pd.DateOffset(months=-abs(today.month - 6)))
                + pd.offsets.QuarterEnd(n=0)
            ).normalize()
            halfBegin = pd.Timestamp(datetime.date(halfEnd.year, 1, 1))
        else:
            halfEnd = (today + pd.offsets.YearEnd(n=-1)).normalize()
            halfBegin = pd.Timestamp(datetime.date(halfEnd.year, 6, 1))
    else:
        halfEnd = today.normalize()
        halfBegin = pd.Timestamp(datetime.date(halfEnd.year, 6, 1))

    # Annual Dates

    if (today.normalize() < (today + pd.offsets.BYearEnd(n=0)).normalize()):
        yearEnd = (today + pd.offsets.YearEnd(n=-1)).normalize()
    else:
        yearEnd = (today).normalize()
    yearBegin = yearEnd + pd.offsets.YearBegin(n=-1)

    return (weekEnd, monthEnd,
            quarterBegin, quarterEnd,
            halfBegin, halfEnd,
            yearBegin, yearEnd)


def getDictStandard(df, period_end, label):
    dfData = df.loc[df.index[df.index <= period_end].max()]
    if dfData.empty:
        dictData = {'label': label,
                    'date': period_end,
                    '1. open': np.nan,
                    '2. high': np.nan,
                    '3. low': np.nan,
                    '4. close': np.nan,
                    '5. adjusted close': np.nan,
                    '6. volume': np.nan,
                    '7. dividend amount': np.nan,
                    }
    else:
        dictData = {'label': label,
                    'date': df.index[df.index <= period_end].max()}
        dictData = {**dictData, **dfData.to_dict()}
    return dictData


def getDictCustom(df, period_begin, period_end, label):
    periodly = df.loc[
        df.index[(df.index <= period_end) & (df.index > period_begin)]]
    if periodly.empty:
        periodly_data = {'label': label,
                         'date': period_end,
                         '1. open': np.nan,
                         '2. high': np.nan,
                         '3. low': np.nan,
                         '4. close': np.nan,
                         '5. adjusted close': np.nan,
                         '6. volume': np.nan,
                         '7. dividend amount': np.nan,
                         }
    else:
        periodly_data = {'label': label,
                         'date': periodly.index[0],
                         '1. open': periodly['1. open'].iloc[-1],
                         '2. high': periodly['2. high'].max(),
                         '3. low': periodly['3. low'].min(),
                         '4. close': periodly['4. close'].iloc[0],
                         '5. adjusted close': periodly['5. adjusted close'].iloc[0],
                         '6. volume': periodly['6. volume'].sum(),
                         '7. dividend amount': periodly['7. dividend amount'].sum(),
                         }
    return periodly_data


if __name__ == '__main__':
    if len(sys.argv) > 1:
        DATA_FILE = sys.argv[1]
        OUTPUT_FILE = None
        if len(sys.argv) > 2:
            OUTPUT_FILE = sys.argv[2]
    else:
        DATA_FILE = "symbolsT.csv"
        OUTPUT_FILE = None
    run_script(DATA_FILE, OUTPUT_FILE)
