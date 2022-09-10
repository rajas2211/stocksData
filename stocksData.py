import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import datetime
import time
import os

apiKey = 'VH10W9NDNWI32HGL'

def loadSymbols(filename):
    ##filename with csv extension
    df = pd.read_csv(filename, header=None, names=['symbols'])
    df['symbols'] = df['symbols'].str.strip()
    df['symbols'] = df['symbols'].str.upper()
    return df['symbols'].to_list()

def executeSymbols(symbols):
    if isinstance(symbols, str):
        symbols = [symbols]
    numOfSymbols = len(symbols)
    dates = getDates()
    numRequests=0
    for idx, symbol in enumerate(symbols):
        if numRequests >= 4:
            numRequests = 0
            time.sleep(60)
        # print(f"{symbol} Start")
        print(f"Getting data for {symbol} ({idx+1}/{numOfSymbols})")
        try:
            start_time = time.time()
            (weekly, monthly) = getData(symbol, dates)
            end_time = time.time()
            # print(f"Duration to fetch data: {end_time - start_time} s")
            if (end_time - start_time) <= 60:
                numRequests += 2
            else:
                numRequests = 0
            # print(f"Running API request count:{numRequests}")
            df = computeData(symbol, weekly, monthly)
            # print(f"Done")
        except:
            print(f"Error occured: {symbol}")
        df.insert(0, 'symbol', symbol)
        writeMasterCSV(df)


# def executeSymbols(symbols):
#     if isinstance(symbols, str):
#         symbols = [symbols]
#     numOfSymbols = len(symbols)
#     dates = getDates()
#     numRequests=0
#     for idx, symbol in enumerate(symbols):
#         # if numRequests >= 4:
#         #     numRequests = 0
#         #     time.sleep(60)
#         # print(f"{symbol} Start")
#         print(f"Getting data for {symbol} ({idx+1}/{numOfSymbols})")
#         try:
#             start_time = time.time()
#             (weekly, monthly, numRequests) = getData(symbol, dates, numRequests)
#             end_time = time.time()
#             # print(f"Duration to fetch data: {end_time - start_time} s")
#             if (end_time - start_time) > 60:
#                 numRequests = 0
#             # else:
#             #     numRequests = 0
#             # print(f"Running API request count:{numRequests}")
#             df = computeData(symbol, weekly, monthly)
#             # print(f"Done")
#         except:
#             print(f"Error occured: {symbol}")
#         try:
#             df.insert(0, 'symbol', symbol)
#         except:
#             continue
#         writeMasterCSV(df)

def writeMasterCSV(df):
    outputFile = f"masterData-{pd.to_datetime('today').strftime('%Y-%m-%d')}.csv"
    df.to_csv(outputFile, mode='a', index=False, header = not os.path.exists(outputFile))


def runScript(filename):
    symbols = loadSymbols(filename)
    executeSymbols(symbols)
    print(f"Script execution complete")

def getData(symbol, dates, BSE=True):
    ts = TimeSeries(key=apiKey, output_format='pandas')
    if BSE:
        symbol = symbol+'.BSE'
    try:
        # print(f"Data fetch start")
        weekly, metaDataW  = ts.get_weekly_adjusted(symbol=symbol)
        monthly, metaDataM = ts.get_monthly_adjusted(symbol=symbol)
        # print(f"Data fetch complete")
    except Exception as e:
        print(e)
        print(f"Error Raised")
    return (weekly, monthly)


# def getData(symbol, dates, numRequests= 0, BSE=True):

#     ts = TimeSeries(key=apiKey, output_format='pandas')
#     if BSE:
#         symbol = symbol+'.BSE'
#     try:
#         # print(f"Data fetch start")
#         if numRequests >= 5 :
#             time.sleep(60)
#             numRequests = 0            
#         weekly, metaDataW  = ts.get_weekly_adjusted(symbol=symbol)
#         numRequests += 1

#         if numRequests >= 5 :
#             time.sleep(60)
#             numRequests = 0
#         monthly, metaDataM = ts.get_monthly_adjusted(symbol=symbol)
#         numRequests += 1
        
#         # print(f"Data fetch complete")
#     except Exception as e:
#         print(e)
#         print(f"Error Raised")
#     return (weekly, monthly, numRequests)


def computeData(symbol, weekly, monthly):
    (weekEnd, monthEnd,
    quarterBegin, quarterEnd, 
    halfBegin, halfEnd,
    yearBegin, yearEnd) = getDates()
    
    weeklyData = getDictStandard(weekly, weekEnd, 'weekly')
    monthlyData = getDictStandard(monthly, monthEnd, 'monthly')
    quarterlyData = getDictCustom(monthly, quarterBegin, quarterEnd,
                                  'quarterly')
    halflyData = getDictCustom(monthly, halfBegin, halfEnd, 'halfly')
    yearlyData = getDictCustom(monthly, yearBegin, yearEnd, 'yearly')

    rename = {
        '1. open': 'open' ,
        '2. high': 'high' ,
        '3. low': 'low' ,
        '4. close': 'close' ,
        '5. adjusted close' : 'adjclose',
    }

    df = pd.DataFrame([weeklyData, monthlyData, quarterlyData, halflyData,
                       yearlyData])
    df1 = df.drop(columns=['6. volume', '7. dividend amount'])
    df2 = df1.rename(columns=rename)
    df2.to_csv(f"{symbol}.csv", index=False)
    # print(f"Export Complete")
    return df2
    # df2.to_excel(f"{symbol[:-4]}.xslx")

def getDates():
    today = pd.Timestamp.today()

    dayOfTheWeek = today.day_of_week
    ## End of week date
    weekEnd = (today + pd.DateOffset(days=-abs(dayOfTheWeek - 4))).normalize() 

    ## End of Month date
    if (today.normalize() < (today + pd.offsets.BMonthEnd(n=0)).normalize()):
        monthEnd = (today + pd.offsets.MonthEnd(n=-1)).normalize()
    else:
        monthEnd = today.normalize()

    ## Quarter Dates
    if (today.normalize() < (today + pd.offsets.BQuarterEnd(n=0)).normalize()):
        quarterEnd = (today + pd.offsets.QuarterEnd(n=-1)).normalize()
    else:
        quarterEnd = today.normalize()
    quarterBegin = quarterEnd + pd.offsets.QuarterBegin(n=-1, startingMonth=1)

    ## Semiannual Dates

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

    ## Annual Dates

    if (today.normalize() < (today + pd.offsets.BYearEnd(n=0)).normalize()):
        yearEnd = (today + pd.offsets.YearEnd(n=-1)).normalize()
    else:
        yearEnd = (today).normalize()
    yearBegin = yearEnd + pd.offsets.YearBegin(n=-1)
    
    return (weekEnd, monthEnd,
            quarterBegin, quarterEnd, 
            halfBegin, halfEnd,
            yearBegin, yearEnd)


def getDictStandard(df, periodEnd, label):
    dfData = df.loc[df.index[df.index <= periodEnd].max()]
    dictData = {'label': label,'date': df.index[df.index <= periodEnd].max()}
    dictData = {**dictData, **dfData.to_dict()}
    return dictData

def getDictCustom(df, periodBegin, periodEnd, label):
    periodly = df.loc[
        df.index[(df.index <= periodEnd) & (df.index > periodBegin)]]
    periodlyData = { 'label': label,
                     'date': periodly.index[0],
                     '1. open': periodly['1. open'].iloc[-1],
                     '2. high': periodly['2. high'].max(),
                     '3. low': periodly['3. low'].min(),
                     '4. close': periodly['4. close'].iloc[0],
                     '5. adjusted close': periodly['5. adjusted close'].iloc[0],
                     '6. volume': periodly['6. volume'].sum(),
                     '7. dividend amount': periodly['7. dividend amount'].sum(),
                    }
    return periodlyData
