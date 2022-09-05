import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import datetime

apiKey = 'VH10W9NDNWI32HGL'

def getData(symbol, BSE=True):
    ts = TimeSeries(key=apiKey, output_format='pandas')
    if BSE:
        symbol = symbol+'.BSE'
    weekly, metaDataW  = ts.get_weekly_adjusted(symbol=symbol)
    monthly, metaDataM = ts.get_monthly_adjusted(symbol=symbol)
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
            halfEnd = ((today + pd.DateOffset(months=-abs(today.month - 6))) + pd.offsets.QuarterEnd(n=0)).normalize()
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
    
    weeklyData = getDictStandard(weekly, weekEnd, 'weekly')
    monthlyData = getDictStandard(monthly, monthEnd, 'monthly')
    quarterlyData = getDictCustom(monthly, quarterBegin, quarterEnd, 'quarterly')
    halflyData = getDictCustom(monthly, halfBegin, halfEnd, 'halfly')
    yearlyData = getDictCustom(monthly, yearBegin, yearEnd, 'yearly')

    rename = {
        '1. open': 'open' ,
        '2. high': 'high' ,
        '3. low': 'low' ,
        '4. close': 'close' ,
        '5. adjusted close' : 'adjclose',
    }

    df = pd.DataFrame([weeklyData, monthlyData, quarterlyData, halflyData, yearlyData])
    df1 = df.drop(columns=['6. volume', '7. dividend amount'])
    df2 =df1.rename(columns=rename)
    df2.to_csv(f"{symbol[:-4]}.csv", index=False)
    # df2.to_excel(f"{symbol[:-4]}.xslx")

def getDictStandard(df, periodEnd, label):
    dfData = df.loc[df.index[df.index <= periodEnd].max()]
    dictData = {'label': label,'date': df.index[df.index <= periodEnd].max()}
    dictData = dictData | dfData.to_dict()
    return dictData

def getDictCustom(df, periodBegin, periodEnd, label):
    periodly = df.loc[df.index[(df.index <= periodEnd) & (df.index > periodBegin)]]
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


