import calendar
import json
import requests
import pandas as pd
import yfinance as yf
from pandas_datareader import data as pdr
import datetime
import time
from io import StringIO

filename = "output.csv"

symbols = [ "GME" , "AMC", "KOSS", "NAKD", "BBBY", "NOK", "GM", "AAPL", "TSLA", "MSFT", "SPY", "SPXS" ]
fields = ["issueSymbolIdentifier", "issueName", "totalWeeklyShareQuantity","totalWeeklyTradeCount","marketParticipantName","tierIdentifier","weekStartDate"]

def timing():
    start_time = time.time()
    return lambda x: print("[{:.2f}s] {}".format(time.time() - start_time, x))

t = timing()

def finra(fields,symbols):

    finraUrl = "https://api.finra.org/data/group/OTCMarket/name/weeklySummary"

    finraData = pd.DataFrame(columns = fields)

    for year in [2019,2020,2021]:
        for month in range(1, 12):
            startDate = str(year) + "-" + str(month).zfill(2) + "-" + "01"
            endDate = str(year) + "-" + str(month).zfill(2) + "-" + str(calendar._monthlen(year, month))

            tempQuery = {
            "fields":fields,  
            "limit" : 50000,
            "domainFilters": [{
            "fieldName" : "issueSymbolIdentifier",
            "values" : symbols
            }],
            "dateRangeFilters": [ {

                "startDate" : startDate,

                "endDate" : endDate,

                "fieldName" : "weekStartDate"

            } ]
            }

            query = json.dumps(tempQuery)

            r = requests.post(finraUrl, query)
            temp = pd.read_csv(StringIO(r.text))
            finraData = finraData.append(temp, ignore_index="True")
    t("Loaded {} rows of OTC data from FINRA".format(len(finraData)))
    return finraData

def price(finra):
    yf.pdr_override()
    df = finra

    df = df.drop(columns=['issueName', 'totalWeeklyShareQuantity','totalWeeklyTradeCount','marketParticipantName','tierIdentifier'])
    df = df.sort_values(by=['issueSymbolIdentifier','weekStartDate'])
    df = df.drop_duplicates()
    symbols = df["issueSymbolIdentifier"].drop_duplicates()

    priceColumns = ['issueSymbolIdentifier','weekStartDate','High', 'Low', 'Open', 'Close', 'Volume']
    priceData = pd.DataFrame(columns = priceColumns)
    priceData.to_csv('price.csv',index=False)
    for symbol in symbols:
            curr = df[df["issueSymbolIdentifier"].isin([symbol])]
            minDate = datetime.date.fromisoformat(curr['weekStartDate'].min())
            maxDate = datetime.date.fromisoformat(curr['weekStartDate'].max())
            maxDate = datetime.date.fromisoformat(curr['weekStartDate'].max()) + datetime.timedelta((5-maxDate.weekday()) % 7 ) #yahoo finance ignores the endDate in the result set.
            dataSet = pdr.get_data_yahoo(symbol,minDate,maxDate)
            for x in curr['weekStartDate']:
                    tempSet = dataSet
                    tempSet['Date'] = pd.to_datetime(tempSet.index)
                    # tempSet['Date'] = pd.to_datetime(tempSet['Date'])
                    startDate = datetime.date.fromisoformat(x)
                    endDate = startDate + datetime.timedelta( (4-startDate.weekday()) % 7 ) 
                    mask = (dataSet['Date'] >= str(startDate)) & (dataSet['Date'] <= str(endDate))
                    # print(mask)
                    temp = dataSet.loc[mask]
                    highPrice = temp['High'].max()
                    lowPrice = temp['Low'].min()
                    openPrice = temp['Open'].mean()
                    closePrice = temp['Close'].mean()
                    volume = temp['Volume'].sum()

                    newRow = {
                            'issueSymbolIdentifier':symbol,
                            'weekStartDate': startDate.isoformat(),
                            'High': highPrice,
                            'Low': lowPrice,
                            'Open': openPrice,
                            'Close': closePrice,
                            'Volume': volume
                            }
                    priceData = priceData.append(newRow, ignore_index="True",)
                    del startDate
                    del endDate
                    del highPrice
                    del lowPrice
                    del openPrice
                    del closePrice
                    del volume
                    del newRow
                    del tempSet
                    del temp
            del curr
    t("Loaded {} rows of pricing data from Yahoo Finance".format(len(priceData)))
    return priceData

print("Beginning processing run")
finra = finra(fields,symbols)
price = price(finra)

join = finra.merge(price, left_on=['issueSymbolIdentifier','weekStartDate'], right_on=['issueSymbolIdentifier','weekStartDate'])
join = join.sort_values(by=['issueSymbolIdentifier','weekStartDate'])

t("Merged {} rows".format(len(join)))

join.to_csv(filename,mode='w',index=False)
t("Exported {} rows".format(len(join)))
