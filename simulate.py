from pandas import read_sql, read_csv, DataFrame
import sqlite3
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from StringIO import StringIO
import requests_cache
import random

initial_bank = 15000

requests_cache.install_cache('simulation')

def get_price_history(date, symbol, time, index=False):

    start_date = datetime.strptime(date, '%Y-%m-%d')
    if time == 'After':
        start_date = start_date + relativedelta(days=1)

    start_dates = str(start_date - relativedelta(days=0)).split(' ')[0].split('-')
    #start_dates = str(start_date).split(' ')[0].split('-')


    if index == True:
        end_dates = str(start_date + relativedelta(days=2000)).split(' ')[0].split('-')
        url = 'http://real-chart.finance.yahoo.com/table.csv?s={6}&a={0}&b={1}&c={2}&d={3}&e={4}&f={5}&g=d&ignore=.csv'.format(int(start_dates[1])-1, start_dates[2], start_dates[0], int(end_dates[1])-1, end_dates[2], end_dates[0], symbol)
        data = requests.get(url).text
        df = read_csv(StringIO(data), index_col = 'Date')


        df = df[['Close']]

    else:
        end_dates = str(start_date + relativedelta(days=20)).split(' ')[0].split('-')
        url = 'http://real-chart.finance.yahoo.com/table.csv?s={6}&a={0}&b={1}&c={2}&d={3}&e={4}&f={5}&g=d&ignore=.csv'.format(int(start_dates[1])-1, start_dates[2], start_dates[0], int(end_dates[1])-1, end_dates[2], end_dates[0], symbol)

        try:
            data = requests.get(url).text
            df = read_csv(StringIO(data), index_col = 'Date')
            df = df[-10:]
            df = df[['Open', 'Close']]
            #df = df.reset_index()
        except Exception as e:
            print e


    return df

conn = sqlite3.connect('data2.sqlite', timeout=30)

choice = int(raw_input('Model: '))
trades = int(raw_input('Trades: '))
sql = 'select cutoff, cutoff2 from results where count > 300 order by mean desc limit %s,1;' % choice
df = read_sql(sql, conn)
cutoff = float(df['cutoff'])
cutoff2 = float(df['cutoff2'])

print cutoff, cutoff2
sql = 'select date,symbol,time,machine_score from earnings_calendar where machine_score>=%s and machine_score<=%s order by date desc limit %s' % (cutoff, cutoff2, trades)
df = read_sql(sql, conn)
df = df.iloc[::-1]
df = df.reset_index()
print df
index_history = get_price_history(df['date'][0], 'spy', 'after', True)


for i in range(len(df)):
    current = df.ix[i]

    price_history = get_price_history(current['date'], current['symbol'], current['time'])
    price_history = price_history[['Open', 'Close']]
    price_history['Score'] = current['machine_score']

    price_history.columns = [
        '%s %s Open' % (current['symbol'], current['date']),
        '%s %s Close' % (current['symbol'], current['date']),
        '%s %s Score' % (current['symbol'], current['date']),
        ]
    index_history = index_history.join(price_history)



index_history = index_history.iloc[::-1]
current_plays = {}
current_play_symbols = []
bank = initial_bank
values = []

for i in range(len(index_history)):
    current_date = index_history.ix[i,1:]
    current_date = current_date.dropna()

    # add plays
    for j in range(0,len(current_date),3):
        current_series = current_date.ix[j:j+3]

        symbol = current_series.index[0].split(' ')[0]
        if symbol not in current_play_symbols:
            current_play_symbols.append(symbol)
            open_price = current_series[0]
            close_price = current_series[1]
            percent_invested = current_series[2]/8.0

            shares = int((bank*percent_invested)/open_price)
            print shares
            data = {'num_shares': shares, 'open_price': open_price, 'close_price': close_price}
            current_plays[current_series.index[0]] = data

            bank -= ((shares * open_price) + 2.50)

        #update close prices
        if symbol in current_play_symbols:
            current_plays[current_series.index[0]]['close_price'] = current_series[1]



    # remove plays
    for j in current_plays.copy():
        symbol = j.split(' ')[0]
        if j not in current_date.index:
            bank += (current_plays[j]['num_shares']*current_plays[j]['close_price'])-2.50
            del current_plays[j]
            current_play_symbols.remove(symbol)

    # determine total sum of current_plays
    current_amount = 0
    for j in current_plays:
        current_amount += current_plays[j]['close_price']*current_plays[j]['num_shares']


    values.append(bank+current_amount)


index_history['Account_Value'] = values



# get index buy and hold strategy
buy_hold_shares = int(initial_bank/index_history['Close'][0])
values = []
for i in index_history['Close']:
    values.append(i*buy_hold_shares)
index_history['Buy_Hold_Value'] = values


index_history = index_history[['Buy_Hold_Value', 'Account_Value']]
index_history['Percent_Buy_Hold'] = (index_history['Buy_Hold_Value']-initial_bank)/initial_bank
index_history['Percent_Account_Value'] = (index_history['Account_Value']-initial_bank)/initial_bank
print index_history[['Percent_Buy_Hold', 'Percent_Account_Value']]
index_history.to_csv('results %s.csv' % trades)
