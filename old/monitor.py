
from pandas import read_html, read_csv, read_sql, to_datetime, rolling_mean
import sqlite3
from StringIO import StringIO
from datetime import date, datetime, timedelta
from multiprocessing import Process, Queue
import requests
import requests_cache
import re
import sys
import math
import numpy as np
from time import sleep
requests_cache.install_cache('cache1')

def get_high_low(df, start_date):

    start_date = df[df['Date'] == start_date].index.values
    end_date = start_date + 252

    year_max = float(df[start_date:end_date]['Adj Close'].max())
    year_min =  float(df[start_date:end_date]['Adj Close'].min())


    return year_max, year_min

def get_change(df):
    df = df.drop_duplicates(subset='Symbol')
    df = df.set_index('Symbol')

    for j in df.index:
        date = datetime.strptime(df.loc[j, 'Date'], '%Y-%m-%d')
        symbol = j

        if df.loc[j, 'Time'] == 'After':
            date = date + timedelta(days=1)

        text = requests.get('http://real-chart.finance.yahoo.com/table.csv?s=%s&d=1&e=&f=2035&g=d&a=3&b=19&c=2005&ignore=.csv' % symbol).text
        try:
            price_df = read_csv(StringIO(text), sep=',')
        except Exception as e:
            # common error here is company is no longer in business
            continue

        while date.weekday()>=5:
            date = date + timedelta(days=1)

        try:
            df.loc[j, 'SUE'] = np.std([float(str(df.loc[j, 'EPS']).replace('$','')), float(str(df.loc[j, 'Cons']).replace('$',''))])
        except:
            # usually cannot convert from foreign currency to float
            pass
        start_date = str(date).split(' ')[0]
        start_price = price_df[price_df['Date'] == start_date]['Open']


        if len(start_price)==0:
            continue

        df.loc[j,'Start_Price'] = float(start_price)

        (high,low) = get_high_low(price_df, start_date)
        df.loc[j, 'Distance_To_High'] = np.std([high, float(start_price)])
        df.loc[j, 'Distance_To_Low'] = np.std([low, float(start_price)])


        end_loc = price_df[price_df['Date'] == start_date].index
        end_loc -= 10
        if end_loc>0:
            end_price = price_df.iloc[end_loc]['Close']
        else:
            continue


        df.loc[j,'Close_Price'] = float(end_price)
        change = (float(end_price)-float(start_price))/float(start_price)
        df.loc[j,'Percent_Change'] = change


    return df

def get_target_price_sheet(df):
    price_target_df = read_csv('price_target.csv')
    price_target_df['Symbol'] = price_target_df['Symbol'].fillna(method='backfill')
    price_target_dfs = price_target_df.groupby('Symbol')

    for j in df.index:
        date = datetime.strptime(df.loc[j, 'Date'], '%Y-%m-%d')
        symbol = j

        # skip if start price is none, ie, company not in business
        if df.loc[j,'Start_Price'] is None:
            continue

        try:
            cur_df = price_target_dfs.get_group(str(symbol)).transpose()
        except:
            continue
        cur_df = cur_df.drop('Symbol')
        cur_df.columns = ['Date','Amount']
        cur_df['Date'] = to_datetime(cur_df['Date'], format="%m/%d/%Y")
        cur_df['Date'].astype('datetime64[ns]')
        if date<=to_datetime('04/01/2015', format="%m/%d/%Y"):
            target = float(cur_df[cur_df['Date']<date]['Amount'][-1:])
            df.loc[j,'Price_Target'] = target

            df.loc[j, 'Distance_To_Target'] = np.std([target, df.loc[j,'Start_Price']])


    return df

def get_rev_surprise(df):
    replace_me = ['\%', '$', 'M', 'B', 'K']
    if len(df) == 0:
        return df

    df2 = df.copy()

    for i in replace_me:
        df2['Percent_Beat_EPS'] = df2['Percent_Beat_EPS'].str.replace(i, '')
        df2['Revs'] = df2['Revs'].str.replace(i, '')
        df2['Revs_Cons'] = df2['Revs_Cons'].str.replace(i, '')

    convert_me = ['Percent_Beat_EPS', 'Revs', 'Revs_Cons']
    for i in convert_me:
        df2[i] = df2[i].convert_objects(convert_numeric=True)

    # calculate some things
    df['Percent_Beat_Revs'] = (df2['Revs']-df2['Revs_Cons'])/df2['Revs_Cons']
    df['Percent_Beat_EPS'] = df2['Percent_Beat_EPS'] / 100
    df['Ratio'] = df['Percent_Beat_EPS']/df['SUE']

    return df

def price_target_getter(df):

    for j in df.index:
        date = datetime.strptime(df.loc[j, 'Date'], '%Y-%m-%d')
        symbol = j

        if df.loc[symbol,'Price_Target'] is None and date > datetime.strptime('2015-03-31', '%Y-%m-%d'):
            try:
                target = read_html('http://finance.yahoo.com/q/ao?s=%s+Analyst+Opinion' % symbol)[6][1][1]
                df.loc[symbol, 'Price_Target'] = target
                df.loc[j, 'Distance_To_Target'] = np.std([target, df.loc[j,'Start_Price']])
            except:
                continue

    return df

def average_getter(symbol, read_date):
    conn = sqlite3.connect('data.sqlite', timeout=30)
    c = conn.cursor()

    current_df = read_sql('select Symbol,`Percent_Beat_EPS`, `Percent_Change` from earnings_calendar where Symbol = \'%s\' and `Date`<\'%s\'' % (symbol, read_date), conn)
    current_df = current_df.dropna()
    if len(current_df)<3:
        return
    try:
        average_change = sum(current_df['Percent_Change'])/float(len(current_df['Percent_Change']))
        percent_beat_average = sum(current_df['Percent_Beat_EPS'])/float(len(current_df['Percent_Beat_EPS']))
        #avg_std = np.std(current_df['Percent_Beat_EPS'])
        if math.isnan(float(average_change)):
            return
        c.execute('update earnings_calendar set `Average_Percent_Beat_EPS` = %s, `Average_Change` = %s where Symbol = \'%s\' and Date=\'%s\' ' % (percent_beat_average, average_change, symbol, read_date))


        conn.commit()
    except Exception as e:
        print e
        pass

def worker(date_queue):
    conn = sqlite3.connect('data.sqlite', timeout=30)
    c = conn.cursor()

    while date_queue.qsize()>0:
        read_date = date_queue.get()
        print read_date
        current_df = get_todays_symbols(read_date)
        if current_df is not None:
            current_df.to_sql('earnings_calendar', conn, if_exists='append')
            for i in current_df.index.values:
                average_getter(i, read_date)

def get_todays_symbols(read_date):

    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.118 Safari/537.36'}

    data = requests.get('http://www.streetinsider.com/ec_earnings.php?day=%s' % str(read_date).split(' ')[0], headers=headers).text

    try:
        dfs = read_html(data, header=0)
    except:
        print 'No earnings found'
        # commmon error, no earnings reports found for this date
        return None

    if len(dfs) == 0:
        return None

    dfs[0]['Time'] = 'Before'
    total_df = dfs[0]


    if len(dfs)==2:
        dfs[1]['Time'] = 'After'
        total_df = total_df.append(dfs[1])

    total_df = total_df.drop(['Details', 'Gd.', '% Since', '% Week'], 1)


    total_df = total_df.dropna()

    total_df.columns = ['Company', 'Symbol', 'Qtr', 'EPS', 'Cons', 'Surprise', 'Percent_Beat_EPS', 'Revs', 'Revs_Cons', 'Time']

    total_df['SUE'] = None
    total_df['Start_Price'] = None
    total_df['Date'] = str(read_date)
    total_df['Average_Change'] = None
    total_df['Machine_Score'] = None
    total_df['Average_Percent_Beat_EPS'] = None
    total_df['Distance_To_High'] = None
    total_df['Distance_To_Low'] = None
    total_df['Price_Target'] = None
    total_df['Distance_To_Target'] = None
    total_df['Percent_Change'] = None
    total_df = get_change(total_df)

    total_df = get_rev_surprise(total_df)
    total_df = get_target_price_sheet(total_df)
    total_df = price_target_getter(total_df)

    return total_df


if __name__ == '__main__':
    conn = sqlite3.connect('data.sqlite', timeout=30)
    c = conn.cursor()



    if len(sys.argv) == 2 and sys.argv[1] == 'setup':
        try:
            c.execute('delete from earnings_calendar')
            conn.commit()
        except:
            pass


        read_date = date(2011, 01, 01)
        total_df = None

        date_queue = Queue()
        while read_date < date(2015, 7, 9):
            read_date = read_date + timedelta(days=1)

            if read_date.isoweekday() not in range(1, 6):
                continue
            date_queue.put(read_date)

        for i in range(20):
            p = Process(target=worker, args=(date_queue,))
            p.start()
    else:
        read_date = datetime.now()
        read_date = str(read_date).split(' ')[0]
        print read_date
        current_df = get_todays_symbols(read_date)
        if current_df is not None:
            current_df.to_sql('earnings_calendar', conn, if_exists='append')
            for i in current_df.index.values:
                average_getter(i, read_date)
