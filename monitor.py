from multiprocessing import Process, Queue
from pandas import read_html, read_csv, to_datetime, DataFrame, Series, read_sql
import requests
import sqlite3
import sys
from datetime import date, datetime, timedelta
from StringIO import StringIO
import numpy as np
import requests_cache
import time
from machine_class import Machine
import math
requests_cache.install_cache('cache2')

class Announcement:
    def __init__(self, date, company, symbol, qtr, eps, cons, surprise, percent_beat_eps, revs, revs_cons, time, setup):
        self.read_date = date
        self.date = date
        self.company = company
        self.symbol = symbol
        self.qtr = qtr
        self.eps = eps
        self.cons = cons
        self.surprise = surprise

        self.price_target_df = read_csv('price_target.csv')
        self.price_target_df['Symbol'] = self.price_target_df['Symbol'].fillna(method='backfill')
        self.price_target_dfs = self.price_target_df.groupby('Symbol')

        self.percent_beat_eps = float(percent_beat_eps)/100.0
        self.revs = revs
        self.revs_cons = revs_cons
        self.time = time
        self.price_history = None
        self.average_change = None
        self.percent_beat_eps_average = None
        self.percent_beat_revs_average = None
        self.machine_score = None
        self.setup = setup

        self.control()

    def control(self):
        self.make_date()
        self.get_price_history()
        if self.price_history is None:
            return

        if self.setup:
            self.get_change()
        else:
            self.get_open()

        self.get_sue()
        self.get_high_low()
        if self.date<=to_datetime('04/01/2015', format="%m/%d/%Y").date():
            self.get_price_target_sheet()
        else:
            self.get_price_target()
        self.get_rev_surprise()
        self.get_ratio()
        self.get_average()

        del(self.price_history)
        del(self.setup)
        del(self.price_target_dfs)
        del(self.price_target_df)
        self.send_to_db()


    def make_date(self):
        date = self.date
        if self.time == 'After':
            date = date + timedelta(days=1)
            while date.weekday()>=5:
                date = date + timedelta(days=1)
        self.date = date

    def get_price_history(self):
        try:
            price_text = requests.get('http://real-chart.finance.yahoo.com/table.csv?s=%s&d=1&e=&f=2035&g=d&a=3&b=19&c=2009&ignore=.csv' % self.symbol).text
            price_df = read_csv(StringIO(price_text), sep=',')
            self.price_history = price_df
        except Exception as e:
            pass


    def get_open(self):
        with requests_cache.disabled():
            price_text = requests.get('http://finance.yahoo.com/q?s=%s&ql=1' % self.symbol).text
            print self.symbol
            start = price_text.find('<span class="time_rtq_ticker">')
            end = price_text.find('</span>', start)
            self.open_price = float(price_text[start+50+len(self.symbol):end])

    def get_change(self):
        try:
            start_date = str(self.date).split(' ')[0]
            self.open_price = float(self.price_history[self.price_history['Date'] == start_date]['Open'])
            end_loc = self.price_history[self.price_history['Date'] == start_date].index-10
            self.close_price = float(self.price_history.iloc[end_loc]['Close'])
            self.percent_change = (float(self.close_price)-float(self.open_price))/float(self.open_price)
        except:
            pass

    def get_high_low(self):
        start_date = str(self.date).split(' ')[0]
        if self.setup:
            start_date = self.price_history[self.price_history['Date'] == start_date].index.values
        else:
            start_date = 0

        end_date = start_date + 252

        if len(start_date)==0 or len(end_date)==0:
            return

        try:
            high = float(self.price_history[start_date:end_date]['Adj Close'].max())
            low =  float(self.price_history[start_date:end_date]['Adj Close'].min())
            self.distance_to_high = np.std([high, float(self.open_price)])
            self.distance_to_low = np.std([low, float(self.open_price)])

            #self.distance_to_high = round(self.distance_to_high,5)
            #self.distance_to_low = round(self.distance_to_low,5)
        except Exception as e:
            #print e
            pass

    def get_sue(self):
        try:
            self.sue = np.std([float(str(self.eps).replace('$','')), float(str(self.cons).replace('$',''))])
            #self.sue = round(self.sue,5)
        except Exception as e:
            # usually cannot convert from foreign currency to float
            pass

    def get_price_target_sheet(self):



        try:
            cur_df = self.price_target_dfs.get_group(str(self.symbol)).transpose().drop('Symbol')
            cur_df.columns = ['Date','Amount']
            cur_df['Date'] = to_datetime(cur_df['Date'], format="%m/%d/%Y")
            cur_df['Date'].astype('datetime64[ns]')


            self.target = float(cur_df[cur_df['Date']<self.date]['Amount'][-1:])
            self.distance_to_target = np.std([self.target, self.open_price])
            #self.distance_to_target = round(self.distance_to_target,5)
        except Exception as e:
            pass

    def get_price_target(self):
        try:
            target_dfs = read_html('http://finance.yahoo.com/q?s=%s&ql=1' % self.symbol)[1]
            target_dfs[0] = target_dfs[0].astype(str)

            self.target = float(target_dfs.iloc[4,1])
            self.distance_to_target = np.std([self.target, self.open_price])
            #self.distance_to_target = round(self.distance_to_target,5)
        except Exception as e:
            #print e
            pass

    def get_rev_surprise(self):
        try:
            self.percent_beat_revs = (self.revs-self.revs_cons)/self.revs_cons
            #self.percent_beat_revs = round(self.percent_beat_revs,5)
        except:
            pass

    def get_ratio(self):
        try:
            self.ratio = self.percent_beat_eps/self.sue
            #self.ratio = round(self.ratio,5)
        except Exception as e:
            #print e
            pass

    def get_average(self):
        conn = sqlite3.connect('data2.sqlite', timeout=30)
        c = conn.cursor()
        current_df = read_sql('select symbol,`percent_beat_eps`, `percent_change` from earnings_calendar where symbol = \'%s\' and `date`<\'%s\'' % (self.symbol, self.read_date), conn)
        current_df = current_df.dropna()
        if len(current_df)<3:
            return
        self.average_change = sum(current_df['percent_change'])/float(len(current_df['percent_change']))
        self.percent_beat_eps_average = sum(current_df['percent_beat_eps'])/float(len(current_df['percent_beat_eps']))

        #self.average_change = round(self.average_change,5)
        #self.percent_beat_eps_average = round(self.percent_beat_eps_average,5)


    def send_to_db(self):
        conn = sqlite3.connect('data2.sqlite', timeout=30)
        c = conn.cursor()
        df = DataFrame(self.__dict__.items(), index=self.__dict__.keys())
        df = df.drop(0,1)
        df = df.transpose()
        df = df.sort(axis=1)
        df.to_sql('earnings_calendar', conn, if_exists='append', index=False)

def get_street_insider(read_date):
    try:
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.118 Safari/537.36'}
        data = requests.get('http://www.streetinsider.com/ec_earnings.php?day=%s' % str(read_date).split(' ')[0], headers=headers).text

        dfs = read_html(data, header=0)

    except:
        print 'No earnings found'
        return None
    return dfs

def clean(df):
    replace_me = ['\%', '$', 'M', 'B', 'K']
    df2 = df.copy()

    for i in replace_me:
        df2['Percent_Beat_EPS'] = df2['Percent_Beat_EPS'].str.replace(i, '')
        df2['Revs'] = df2['Revs'].str.replace(i, '')
        df2['Revs_Cons'] = df2['Revs_Cons'].str.replace(i, '')

    convert_me = ['Percent_Beat_EPS', 'Revs', 'Revs_Cons']
    for i in convert_me:
        df2[i] = df2[i].convert_objects(convert_numeric=True)

    return df2

def get_announcements(read_date, setup=False):
    print read_date
    dfs = get_street_insider(read_date)

    if dfs is None:
        return

    if setup == True:
        dfs[0]['Time'] = 'Before'
        total_df = dfs[0]
        if len(dfs)==2:
            dfs[1]['Time'] = 'After'
            total_df = total_df.append(dfs[1])
        total_df = total_df.drop(['Details', 'Gd.', '% Since', '% Week'], 1)
        total_df.columns = ['Company', 'Symbol', 'Qtr', 'EPS', 'Cons', 'Surprise', 'Percent_Beat_EPS', 'Revs', 'Revs_Cons', 'Time']

        total_df = total_df.dropna()
        total_df = total_df.drop_duplicates(subset='Symbol')
        if len(total_df) == 0:
            print 'No earnings found'
            return
        total_df = clean(total_df)




        for i in total_df.values:
            Announcement(read_date,i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7],i[8],i[9], setup)
    else:
        # get this mornings earnings
        dfs[0]['Time'] = 'Before'
        df = dfs[0]
        df = df.drop(['Details', 'Gd.', '% Since', '% Week'], 1)
        df = df.dropna()
        df.columns = ['Company', 'Symbol', 'Qtr', 'EPS', 'Cons', 'Surprise', 'Percent_Beat_EPS', 'Revs', 'Revs_Cons', 'Time']
        for i in df.values:
            Announcement(read_date,i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7],i[8],i[9], setup)

        #get yesterdays earnings, after close
        read_date = read_date - timedelta(days=1)
        print read_date
        dfs = get_street_insider(read_date)
        if len(dfs)==2:
            dfs[1]['Time'] = 'After'
            df = dfs[1]
        df = df.drop(['Details', 'Gd.', '% Since', '% Week'], 1)
        df = df.dropna()
        df.columns = ['Company', 'Symbol', 'Qtr', 'EPS', 'Cons', 'Surprise', 'Percent_Beat_EPS', 'Revs', 'Revs_Cons', 'Time']

        for i in df.values:
            Announcement(read_date,i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7],i[8],i[9], setup)





def worker(date_queue):
    while date_queue.qsize()>0:
        read_date = date_queue.get()
        get_announcements(read_date, setup=True)



if __name__ == '__main__':
    conn = sqlite3.connect('data2.sqlite', timeout=30)
    if sys.argv[1] == 'setup':
        try:
            c = conn.cursor()
            c.execute('delete from earnings_calendar')
            conn.commit()
        except:
            pass
        read_date = date(2011, 01, 01)
        date_queue = Queue()
        while read_date < date(2015, 7, 11):
            read_date = read_date + timedelta(days=1)
            if read_date.isoweekday() not in range(1, 6):
                continue
            date_queue.put(read_date)

        for i in range(10):
            p = Process(target=worker, args=(date_queue,))
            p.start()
    elif sys.argv[1] == 'store':

        sql = 'select mean, gamma, epsilon, c, cutoff, cutoff2 from results where count > 300 order by mean desc limit 1;'
        df = read_sql(sql, conn)
        print df

        x = Machine(float(df['Gamma']), float(df['epsilon']), float(df['c']), float(df['cutoff']), float(df['cutoff2']))
        for date in [2012, 2013, 2014, 2015]:
            x.train(date)
            x.store(date)

    elif sys.argv[1] == 'update':
        read_date = datetime.now().date()
        get_announcements(read_date)
