from pandas import read_sql, DataFrame
from random import shuffle
import sqlite3
from sklearn import svm
import numpy as np
import sys
from multiprocessing import Process, Queue

def simulate(item, store_it):
    conn = sqlite3.connect('data2.sqlite', timeout=30)
    cursor = conn.cursor()

    # setup params
    (open_price, price, gamma, epsilon , c, cutoff, cutoff2) = item

    # check if we even need to find this item

    if store_it == False:
        sql = 'select * from results where open_price = %s and price = %s and gamma = %s and epsilon = %s and c = %s and cutoff = %s and cutoff2 = %s' % (open_price, price, gamma, epsilon , c, cutoff, cutoff2)
        try:
            if len(read_sql(sql,conn)) > 0:
                print 'Already found %s' % item
                return
        except:
            pass

    results = []

    #iterate through dates
    dates = [2012,2013,2014,2015]
    for i in range(len(dates)):
        sql = 'select key, percent_beat_eps, sue, average_change, percent_beat_eps_average, percent_beat_revs, ratio, distance_to_high, distance_to_low, distance_to_target, percent_change from earnings_calendar where (date<\'%s-01-01\' or date>\'%s-12-31\') and open_price<=%s and open_price>=%s' % (dates[i], dates[i], price, open_price)
        df = read_sql(sql, conn, index_col = 'key')

        df = df.dropna()


        # get percent change observations
        percent_change = df['percent_change'].copy()
        percent_change = percent_change*100
        df = df.drop('percent_change',1)

        # normalize
        df = (df - df.mean()) / (df.max() - df.min())

        # make machine, and train it
        clf = svm.SVR(gamma=gamma, epsilon = epsilon, C = c)
        clf.fit(df, percent_change)

        # test the machine
        sql = 'select key, percent_beat_eps, sue, average_change, percent_beat_eps_average, percent_beat_revs, ratio, distance_to_high, distance_to_low, distance_to_target, percent_change from earnings_calendar where (date>\'%s-01-01\' and date<\'%s-12-31\') and open_price<=%s and open_price>=%s' % (dates[i], dates[i], price, open_price)

        df = read_sql(sql, conn, index_col = 'key')
        if store_it == False:
            df = df.dropna()
        else:
            df = df.dropna(subset=['percent_beat_eps', 'sue', 'average_change', 'percent_beat_eps_average', 'percent_beat_revs', 'ratio', 'distance_to_high', 'distance_to_low', 'distance_to_target'])

        # get percent change observations
        percent_change = df['percent_change'].copy()
        percent_change = percent_change*100
        df = df.drop('percent_change',1)

        # normalize the data
        df = (df - df.mean()) / (df.max() - df.min())

        # make predictions
        for i in range(len(df)):
            prediction = round(clf.predict(df.iloc[i])[0])
            if store_it:
                cursor.execute('update earnings_calendar set `Machine_Score` = %s where Key = %s' % (prediction, df.index[i]))

            results.append([prediction, percent_change.iloc[i]])

    if store_it:
        conn.commit()

    # get results
    df = DataFrame(results)
    result = df[(df[0]>=cutoff) & (df[0]<=cutoff2)][1].describe()
    if result['count']<50:
        return

    # modify output them
    result['Start_Price'] = open_price
    result['Price'] = price
    result['Gamma'] = gamma
    result['epsilon'] = epsilon
    result['c'] = c
    result['cutoff'] = cutoff
    result['cutoff2'] = cutoff2
    columns = result.index
    result = result.reset_index()
    result = result.transpose()
    result.columns = columns
    result = result.drop('index')

    # print and store results
    print float(result['mean']) , float(result['std'])
    result.to_sql('results', conn, if_exists='append', index=False)

    if float(result['mean'])>3.5:
        df.to_csv('result/results %s.csv' % float(result['mean']), index=False)

def worker(combo_queue):
    conn = sqlite3.connect('data2.sqlite', timeout=30)
    c = conn.cursor()
    store_it = False
    while combo_queue.qsize()>0:
        item = combo_queue.get()
        simulate(item, store_it)

if __name__ == '__main__':

    conn = sqlite3.connect('data2.sqlite', timeout=30)
    cursor = conn.cursor()

    if len(sys.argv)>=2 and sys.argv[1] == 'store':
        cursor.execute('update earnings_calendar set `Machine_Score` = Null')
        conn.commit()
        choice = int(raw_input('Choice: '))
        sql = 'select open_price, Price, Gamma, epsilon, c, cutoff, cutoff2 from results where count > 300 order by mean/std desc limit %s, 1;' % choice
        print sql
        df = read_sql(sql, conn)
        print df
        simulate(df.values[0], store_it = True)
    else:
        try:
            cursor.execute('delete from results')
            conn.commit()
        except:
            pass

        combos = []

        for cutoff in range(0,4):
            for cutoff2 in range(1,6):
                if cutoff2<cutoff: continue
                combos.append([2, 25,100.0, .1, 1, cutoff, cutoff2])

        shuffle(combos)
        #for item in combos:
            #simulate(item)

        combo_queue = Queue()
        print len(combos)
        for i in combos:
            combo_queue.put(i)


        for i in range(10):
            p = Process(target=worker, args=(combo_queue,))
            p.start()
