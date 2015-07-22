from pandas import read_sql, DataFrame
from random import shuffle
import sqlite3
from sklearn import svm
import numpy as np
import sys
from multiprocessing import Process, Queue

class Machine:
    def __init__(self, gamma, epsilon, c, cutoff1, cutoff2):
        self.gamma = gamma
        self.epsilon = epsilon
        self.c = c
        self.cutoff1 = cutoff1
        self.cutoff2 = cutoff2
        self.conn = sqlite3.connect('C:/Users/Nicholas/OneDrive/aitrader2/data2.sqlite', timeout=30)
        self.cursor = self.conn.cursor()

    def train(self, date):
        sql = 'Select key, percent_beat_eps, sue, average_change, percent_beat_eps_average, percent_beat_revs, ratio, distance_to_high, distance_to_low, distance_to_target, percent_change from earnings_calendar where (date<\'%s-01-01\' or Date>\'%s-12-31\') and open_price<=25 and open_price>=2'% (date, date)
        df = read_sql(sql, self.conn, index_col = 'key')
        df = df.dropna()

        # get percent change observations
        percent_change = df['percent_change'].copy()
        percent_change = percent_change*100
        df = df.drop('percent_change',1)

        # normalize
        df = (df - df.mean()) / (df.max() - df.min())

        # make machine, and train it
        clf = svm.SVR(gamma=self.gamma, epsilon = self.epsilon, C = self.c)
        clf.fit(df, percent_change)

        # store in the class
        self.clf = clf

    def store(self, date):
        sql = 'Select key, percent_beat_eps, sue, average_change, percent_beat_eps_average, percent_beat_revs, ratio, distance_to_high, distance_to_low, distance_to_target, percent_change from earnings_calendar where (date>\'%s-01-01\' and Date<\'%s-12-31\') and open_price<=25 and open_price>=2'% (date, date)
        df = read_sql(sql, self.conn, index_col = 'key')
        df = df.dropna(subset=['percent_beat_eps', 'sue', 'average_change', 'percent_beat_eps_average', 'percent_beat_revs', 'ratio', 'distance_to_high', 'distance_to_low', 'distance_to_target'])
        # get percent change observations
        percent_change = df['percent_change'].copy()
        percent_change = percent_change*100
        df = df.drop('percent_change',1)

        # normalize the data
        df = (df - df.mean()) / (df.max() - df.min())

        # make predictions
        results = []
        for i in range(len(df)):
            prediction = round(self.clf.predict(df.iloc[i])[0],1)
            self.cursor.execute('update earnings_calendar set `machine_score` = %s where key = %s' % (prediction, df.index[i]))
            results.append([prediction, percent_change.iloc[i]])
        self.conn.commit()

        df = DataFrame(results)
        result = df[(df[0]>=self.cutoff1) & (df[0]<=self.cutoff2)][1].describe()
        print result
