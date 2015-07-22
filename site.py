from flask import Flask, render_template, request, flash
from flask.ext.mail import Message
from flask.ext.wtf import Form
from flask.ext.mail import Message, Mail
from wtforms import TextField, TextAreaField, SubmitField,  validators, ValidationError
from models import earnings_calendar, user, db

from pandas import read_csv, Series, read_sql
import time
from datetime import datetime
from sqlalchemy.sql.expression import func, select
import requests
import requests_cache
import re
import numpy as np
import sys

from dateutil.relativedelta import relativedelta
from sqlalchemy.sql import func
import sqlite3
import math
from StringIO import StringIO

mail = Mail()

app = Flask(__name__)


app.config["MAIL_SERVER"] = "smtp.gmail.com"
app.config["MAIL_PORT"] = 465
app.config["MAIL_USE_SSL"] = True
app.config["MAIL_USERNAME"] = 'nicholas.reichel@gmail.com'
app.config["MAIL_PASSWORD"] = 'C00kie32!'

mail.init_app(app)

app.secret_key = 'hardtocrackpassword?'

class ContactForm(Form):
    name = TextField("Name",  [validators.Required("Please enter your name.")])
    email = TextField("Email",  [validators.Required("Please enter your email address."), validators.Email("Please enter your email address.")])
    subject = TextField("Subject",  [validators.Required("Please enter a subject.")])
    message = TextAreaField("Message",  [validators.Required("Please enter a message.")])
    submit = SubmitField("Send")

class SignUpForm(Form):
    name = TextField("Name",  [validators.Required("Please enter your name.")])
    email = TextField("Email",  [validators.Required("Please enter your email address."), validators.Email("Please enter your email address.")])
    submit = SubmitField("Send")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/join', methods=['GET', 'POST'])
def join():

    form = SignUpForm()
    if request.method == 'POST':
        if form.validate() == False:
            flash('All fields are required.')
            return render_template('join.html', form=form)
        else:
            u = user(Name=form.name.data, Email=form.email.data)
            db.session.add(u)
            db.session.commit()
            return render_template('join.html', success=True)

    elif request.method == 'GET':
      return render_template('join.html', form=form)

@app.route('/contact', methods=['GET', 'POST'])
def contact():
    form = ContactForm()
    if request.method == 'POST':
        if form.validate() == False:
            flash('All fields are required.')
            return render_template('contact.html', form=form)
        else:
            msg = Message(form.subject.data, sender='nicholas.reichel@gmail.com', recipients=['nicholas.reichel@gmail.com'])
            msg.body = """
            From: %s <%s>
            %s
            """ % (form.name.data, form.email.data, form.message.data)
            mail.send(msg)

            return render_template('contact.html', success=True)

    elif request.method == 'GET':
      return render_template('contact.html', form=form)

@app.route('/stock/<symbol>/<date>/<time>')
def stock(symbol, date, time):

    start_date = datetime.strptime(date, '%Y-%m-%d')
    if time == 'After':
        start_date = start_date + relativedelta(days=1)
    print start_date
    start_dates = str(start_date - relativedelta(days=0)).split(' ')[0].split('-')


    end_dates = str(start_date + relativedelta(days=20)).split(' ')[0].split('-')

    url = 'http://real-chart.finance.yahoo.com/table.csv?s={6}&a={0}&b={1}&c={2}&d={3}&e={4}&f={5}&g=d&ignore=.csv'.format(int(start_dates[1])-1, start_dates[2], start_dates[0], int(end_dates[1])-1, end_dates[2], end_dates[0], symbol)

    try:
        data = requests.get(url).text
        df = read_csv(StringIO(data))
        df = df.drop(['Adj Close', 'Volume'],1)

        df = df.iloc[::-1]
        df = df.astype(str)
        print df
        df.columns = ['date', 'open', 'high', 'low', 'close']
        print df.to_dict('records')
        trade_dates = df['date'].values

    except Exception as e:
        print e


    return render_template('stock.html', time=time, date=date, trade_dates = trade_dates, chart_data = df.to_dict('records'))

@app.route('/dashboard/<trade_count>')
def dashboard(trade_count):
    (cutoff_1, cutoff_2) = 2.9,4.1

    trades = earnings_calendar.query.filter(earnings_calendar.Machine_Score >= cutoff_1, earnings_calendar.Machine_Score< cutoff_2).order_by(earnings_calendar.Date.desc()).limit(trade_count).all()
    df = read_csv('results %s.csv' % trade_count)

    df['Percent_Buy_Hold'] = df['Percent_Buy_Hold']
    df['Percent_Account_Value'] = df['Percent_Account_Value']
    simulation = df[['Date','Percent_Buy_Hold', 'Percent_Account_Value']].values


    small_sim = []
    divisions = len(simulation)/15
    for i in range(0, len(simulation), divisions):
        small_sim.append(simulation[i])


    conn = sqlite3.connect('data2.sqlite')

    df = read_sql('Select Percent_Change from earnings_calendar where Machine_Score>%s and Machine_Score<%s order by Date Desc limit %s ' % (cutoff_1, cutoff_2, trade_count), conn)

    doughnut = {}
    doughnut['Green'] = int(df[df>=.025].count())
    doughnut['Yellow'] = int(df[(df<.025) & (df >=-.03)].count())
    doughnut['Red'] = int(df[df<-.03].count())


    doughnut['Average'] = (sum(df.values)/float(len(df)))[0]*100
    doughnut['Median'] = float(df.median())*100


    return render_template('dashboard.html', trades=trades, simulation=small_sim, doughnut = doughnut)


@app.route('/calendar')
def calendar():

    return render_template('calendar.html')

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/analysis')
def analysis():

    x = earnings_calendar.query.filter_by(Date=cur_date).all()

    return render_template('analysis.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
