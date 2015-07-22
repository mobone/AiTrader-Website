from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy

import os
basedir = os.path.abspath(os.path.dirname(__file__))
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'data2.sqlite')
app.config['SQLALCHEMY_COMMIT_ON_TEARDOWN'] = True

db = SQLAlchemy(app)

class earnings_calendar(db.Model):
    __tablename__ = 'earnings_calendar'
    Key = db.Column(db.Integer, primary_key=True)
    Date = db.Column(db.String(15))
    Time = db.Column(db.String(10))
    Symbol = db.Column(db.String(12))
    Company = db.Column(db.String(100))
    Qtr = db.Column(db.String(6))
    EPS = db.Column(db.String(12))
    Cons = db.Column(db.String(12))
    Surprise = db.Column(db.String(12))
    Percent_Beat_EPS = db.Column(db.String(12))
    Revs = db.Column(db.String(10))
    Revs_Cons = db.Column(db.String(10))
    Percent_Beat_Revs = db.Column(db.String(12))
    Average_Change = db.Column(db.Float)
    Percent_Change = db.Column(db.Float)
    Machine_Score = db.Column(db.Integer)

    def __repr__(self):
        return self.Symbol

class user(db.Model):
    __tablename__ = 'news_memebrs'
    Key = db.Column(db.Integer, primary_key=True)
    Name = db.Column(db.String(40))
    Email = db.Column(db.String(100))

    def __repr__(self):
        return self.Symbol

db.create_all()
