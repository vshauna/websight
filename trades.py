import os
import time
import urllib.parse
import settings
import hmac
import hashlib
import requests
import sqlite3
import datetime
os.environ['OMP_NUM_THREADS'] = '4'
import numpy as np
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
from decimal import Decimal
from models import session, Trade
import logging
logger = logging.getLogger('trades')
logger.setLevel(logging.DEBUG)
c_handler = logging.StreamHandler()
c_handler.setLevel(logging.DEBUG)
f_handler = logging.FileHandler(os.path.join(settings.BASE_DIR, 'site.log'))
f_handler.setLevel(logging.DEBUG)
logger_format = logging.Formatter('%(asctime)s - %(filename)s:%(lineno)d - %(name)s - %(funcName)s - %(levelname)s - %(message)s')
c_handler.setFormatter(logger_format)
f_handler.setFormatter(logger_format)
logger.addHandler(c_handler)
logger.addHandler(f_handler)

LOCALBITCOINS_URL = 'https://localbitcoins.com'

def convert_old_database():
    conn = sqlite3.connect(settings.TRADES_DATABASE_NAME)
    c = conn.cursor()
    result = c.execute('SELECT * FROM trades')
    for i, row in enumerate(result):
        session.add(Trade(source_id=row[3],
                          date=datetime.datetime.utcfromtimestamp(int(row[0])),
                          amount=Decimal(row[2]),
                          price=Decimal(row[1]),
                          source='localbitcoins'))
        if i % 100000 == 0:
            logger.debug('added rows up to tid %d', row[3])
            session.commit()
        
    

def localbitcoins_headers(api_endpoint, params={}):
    nonce = int(time.time())
    get_or_post_params_urlencoded = urllib.parse.urlencode(params)
    message = str(nonce) + settings.LOCALBITCOIN_HMAC_KEY + api_endpoint + get_or_post_params_urlencoded
    message_bytes = message.encode('utf-8')
    signature = hmac.new(settings.LOCALBITCOIN_HMAC_SECRET.encode('utf-8'), msg=message_bytes, digestmod=hashlib.sha256).hexdigest().upper()
    headers = {'Apiauth-Key': settings.LOCALBITCOIN_HMAC_KEY,
               'Apiauth-Nonce': str(nonce),
               'Apiauth-Signature': signature,}
    return headers

def localbitcoins_trades(max_tid=None, currency=settings.DEFAULT_CURRENCY):
    if max_tid:
        params = {'max_tid': max_tid}
    else:
        params = {}
    endpoint = '/bitcoincharts/{}/trades.json'.format(currency)
    headers = localbitcoins_headers(endpoint, params)
    logger.debug('requesting %s', LOCALBITCOINS_URL+endpoint)
    r = requests.get(LOCALBITCOINS_URL+endpoint, params=params, headers=headers)
    logger.debug('got %s', LOCALBITCOINS_URL+endpoint)
    try:
        trades = r.json()
    except:
        raise Exception(r.text)
    return trades

def db_conn():
    conn = sqlite3.connect(settings.TRADES_DATABASE_NAME)
    c = conn.cursor()
    c.execute('SELECT name FROM sqlite_master WHERE type="table" AND name="trades"')
    if not c.fetchone():
        c.execute('CREATE TABLE "trades" (date integer, price text, amount text, tid integer primary key)')
        c.execute('CREATE INDEX trades_dates ON trades (date DESC)')
        c.execute('CREATE INDEX trades_tid ON trades (tid DESC)')
        conn.commit()
    return conn

def localbitcoins_store_in_db_all_new_trades():
    logger.debug('getting all existing tids from database')
    all_trades = session.query(Trade.source_id).all()
    existing_tids = [int(x[0]) for x in all_trades]
    logger.debug('got %d tids', len(existing_tids))
    oldest_max_tid = max(existing_tids)

    trades = localbitcoins_trades(max_tid=None)

    if not set([t['tid'] for t in trades]).issubset(existing_tids):
        added_tids = []
        not_added_tids = []
        for t in trades:
            if t['tid'] > oldest_max_tid:
                session.add(Trade(source_id=t['tid'],
                                  date=datetime.datetime.utcfromtimestamp(int(t['date'])),
                                  amount=Decimal(t['amount']),
                                  price=Decimal(t['price']),
                                  source='localbitcoins'))
                added_tids.append(t['tid'])
            else:
                not_added_tids.append(t['tid'])
        if added_tids:
            logger.debug('added tids %s to session', str(added_tids))
        if not_added_tids:
            logger.debug('not added tids %s to session', str(not_added_tids))
        session.commit()
        logger.debug('committed new records')
    else:
        logger.debug('trades table seems up to date')
        return

    max_tid = trades[-1]['tid']-1
    while len(trades) == 500 and not set([t['tid'] for t in trades]).issubset(existing_tids):
        logger.debug('max_tid for next request is %s', max_tid)
        trades = localbitcoins_trades(max_tid)
        added_tids = []
        not_added_tids = []
        for t in trades:
            if t['tid'] > oldest_max_tid:
                session.add(Trade(source_id=t['tid'],
                                  date=datetime.datetime.utcfromtimestamp(int(t['date'])),
                                  amount=Decimal(t['amount']),
                                  price=Decimal(t['price']),
                                  source='localbitcoins'))
                added_tids.append(t['tid'])
            else:
                not_added_tids.append(t['tid'])
        if added_tids:
            logger.debug('added tids %s to session', str(added_tids))
        if not_added_tids:
            logger.debug('not added tids %s to session', str(not_added_tids))
        session.commit()
        logger.debug('committed new records')
        max_tid = trades[-1]['tid']
    session.commit()

def aggregate_trades():
    data = {}
    conn = db_conn()
    c = conn.cursor()
    trades = c.execute('SELECT * FROM trades')
    for row in trades:
        trade = (datetime.datetime.utcfromtimestamp(int(row[0])), float(row[1]), float(row[2]), row[3])
        if trade[0].date() in data:
            data[trade[0].date()]['trades'].append((trade[1], trade[2]))
        else:
            data[trade[0].date()] = {'trades': [(trade[1], trade[2])]}

    for key in data:
        volume = sum([x[1] for x in data[key]['trades']])
        data[key]['mean'] = sum(trade[1]/volume*trade[0] for trade in data[key]['trades'])
        data[key]['volume'] = volume
        
    return data


if __name__ == '__main__':
#    convert_old_database()
    localbitcoins_store_in_db_all_new_trades()
    quit()
    data = aggregate_trades()
    fig, ax1 = plt.subplots()
    ax1.ticklabel_format(useOffset=False, style='plain', axis='y')
    ax1.plot(data.keys(), [x['mean'] for x in data.values()])
    ax1.set_yscale('log')
    ax2 = ax1.twinx()
    ax2.plot(data.keys(), [x['volume'] for x in data.values()], color='red', alpha=0.5)
    fig.tight_layout()
#    plt.savefig('/home/shauhedx/public_html/media/volume.png', dpi=150)
    plt.savefig('/home/shauhedx/public_html/media/price.png', dpi=150)
#    localbitcoins_store_in_db_all_new_trades()
