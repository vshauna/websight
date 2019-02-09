import os
from flask import Flask, render_template, request, jsonify
import json
from werkzeug.debug import DebuggedApplication
import sys
import datetime
import pytz
import settings
import sexycaracas
import localbtc
os.environ['OMP_NUM_THREADS'] = '4'
import numpy as np
from sqlalchemy import cast, Numeric
from models import session, SexProviderSnapshot, Trade
import random
import chaturbate
import locale
locale.setlocale(locale.LC_ALL, 'en_US')



CUTOFF_MINUTES = 10
TIMEZONE = pytz.timezone(datetime.datetime.now(datetime.timezone.utc).astimezone().tzname())

app = Flask(__name__)

def ads_get_params():
    params = {}

    amount = request.args.get('amount', None)
    if amount:
        params['amount'] = float(amount)

    bank_name = request.args.get('bank_name', None)
    if bank_name:
        params['bank_name'] = bank_name

    currency = request.args.get('currency', None)
    if currency:
        params['currency'] = currency

    params['symbol'] = request.args['symbol']
    return params

def ads_filter(ads, params):
    if 'amount' in params:
        if params['symbol'] != 'BTC':
            ads = [ad for ad in ads
                   if ((float(ad['data']['min_amount']) if ad['data']['min_amount'] is not None else float('-inf'))
                      <= params['amount']
                      <= (float(ad['data']['max_amount']) if ad['data']['max_amount'] is not None else float('inf')))]
        else:
            ads = [ad for ad in ads
                   if ((float(ad['data']['min_amount']) if ad['data']['min_amount'] is not None else float('-inf'))
                      <= params['amount']*float(ad['data']['temp_price'])
                      <= (float(ad['data']['max_amount']) if ad['data']['max_amount'] is not None else float('inf')))]
    if 'bank_name' in params:
        ads = [ad for ad in ads if params['bank_name'].casefold() in ad['data']['bank_name'].casefold()]
    if 'currency' in params:
        ads = [ad for ad in ads if params['currency'] == ad['data']['currency']]
    return ads

def fix_ad_time(ad):
    return datetime.datetime.strptime(ad['data']['profile']['last_online'][:22]+ad['data']['profile']['last_online'][23:], '%Y-%m-%dT%H:%M:%S%z')

@app.route('/localbitcoins-ads.json')
def ads():
    update = request.args.get('update', None)
    if update == 'yes':
        ads = localbtc.fetch_ads()
        ads_json = json.dumps(ads)
        with open('current_ads.json', 'w') as f:
            f.write(ads_json)
    with open('current_ads.json') as f:
        ads = json.loads(f.read())
    ads = ads_filter(ads, ads_get_params())
    file_date = datetime.datetime.fromtimestamp(os.stat('current_ads.json').st_mtime).replace(tzinfo=TIMEZONE)
    ads = (sorted([ad for ad in ads if file_date-fix_ad_time(ad) <= datetime.timedelta(minutes=CUTOFF_MINUTES)], key=lambda ad: -float(ad['data']['temp_price']))
          + sorted([ad for ad in ads if file_date-fix_ad_time(ad) > datetime.timedelta(minutes=CUTOFF_MINUTES)], key=lambda ad: -fix_ad_time(ad).timestamp()))
    return jsonify(ads)

@app.route('/sexycaracas.json')
def sexycaracas():
    providers = [p.for_public() for p in session.query(SexProviderSnapshot).filter(SexProviderSnapshot.available==True).all()]
    return jsonify(providers)

@app.route('/chaturbate.json')
def chaturbate():
    with open('chaturbate.json') as f:
        chaturbate = json.loads(f.read())
    return jsonify(chaturbate)

@app.route('/test')
def test():
    return render_template('test.html')

@app.route('/damas')
def damas():
    sex_providers = session.query(SexProviderSnapshot).filter(SexProviderSnapshot.available==True).filter(SexProviderSnapshot.gender=='woman').order_by(SexProviderSnapshot.price.asc()).all()
    result = []
    for p in sex_providers:
        old_prices = [int(x[0]) for x in session.query(SexProviderSnapshot.price).filter(SexProviderSnapshot.source_id==p.source_id).order_by(SexProviderSnapshot.created_at.asc()).all()]
        if len(old_prices) > 1:
            p.old_prices = old_prices[:-1]
            p.price_change = round((p.price/p.old_prices[-1]-1)*100, 2)
        else:
            p.old_prices = []
            p.price_change = 0
        p.price = int(p.price)
        result.append(p)
    
    return render_template('damas.html', sex_providers=result)

@app.route('/vesusd.json')
def vesusd():
    with open('vesusd.json') as f:
        vesusd = json.loads(f.read())
    return jsonify(vesusd)

@app.route('/')
def index():
    with open('sexycaracas.json') as f:
        providers = json.loads(f.read())
    with open('vesusd.json') as f:
        vesusd = json.loads(f.read())
    with open('chaturbate.json') as f:
        s = f.read()
        streams = json.loads(s)
    if request.args.get('cb', False) != False:
        cb = True
    else:
        cb = False
    sc = session.query(SexProviderSnapshot).filter(SexProviderSnapshot.available==True).order_by(SexProviderSnapshot.price.asc()).all()
    prices_sc = [float(p.price) for p in sc]
    stats_sc = {'mean': int(np.mean(prices_sc)), 'median': int(np.median(prices_sc)), 'min': min(prices_sc), 'max': max(prices_sc)}
    last_trades = Trade.last_trades(100, result=(Trade.date, Trade.amount, Trade.price))
    last_trades.reverse()
    last_trades = tuple((trade[0].replace(tzinfo=datetime.timezone.utc).astimezone(tz=pytz.timezone('America/Caracas')).replace(tzinfo=None),
                         locale.format('%.8f', trade[1], grouping=True),
                         locale.format('%.2f', trade[2], grouping=True),
                         locale.format('%.2f', trade[1]*trade[2], grouping=True))
                        for trade in last_trades)
    if streams['good']:
        stream = streams['good'][0]
    else:
        stream = None
    
    music_tags = ['<iframe style="border: 0; width: 100%; height: 120px;" src="https://bandcamp.com/EmbeddedPlayer/album=158016030/size=large/bgcol=ffffff/linkcol=0687f5/tracklist=false/artwork=small/transparent=true/" name="{}" seamless><a href="http://galaxie500.bandcamp.com/album/on-fire">On Fire by Galaxie 500</a></iframe>'.format(int(datetime.datetime.utcnow().timestamp())),
                  '<iframe style="border: 0; width: 100%; height: 120px;" src="https://bandcamp.com/EmbeddedPlayer/album=1696464046/size=large/bgcol=ffffff/linkcol=0687f5/tracklist=false/artwork=small/transparent=true/" name="{}" seamless><a href="http://pwelverumandsun.bandcamp.com/album/dont-wake-me-up">Don&#39;t Wake Me Up by the Microphones</a></iframe>'.format(int(datetime.datetime.utcnow().timestamp())),]
    return render_template('index.html', stats_sc=stats_sc, vesusd=vesusd, last_trades=last_trades, sc_urls=[sc[0].source_url, random.choice([x for x in sc if int(x.price) == stats_sc['median']]).source_url, sc[-1].source_url], stream=stream, music_tag=random.choice(music_tags), cb=cb)

app.debug = True
app = DebuggedApplication(app, True, pin_security=False)
if __name__ == '__main__':
    app.run(debug=False)
