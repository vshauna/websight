import os
import requests
import sys
import time
import json
import settings
import xlrd
from decimal import Decimal
from sqlalchemy import func 
from models import session, ExchangeRateSnapshot, Trade
import datetime
import pytz
os.environ['OMP_NUM_THREADS'] = '4'
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import logging
logger = logging.getLogger('currency')
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

os.chdir(settings.BASE_DIR)
OER_API_URL = 'https://openexchangerates.org/api/latest.json?app_id={}'.format(settings.OER_ID)

def plot():
    l = (('localbitcoins 1h', 'shauna.website'),
         ('dolartoday', 'dolartoday'),
         ('interbanex', 'interbanex'),
         ('dicom', 'banco central de venezuela'),)
    plt.style.use('ggplot')
    fig, ax = plt.subplots(figsize=(12, 7))
    min_created_at = datetime.datetime.utcnow()
    max_created_at = datetime.datetime.fromtimestamp(0)
    for t in l:
        l = session.query(ExchangeRateSnapshot.created_at, ExchangeRateSnapshot.price).filter(ExchangeRateSnapshot.name==t[0]).filter(ExchangeRateSnapshot.source==t[1]).all()
        l = [(t[0], float(t[1])) for t in l]+[(datetime.datetime.utcnow(), float(l[-1][1]),)]
        l = [(t[0].replace(tzinfo=datetime.timezone.utc).astimezone(tz=pytz.timezone('America/Caracas')).replace(tzinfo=None), t[1]) for t in l]
        min_created_at = min([t[0] for t in l]+[min_created_at])
        max_created_at = max([t[0] for t in l]+[max_created_at])
        data = pd.DataFrame(l, columns=('date', '{} VES/USD'.format(t[0])))
        data.set_index('date', inplace=True)
        data.plot(ax=ax, title='precios VES/USD y VES/BTC')

    rows = session.query(Trade.date, Trade.amount, Trade.price).filter(Trade.date > min_created_at).all()
    d = Trade.aggregate_trades_rows(rows, by='hour')
    del rows
    l = [(key.replace(tzinfo=datetime.timezone.utc).astimezone(tz=pytz.timezone('America/Caracas')).replace(tzinfo=None), float(d[key]['mean_price']), float(d[key]['volume'])) for key in d]
    l = [(key, float(d[key]['mean_price']), float(d[key]['volume'])) for key in d]

    ax.legend(loc='center right', bbox_to_anchor=(0.0, 1.0))
    box = ax.get_position()
    ax.set_position([box.x0*1.65, box.y0, box.width * 0.85, box.height])

    ax2 = ax.twinx()
    box = ax2.get_position()
    ax2.set_position([box.x0*1.65, box.y0, box.width * 0.85, box.height])
    ax2.plot([t[0] for t in l], [t[1] for t in l], color='green', alpha=0.2, label='VES/BTC')

    ax2.legend(loc='center left', bbox_to_anchor=(1.0, 1.0))
    ax2.grid(False)
    plt.savefig(os.path.join(settings.MEDIA_DIR, 'vesusd.png'))



    data = pd.DataFrame(l, columns=('date', 'mean_price', 'volume'))
    data.set_index('date', inplace=True)

    data.plot(y='mean_price', ax=ax, title='precio ves/btc')
    plt.savefig(os.path.join(settings.MEDIA_DIR, 'mean_price.png'))


    fig, ax = plt.subplots(figsize=(12, 7))

    data.plot(y='volume', ax=ax, title='volumen localbitcoins de transacciones en VES', kind='area', stacked=True)
    plt.savefig(os.path.join(settings.MEDIA_DIR, 'volume.png'))
    

    fig, ax = plt.subplots(figsize=(12, 7))
    usdbtc_prices = session.query(ExchangeRateSnapshot.created_at, ExchangeRateSnapshot.price) \
                           .filter(ExchangeRateSnapshot.from_currency == 'BTC') \
                           .filter(ExchangeRateSnapshot.to_currency == 'USD') \
                           .filter(ExchangeRateSnapshot.source == 'openexchangerates') \
                           .filter(ExchangeRateSnapshot.created_at > min_created_at) \
                           .all()
    usdbtc_prices = [(x[0], 1/float(x[1])) for x in usdbtc_prices]

    data = pd.DataFrame(usdbtc_prices, columns=('date', 'usdbtc'))
    print(data)
    data.set_index('date', inplace=True)
    data.plot(y='usdbtc', ax=ax, title='ves/btc, usd/btc')
    for x in l:
        print(x)
    ax2 = ax.twinx()
    ax2.plot([x[0] for x in l], [x[1] for x in l], 'b', label='VES/BTC')
    ax2.legend(loc='center left', bbox_to_anchor=(1.0, 1.0))
    ax2.grid(False)
#    fig, ax = plt.subplots(figsize=(12, 7))
#    ax.plot(*usdbtc_prices)
    plt.savefig(os.path.join(settings.MEDIA_DIR, 'usdbtc.png'))
    


def fetch_dicom_bs_usd():
    r = requests.get('http://bcv.org.ve/sites/default/files/EstadisticasGeneral/2_1_2a19.xls')
    wb = xlrd.open_workbook(file_contents=r.content)
    sheet = wb.sheet_by_index(1)
    second_column = sheet.col(1)
    for i, cell in enumerate(second_column):
        if cell.value == 'USD':
            usd_row_position = i
            break

    usd_row = sheet.row(usd_row_position)
    return Decimal(str(usd_row[-1].value))

def fetch_interbanex_bs_usd():
    r = requests.get('https://api.interbanex.com/v1/public/instrument/ticker/USDVES', headers={'Origin': 'https://www.interbanex.com'})
    return r.json(parse_float=Decimal)['price']

def fetch_ads():
    ads_json = requests.get('https://localbitcoins.com/sell-bitcoins-online/ve/venezuela/.json').json()
    ads = ads_json['data']['ad_list']
    k = 0
    while 'next' in ads_json['pagination']:
        k += 1
        ads_json = requests.get(ads_json['pagination']['next']).json()
        ads += ads_json['data']['ad_list']
    return ads

def oer_prices():
    logger.debug('requesting %s', OER_API_URL)
    r = requests.get(OER_API_URL)
    logger.debug('got %s', OER_API_URL)
    return r.json(parse_float=Decimal)['rates']

def usdbtc_price():
    r = requests.get(OER_API_URL)
    rates = r.json()['rates']
    return 1/rates['BTC']

def fetch_ticker():
    url = 'https://localbitcoins.com/bitcoinaverage/ticker-all-currencies/'
    r = requests.get(url)
    return r.json()

def fiat_price(currency):
    ticker = fetch_ticker()
    btc_price = usdbtc_price()
    fiat_price = float(ticker[currency]['avg_1h'])/btc_price
    return fiat_price

def check_and_add_exchange_rate_snapshot(name, source, from_currency, to_currency, price):
    logger.debug('%s:%s price for %s/%s in is %s', source, name, from_currency, to_currency, repr(price))
    last_price = ExchangeRateSnapshot.latest_price(
        name=name,
        source=source,
        from_currency=from_currency,
        to_currency=to_currency)

    if last_price is not None:
        logger.debug('latest price in db for %s/%s with %s:%s is %s', from_currency, to_currency, source, name, repr(last_price))
    else:
        logger.debug('not found a price in db for %s/%s with %s:%s', from_currency, to_currency, source, name)
        
    logger.debug('comparing %s != %s', repr(last_price), repr(price))
    if last_price != price:
        logger.debug('adding exchange rate %s:%s %s/%s=%s', source, name, from_currency, to_currency, price)
        session.add(ExchangeRateSnapshot(
            name=name,
            source=source,
            from_currency=from_currency,
            to_currency=to_currency,
            price=price))
    else:
        logger.debug('not adding exchange rate %s:%s %s/%s because it hasn\'t changed', source, name, from_currency, to_currency)

if __name__ == '__main__':
    if sys.argv[1] == 'ads':
        ads = fetch_ads()
        ads_json = json.dumps(ads)
        with open('current_ads.json', 'w') as f:
            f.write(ads_json)

        
        with open('vesusd.json') as f:
            vesusd = json.loads(f.read())
        try:
            vesusd['interbanex'] = fetch_interbanex_bs_usd()
        except Exception as e:
            print(e)

        with open('vesusd.json', 'w') as f:
            f.write(json.dumps(vesusd))
    if sys.argv[1] == 'usdbtc':
        oer_prices = oer_prices()
        for currency in oer_prices:
            check_and_add_exchange_rate_snapshot('openexchangerates', 'openexchangerates', currency, 'USD', oer_prices[currency])
        session.commit()

        lbtc_vesusd = Trade.mean_time(60)*oer_prices['BTC']
        if lbtc_vesusd:
            check_and_add_exchange_rate_snapshot('localbitcoins 1h', 'shauna.website', 'VES', 'USD', lbtc_vesusd)
            session.commit()
        else:
            logger.error('not adding exchange rate %s:%s %s/%s because it\'s zero', 'localbitcoins 1h', 'shauna.website', 'VES', 'USD')

        dicom_price = fetch_dicom_bs_usd()
        check_and_add_exchange_rate_snapshot('dicom', 'banco central de venezuela', 'VES', 'USD', dicom_price)
        session.commit()

        try:
            dolartoday_price = requests.get('https://s3.amazonaws.com/dolartoday/data.json').json(parse_float=Decimal)['USD']
            for key in dolartoday_price:
                check_and_add_exchange_rate_snapshot(key, 'dolartoday', 'VES', 'USD', Decimal(dolartoday_price[key]))
        except Exception as e:
            raise e
        session.commit()

        interbanex_price = fetch_interbanex_bs_usd()
        check_and_add_exchange_rate_snapshot('interbanex', 'interbanex', 'VES', 'USD', interbanex_price)
        session.commit()
            
        with open('vesusd.json', 'w') as f:
            vesusd = {}
            l = (('dicom', 'banco central de venezuela', 'VES', 'USD'),
                 ('localbitcoins 1h', 'shauna.website', 'VES', 'USD'),
                 ('dolartoday', 'dolartoday', 'VES', 'USD'),
                 ('interbanex', 'interbanex', 'VES', 'USD'),)
            for p in l:
                if p[:2] == ('localbitcoins 1h', 'shauna.website'):
                    vesusd['localbitcoins'] = round(float(ExchangeRateSnapshot.latest_price(*p)), 2)
                else:
                    vesusd[p[0]] = round(float(ExchangeRateSnapshot.latest_price(*p)), 2)
            f.write(json.dumps(vesusd))
    if sys.argv[1] == 'plot':
        plot()
