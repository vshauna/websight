import os
import json
import requests
import re
from bs4 import BeautifulSoup
import settings
from models import session, CategorySnapshot, SexProviderSnapshot, ProductSnapshot
import pprint
from decimal import Decimal
import mimetypes
import hashlib
import io
import logging
import datetime
logger = logging.getLogger('sexycaracas')
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

pp = pprint.PrettyPrinter()
os.chdir(settings.BASE_DIR)

def compare_files(file_a, file_b):
    def hash_bytestr_iter(bytesiter, hasher, ashexstr=False):
        for block in bytesiter:
            hasher.update(block)
        return hasher.hexdigest() if ashexstr else hasher.digest()

    def file_as_blockiter(afile, blocksize=65536):
        with afile:
            block = afile.read(blocksize)
            while len(block) > 0:
                yield block
                block = afile.read(blocksize)

    hash_a = hash_bytestr_iter(file_as_blockiter(file_a), hashlib.sha256())
    hash_b = hash_bytestr_iter(file_as_blockiter(file_b), hashlib.sha256())
    return hash_a == hash_b


def fetch():
    urls = ('http://www.sexycaracas.com/sexyccs/p_index.php?ids=1&WHERE=edad&/Damas',
            'http://www.sexycaracas.com/sexyccs/p_index.php?ids=5&WHERE=edad&/Trans',
            'http://www.sexycaracas.com/sexyccs/p_index.php?ids=6&WHERE=edad&/Hombres',)
    types = ['woman', 'transwoman', 'man']
    providers = []
    for url, type in zip(urls, types):
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        provider_div = soup.find_all(class_='thumbnailSinBorde')
        for div in provider_div:
            provider = {}
            provider['type'] = type
            provider['name'] = div.find(id='TextoAguamarina').text
            provider['url'] = 'http://www.sexycaracas.com/sexyccs/'+div.find('a')['href']
            tooltip = div.find('span', class_='span-centrado')
            provider['url_pic'] = 'http://www.sexycaracas.com'+re.search('url\((.*?)\)', tooltip['style']).group(1)
            provider['source_id'] = div.parent.find('input', {'name': 'ids_comparar[]'})['value']
            provider['age'] = int(tooltip['title'].split('\n')[0].split()[0])
            try:
                if tooltip['title'].split()[-2] != 'De':
                    provider['price'] = tooltip['title'].split()[-2]
                    provider['phone_number'] = tooltip['title'].split('\n')[2].split()[0]
                else:
                    provider['price'] = tooltip['title'].split()[-3]
                    provider['phone_number'] = tooltip['title'].split('\n')[2].split()[0]
            except ValueError:
                raise Exception(tooltip['title'])

            providers.append(provider)
        
    urls = ('http://www.sexycaracas.com/sexyccs/p_index.php?ids=1&WHERE={}&/Damas',
            'http://www.sexycaracas.com/sexyccs/p_index.php?ids=5&WHERE={}&/Trans',
            'http://www.sexycaracas.com/sexyccs/p_index.php?ids=6&WHERE={}&/Hombres',)
    source_keys = ('peso', 'estatura')
    keys = ('weight', 'height')
    for url in urls:
        for source_key, key in zip(source_keys, keys):
            r = requests.get(url.format(source_key))
            soup = BeautifulSoup(r.text, 'html.parser')
            provider_div = soup.find_all(class_='thumbnailSinBorde')
            for div in provider_div:
                tooltip = div.find('span', class_='span-centrado')
                source_id = div.parent.find('input', {'name': 'ids_comparar[]'})['value']
                for provider in providers:
                    if provider['source_id'] == source_id:
                        target_provider = provider
                        break
                provider['source_id'] = div.parent.find('input', {'name': 'ids_comparar[]'})['value']
                provider[key] = tooltip['title'].split('\n')[0].split()[0]
    
    return providers 


def create_categories():
    urls = ('http://www.sexycaracas.com/sexyccs/p_index.php?ids=1&/Damas',
            'http://www.sexycaracas.com/sexyccs/p_index.php?ids=5&/Trans',
            'http://www.sexycaracas.com/sexyccs/p_index.php?ids=6&/Hombres')
    names = ('Damas', 'Trans', 'Hombres')
    sources_ids = (1, 5, 6)

    if not session.query(CategorySnapshot).all():
        for url, name, source_id in zip(urls, names, sources_ids):
            session.add(CategorySnapshot(source_id=source_id, name=name, source='sexycaracas', source_url=url))
    session.commit()

def store_providers(providers):
    source_ids = tuple(p['source_id'] for p in providers)
    existing_available_source_ids = session.query(ProductSnapshot.source_id) \
                                        .filter(ProductSnapshot.available == True) \
                                        .all()
    logger.debug('available providers source ids: %s', source_ids)
    existing_available_source_ids = [p[0] for p in existing_available_source_ids]
    logger.debug('existing providers source ids: %s', existing_available_source_ids)
    disappeared_source_ids = list(set(existing_available_source_ids)-set(source_ids))
    logger.debug('disappeared providers source ids: %s', disappeared_source_ids)
    for source_id in disappeared_source_ids:
        p = session.query(ProductSnapshot) \
                   .filter(ProductSnapshot.source_id == source_id) \
                   .filter(ProductSnapshot.available == True) \
                   .first()
        if p:
            p.available = False
            p.unavailable_at = datetime.datetime.utcnow()
            logger.debug('sex provider %s with id=%s and source_id=%s disappeared', p.name, p.id, p.source_id)
    session.query(ProductSnapshot).filter(~ProductSnapshot.source_id.in_(source_ids)).update({'available': False}, synchronize_session='fetch')
    session.commit()
    for provider in providers:
        decimal_price = Decimal(provider['price'].translate(str.maketrans(',', '.', '.')))
        p = session.query(ProductSnapshot) \
                   .filter(ProductSnapshot.source_id == provider['source_id']) \
                   .filter(ProductSnapshot.available == True) \
                   .first()
        if p and p.price != decimal_price:
            logger.info('sex provider %s with id=%s and source_id=%s changed price from %s to %s', p.name, p.id, p.source_id, p.price, decimal_price)
            p.available = False
            p.unavailable_at = datetime.datetime.utcnow()
        else:
            if p:
                logger.debug('sex provider %s with id=%s and source_id=%s price hasn\'t changed', p.name, p.id, p.source_id)
            else:
                logger.debug('new sex provider %s with source_id=%s', provider['name'], provider['source_id'])
    session.commit()
        
    for provider in providers:
        decimal_price = Decimal(provider['price'].translate(str.maketrans(',', '.', '.')))
        if not session.query(SexProviderSnapshot) \
                      .filter(SexProviderSnapshot.source_id==provider['source_id']) \
                      .filter(SexProviderSnapshot.price==decimal_price) \
                      .filter(SexProviderSnapshot.available==True).all():

            logger.debug('requesting %s', provider['url_pic'])
            r = requests.get(provider['url_pic'])
            logger.debug('got %s', provider['url_pic'])

            pic_index = 0
            pic_mime = mimetypes.guess_type(provider['url_pic'])[0]
            if pic_mime == 'image/jpeg':
                pic_ext = mimetypes.guess_all_extensions(pic_mime)[2]
            else:
                pic_ext = mimetypes.guess_extension(pic_mime)
            pic_filename = os.path.join(settings.MEDIA_DIR, 'sexycaracas', '{}_{}{}'.format(provider['source_id'], pic_index, pic_ext))

            if os.path.exists(pic_filename):
                r_file = io.BytesIO(r.content)
                with open(pic_filename, 'rb') as f:
                    files_are_equal = compare_files(f, r_file)
                logger.debug('sex provider picture exists, are they equal? %s', files_are_equal)

                if not files_are_equal:
                    while os.path.exists(pic_filename):
                        pic_index += 1
                        pic_filename = os.path.join(settings.MEDIA_DIR, 'sexycaracas', '{}_{}{}'.format(provider['source_id'], pic_index, pic_ext))
                    with open(pic_filename, 'wb') as f:
                        logger.debug('storing sex provider picture in %s', pic_filename)
                        f.write(r.content)
            else:
                with open(pic_filename, 'wb') as f:
                    logger.debug('storing sex provider picture in %s', pic_filename)
                    f.write(r.content)

            pic_url = os.path.join(settings.MEDIA_URL, 'sexycaracas', '{}_{}{}'.format(provider['source_id'], pic_index, pic_ext))

            sps = SexProviderSnapshot(
                name=provider['name'],
                source_id=provider['source_id'],
                gender=provider['type'],
                source_url=provider['url'],
                price=decimal_price,
                phone_number=provider['phone_number'],
                pic_filename=pic_filename,
                pic_url=pic_url,
                height=provider['height'].translate(str.maketrans(',', '.', '.')),
                weight=provider['weight'],
                age=provider['age'],
                available=True)
            session.add(sps)
            logger.debug('added sex provider %s', sps)
    session.commit()

if __name__ == '__main__':
    create_categories()
    providers = fetch()
    store_providers(providers)
