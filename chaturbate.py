import os
import requests
from bs4 import BeautifulSoup
import random
import subprocess
import settings
import json
os.chdir(settings.BASE_DIR)

def find_streams_urls(cams_soup):
    good_streams, bad_streams = [], []
    for li in cams_soup.find('ul', class_='list').find_all('li'):
        if 'venezuela' in li.get_text().lower():
            try:
                good_stream_url = 'https://chaturbate.com{}'.format(li.find('div', class_='title').find('a')['href'])
                good_streams.append(good_stream_url)
            except:
                pass
        else:
            try:
                bad_stream_url = 'https://chaturbate.com{}'.format(li.find('div', class_='title').find('a')['href'])
                bad_streams.append(bad_stream_url)
            except:
                pass
    return good_streams, bad_streams

def get_streams():
    cams_page = requests.get('https://chaturbate.com/female-cams/')
    cams_soup = BeautifulSoup(cams_page.content, 'html.parser')
    last_page = int(cams_soup.find(class_='paging').find_all('li')[-2].text)

    streams_urls = find_streams_urls(cams_soup)
    good_streams = streams_urls[0]
    bad_streams = streams_urls[1]

    for page in range(2, last_page+1):
        print('page: {}'.format(page))
        cams_page = requests.get('https://chaturbate.com/female-cams/?page={}'.format(page))
        cams_soup = BeautifulSoup(cams_page.content, 'html.parser')
        streams_urls = find_streams_urls(cams_soup)
        good_streams += streams_urls[0]
        bad_streams += streams_urls[1]

    good_chaturbates = []
    for stream in good_streams:
        try:
            stream_url = json.loads(subprocess.check_output([settings.YOUTUBE_DL_PATH, '-j', stream]).decode('ascii'))['formats'][0]['manifest_url']
        except Exception as e:
            print(e)
        else:
            good_chaturbates.append({'source': stream_url.strip(), 'profile': stream.strip()})

    bad_streams = random.sample(bad_streams, 3)
    bad_chaturbates = []
    for stream in bad_streams:
        try:
            stream_url = json.loads(subprocess.check_output([settings.YOUTUBE_DL_PATH, '-j', stream]).decode('ascii'))['formats'][0]['manifest_url']
        except Exception as e:
            print(e)
        else:
            bad_chaturbates.append({'source': stream_url.strip(), 'profile': stream.strip()})

    streams = {}
    streams['good'] = good_chaturbates
    streams['bad'] = bad_chaturbates
    return streams

if __name__ == '__main__':
    streams = get_streams()
    with open('chaturbate.json', 'w') as f:
        f.write(json.dumps(streams))
