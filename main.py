from urllib3.util import parse_url
from urllib.parse import unquote

import os
import gzip
import io

import pandas as pd
import tldextract
from bs4 import BeautifulSoup
import requests


DATASET_PATH = './Dataset'


def download_file(url: str, destination: str):
    print(f'downloading from {url}')
    response = requests.get(url)
    if response.status_code == 200:

        with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
            extracted_content = f.read()

        with open(destination, 'w', encoding='utf-8') as f:
            f.write(extracted_content)

    else:
        print(f"failed to download file. status code: {response.status_code}")

def update_dataset_from_urlabuse():
    ''' download daily dumps '''
    print('get a list of files')
    base_url = 'https://urlabuse.com/public/data/dumps'
    response = requests.get(base_url)
    if response.status_code != 200:
        raise Exception
    soup = BeautifulSoup(response.text, 'html.parser')
    links = set([link.get('href')[:-3] for link in soup.find_all('a') if link.get('href').startswith('dumps')])
    existing_files = set(os.listdir(DATASET_PATH))
    new_files = links.difference(existing_files)

    if len(new_files) > 0:
        print(f'{len(new_files)} update available')
    else:
        print('no update available')
        return None

    for file in new_files:
        file_name = file + '.gz'
        download_file(f'{base_url}/{file_name}', f'{DATASET_PATH}/{file}')

def process_record(record: str) -> list | None:
    '''compatible with urlabuse.com'''
    try:
        record = record.replace("\\N", '"None"')

        url, dt1, dt2, target, *_ = record.split('","')
        # for example #TARGET
        if dt1.startswith('#') or dt2.startswith('#') or target.startswith('#'):
            return None
        
        url = url.split(',"', 1)[1]
        url = unquote(url)

        urllib3_url_parse = parse_url(url)
        path = urllib3_url_parse.path
        scheme = urllib3_url_parse.scheme
        port = urllib3_url_parse.port
        query_param = urllib3_url_parse.query
        if not port:
            if scheme == 'https':
                port = 443
            elif scheme == 'http':
                port = 80
        if path:
            path = unquote(path)

        tldextract_url_parse = tldextract.extract(url)
        domain = tldextract_url_parse.domain
        fqdn = tldextract_url_parse.fqdn
        subdomain = tldextract_url_parse.subdomain
        suffix = tldextract_url_parse.suffix
        
        
        return [
            scheme,
            subdomain,
            domain,
            port,
            path,
            query_param,
            suffix,
            fqdn,
            target,
            url
        ]

    except Exception as ex:
        #TODO save this record for check it later
        print(ex)

def load_urlabuse_dataset() -> pd.DataFrame:
    if os.path.exists('dataset.csv'):
        print('read from csv')
        return pd.read_csv('dataset.csv')
    else:
        print('read for dir')
    
    files_name = os.listdir(DATASET_PATH)
    data = []
    
    for file_name in files_name:
        with open(f'{DATASET_PATH}/{file_name}') as file:
            for record in file.readlines():
                record = process_record(record)
                if not record:
                    continue
                data.append(record)
    
    df = pd.DataFrame(data)
    df.columns = [
        'SCHEME',
        'SUBDOMAIN',
        'DOMAIN',
        'PORT',
        'PATH',
        'QUERY_PARAM',
        'SUFFIX',
        'FQDN',
        'TARGET',
        'FULL_URL',
    ]
    df.to_csv('dataset.csv')
    print('save dataset as csv file')
    return df


if __name__ == '__main__':
    update_dataset_from_urlabuse()
    # load_urlabuse_dataset()