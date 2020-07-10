import argparse
import os
import json
import time
import re
import requests

import logging
from logging.handlers import TimedRotatingFileHandler
from config import Config

logger = logging.getLogger('es mapping backup logger')
logger.setLevel(logging.DEBUG)
handler = TimedRotatingFileHandler('./logs/es_mapping_backup.log', when='midnight', interval=1, backupCount=30)
handler.setLevel(logging.DEBUG)
handler.setFormatter(logging.Formatter(
    fmt='[%(asctime)s.%(msecs)03d] [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))
handler.suffix = "%Y%m%d"
handler.extMatch = re.compile(r"^\d{8}$")
logger.addHandler(handler)

rs = requests.session()
_http_headers = {'Content-Type': 'application/json'}
_es_size = 100
_es_type = '_doc'


def store_mapping(mapping, file_name):
    with open(file_name, 'w') as outfile:
        json.dump(mapping, outfile)


def download_mapping(indices_list):
    for index_name in indices_list:
        file_location = f'mapping-data/{index_name}.json'
        url = 'http://{}/{}/_mapping'.format(Config.ES_HOST, index_name)
        response = rs.get(url=url, headers=_http_headers).json()
        logger.debug('mapping response: ' + str(response))
        mapping = response[index_name]['mappings']
        store_mapping(mapping, file_location)


def get_index_list():
    url = 'http://{}/_cat/indices?format=json&pretty=true'.format(Config.ES_HOST)
    response = rs.get(url=url, headers=_http_headers).json()
    logger.debug('index list response: ' + str(response))
    indices_list = []
    for index in response:
        if index['index'].startswith('.'):
            continue
        indices_list.append(index['index'])
    return indices_list


if __name__ == '__main__':
    logger.info('START RUNNING ES MAPPING BACKUP SCRIPT')
    t1 = time.time()

    parser = argparse.ArgumentParser(description='es mapping backup parser')
    parser.add_argument('--schema', help="Provide list of schema")
    args = parser.parse_args()

    indices_list = Config.index_list

    if args.schema and args.schema == 'ALL':
        indices_list = get_index_list()

    download_mapping(indices_list)

    logger.info('END ES MAPPING BACKUP SCRIPT')
    t2 = time.time()
    logger.info('TIME TAKEN: ' + str(t2-t1))
