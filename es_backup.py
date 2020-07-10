import argparse
import os
import json
import time
import re
import requests

import logging
from logging.handlers import TimedRotatingFileHandler
from config import Config

logger = logging.getLogger('es backup logger')
logger.setLevel(logging.DEBUG)
handler = TimedRotatingFileHandler('./logs/es_backup.log', when='midnight', interval=1, backupCount=30)
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


def store_json_data(json_data, file_name):
    with open(file_name, 'w') as outfile:
        json.dump(json_data, outfile)


def download_index_data(index_name):
    logger.info(f'Backup data for index: {index_name}')
    try:
        page = 0
        item_list = []

        while 1:
            query_json = {'query': {'match_all': {}}}
            query_json['from'] = page*_es_size
            query_json['size'] = _es_size
            search_url = 'http://{}/{}/{}/_search'.format(Config.ES_HOST, index_name, _es_type)
            response = rs.post(url=search_url, json=query_json, headers=_http_headers).json()
            if 'hits' in response:
                if len(response['hits']['hits']) == 0:
                    break
                for hit in response['hits']['hits']:
                    item_list.append(hit)
            else:
                logger.error('Elasticsearch down, response : ' + str(response))
                break
            page += 1

        json_data = {
            'index_data': item_list,
            'index_name': index_name,
            'backup_time': int(time.time()),
        }
        file_location = f'backup-data/{index_name}.json'
        store_json_data(json_data, file_location)
    except Exception as e:
        logger.debug(f'Exception occurred: {e}')


def back_up_es_data(index_list):
    for index in index_list:
        download_index_data(index)


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
    logger.info('START RUNNING ES BACKUP SCRIPT')
    t1 = time.time()

    parser = argparse.ArgumentParser(description='es backup parser')
    parser.add_argument('--schema', help="Provide list of schema")
    args = parser.parse_args()

    indices_list = Config.index_list

    if args.schema and args.schema == 'ALL':
        indices_list = get_index_list()

    back_up_es_data(indices_list)

    logger.info('END ES BACKUP SCRIPT')
    t2 = time.time()
    logger.info('TIME TAKEN: ' + str(t2-t1))
