import argparse
import os
import json
import time
import re
from hashlib import md5
import requests

import logging
from logging.handlers import TimedRotatingFileHandler
from config import Config

logger = logging.getLogger('es restore logger')
logger.setLevel(logging.DEBUG)
handler = TimedRotatingFileHandler('./logs/es_restore.log', when='midnight', interval=1, backupCount=30)
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


def read_json_data(file_name):
    with open(file_name) as json_file:
        data = json.load(json_file)
        return data


def restore_index_data(index_name):
    logger.info(f'Backup data for index: {index_name}')
    try:
        file_location = f'backup-data/{index_name}.json'
        restored_data = read_json_data(file_location)

        for item in restored_data['index_data']:
            id = item['_id']
            data = item['_source']
            url = 'http://{}/{}/{}/{}'.format(Config.ES_HOST, index_name, _es_type, id)
            response = rs.post(url=url, json=data, headers=_http_headers).json()
            if 'result' not in response:
                logger.error(f'Could not insert elasticsearch data for id: {id}')

    except Exception as e:
        logger.debug(f'Exception occurred: {e}')


def restore_es_data(indices_list):
    for index in indices_list:
        restore_index_data(index)


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
    logger.info('START RUNNING ES RESTORE SCRIPT')
    t1 = time.time()

    parser = argparse.ArgumentParser(description='es restore parser')
    parser.add_argument('--schema', help="Provide list of schema")
    args = parser.parse_args()

    indices_list = Config.index_list

    if args.schema and args.schema == 'ALL':
        indices_list = get_index_list()

    restore_es_data(indices_list)
    logger.info('END ES RESTORE SCRIPT')
    t2 = time.time()
    logger.info('TIME TAKEN: ' + str(t2-t1))
