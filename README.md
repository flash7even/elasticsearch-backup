Elasticsearch Version: 7.6.2 and above

Python Dependency:

pip install -r requirements.txt


#### download-mapping

1. python3 es_mapping_backup.py --schema ALL (Download all the mappings)
2. python3 es_mapping_backup.py (Download mapping for only the config file indices)


#### download-elasticsearch-data

1. python3 es_backup.py --schema ALL (Download all the indices data)
2. python3 es_backup.py (Download data for only the config file indices)


#### restore-mapping

1. python3 es_mapping_uploader.py --dir mapping-data/ --create true (Upload all the mappings from corresponding directory)

Other possible arguments: --update true, --delete true


#### upload-elasticsearch-data

1. python3 es_restore.py --schema ALL (Upload all the indices data)
2. python3 es_restore.py (Upload data for only the config file indices)
