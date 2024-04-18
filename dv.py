#!/usr/bin/env python
# Get mapping from CBS data design DOI to "Alternative Title" from 
# the dataverse API and write results to two column CSV.
# To prevent overloading the dataverse instance during development,
# API results are cached to disk for 3 days.

import requests
import json

from cache_to_disk import cache_to_disk, delete_disk_caches_for_function

PORTAL='https://portal.odissei.nl'
outputfile='cbs.csv'

@cache_to_disk(3)
def get_datasets():
  dv_list = requests.get(f'{PORTAL}/api/v1/dataverses/cbs/contents')
  return json.loads(dv_list.content)

@cache_to_disk(3)
def get_dataset(doi):
  url = f'{PORTAL}/api/v1/datasets/export?exporter=dataverse_json&persistentId={doi}'
  ds_record = requests.get(url)
  return json.loads(ds_record.content)

if __name__ == '__main__':
  # delete_disk_caches_for_function('get_datasets')
  # delete_disk_caches_for_function('get_dataset')
  with open(outputfile, 'w', encoding="utf-8") as f:
    dv_list = get_datasets()
    f.write('DOI, alternativeTitle\n')
    for r in dv_list['data']:
      doi = r['persistentUrl']
      metadata = get_dataset(doi) 
      try: 
        for field in metadata['datasetVersion']['metadataBlocks']['citation']['fields']:
          if field['typeName'] == 'alternativeTitle':
            altTitle = field['value'][0]
            f.write(f'{doi}, "{altTitle}"\n')
            print(f'{doi}, "{altTitle}"')
      except KeyError:
        print(f"Oops {metadata}")
  print(f"Results written to {outputfile}")