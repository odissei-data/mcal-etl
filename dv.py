#!/usr/bin/env python
# Get mapping from CBS data design DOI to "Alternative Title" from 
# the dataverse API and write results to two column CSV.
# To prevent overloading the dataverse instance during development,
# API results are cached to disk for 3 days.

import requests
import json
import logging

from cache_to_disk import cache_to_disk, delete_disk_caches_for_function

PORTAL='https://portal.odissei.nl'
outputfile='cbs.csv'

logger = logging.getLogger(__name__)
logging.basicConfig(encoding='utf-8', level=logging.INFO)

@cache_to_disk(3)
def get_datasets():
  uri = f'{PORTAL}/api/v1/dataverses/cbs/contents'
  logger.debug(f'Requesting datasets overview from {uri}')
  dv_list = requests.get(uri)
  return json.loads(dv_list.content)

@cache_to_disk(3)
def get_dataset(doi):
  uri = f'{PORTAL}/api/v1/datasets/export?exporter=dataverse_json&persistentId={doi}'
  logger.debug(f'Requesting dataset metadata from {uri}')
  ds_record = requests.get(uri)
  return json.loads(ds_record.content)

if __name__ == '__main__':
  # delete_disk_caches_for_function('get_datasets')
  # delete_disk_caches_for_function('get_dataset')
  with open(outputfile, 'w', encoding="utf-8") as f:
    dv_list = get_datasets()
    f.write('DOI, alternativeTitle, publicationDate, relatedSkosConcepts\n')
    for r in dv_list['data']:
      doi = r['persistentUrl']
      publicationDate = r['publicationDate']
      metadata = get_dataset(doi) 
      concepts = ""
      try:
        for complexValue in metadata['datasetVersion']['metadataBlocks']['enrichments']['fields'][0]['value']:
          for field in complexValue.values():
            if field['typeName'] == 'vocabVarUri1':
              if concepts:
                concepts = concepts + f' {field['value']}'
              else:
                concepts = field['value']
      except AttributeError:
        logger.info(f'No SKOS vocabulary enrichments for {doi}')
      except KeyError:
        logger.info(f'No enrichments for {doi}')

      try: 
        for field in metadata['datasetVersion']['metadataBlocks']['citation']['fields']:
          if field['typeName'] == 'alternativeTitle':
            altTitle = field['value'][0]
            altTitle = altTitle.translate({91:95, 93:95})
            resultString = f'{doi}, "{altTitle}", "{publicationDate}", "{concepts}"\n'
            f.write(resultString)
            logger.debug(resultString)
      except KeyError:
        print(f"Oops {metadata}")
  print(f"Results written to {outputfile}")