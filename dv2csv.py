#!/usr/bin/env python
# Get metadata from CBS data designs from the dataverse API and write results to CSV.
# To prevent overloading the dataverse instance during development, API results are 
# cached to disk for 3 days. This script can simply be called by:
#   ./dv2csv.py 
# or, alternatively
#   ./dv2csv.py --loglevel ERROR 
# or any other of the standard python log levels (defaults to INFO).
# In the current version for each dataset we collect for each dataset:
#   alternativeTitle, publicationDate, DOI, validFrom date, 
#   validTill date, relatedSkosConcepts

from argparse import ArgumentParser
import json
import logging
import requests
import sys

from cache_to_disk import cache_to_disk, delete_disk_caches_for_function

PORTAL='https://portal.odissei.nl'
outputfile='cbs.csv'

logging.basicConfig(encoding='utf-8', level=logging.INFO)
logger = logging.getLogger(__name__)

@cache_to_disk(3)
def get_datasets():
  """Get an overview of all dataset ids by calling the /contents API.
  Returns a Python dict version of the JSON returned by Dataverse."""
  uri = f'{PORTAL}/api/v1/dataverses/cbs/contents'
  logger.debug(f'Requesting datasets overview from {uri}')
  dv_list = requests.get(uri)
  return json.loads(dv_list.content)

@cache_to_disk(3)
def get_dataset(doi):
  """Get an overview of all metadata for the dataset identified by doi.
    Returns a Python dict version of the JSON returned by Dataverse.
  """
  uri = f'{PORTAL}/api/v1/datasets/export?exporter=dataverse_json&persistentId={doi}'
  logger.debug(f'Requesting dataset metadata from {uri}')
  ds_record = requests.get(uri)
  return json.loads(ds_record.content)

def get_skos_concepts(doi, metadata):
  concepts = ''
  try:
    for complexTerm in metadata['datasetVersion']['metadataBlocks']['enrichments']['fields'][0]['value']:
      conceptx = complexTerm['vocabVarUri1']
      concept = conceptx['value']
      concepts += f'{concept} '
  except TypeError:
    logger.warning(f'No SKOS vocabulary enrichments for {doi}')
  except KeyError:
    logger.warning(f'No enrichments for {doi}')
  return concepts


def get_alt_title(doi, metadata):
  altTitle=''
  try: 
    for field in metadata['datasetVersion']['metadataBlocks']['citation']['fields']:
      if field['typeName'] == 'alternativeTitle':
        altTitle = field['value'][0]
        altTitle = altTitle.translate({91:95, 93:95})
  except KeyError:
    logger.error(f"Oops no alt title found in {metadata}")
  return altTitle

def dataverse2csv():
  """Loop over all datasets and write selected metadata to CSV."""
  with open(outputfile, 'w', encoding="utf-8") as f:
    dv_list = get_datasets()
    f.write('alternativeTitle, publicationDate, DOI, validFrom, validTill, relatedSkosConcepts\n')
    for r in dv_list['data']:
      doi = r['persistentUrl']
      publicationDate = r['publicationDate']
      metadata = get_dataset(doi) 
      concepts = get_skos_concepts(doi, metadata)
      altTitle = get_alt_title(doi, metadata)
      altTitle=''
      try: 
        for field in metadata['datasetVersion']['metadataBlocks']['citation']['fields']:
          if field['typeName'] == 'alternativeTitle':
            altTitle = field['value'][0]
            altTitle = altTitle.translate({91:95, 93:95})
      except KeyError:
        logger.error(f"Oops {metadata}")

      valid={'from':'', 'till':''}
      try:
        for field in metadata['datasetVersion']['metadataBlocks']['CBSMetadata']['fields']:
          if field['typeName'] == 'GeldigVanaf':
            valid['from'] = field['value']
          if field['typeName'] == 'GeldigTot':
            valid['till'] = field['value']
      except KeyError:
          logger.warning('No CBS metadata for {doi}')

      resultString = f'"{altTitle}", "{publicationDate}", "{doi}", "{valid['from']}", "{valid['till']}", "{concepts}"\n'
      f.write(resultString)
      logger.debug(resultString)
    print(f"Results written to {outputfile}")

def main(args):
  # delete_disk_caches_for_function('get_datasets')
  # delete_disk_caches_for_function('get_dataset')
  parser = ArgumentParser()
  parser.add_argument("-log", "--loglevel", help="Python standard loglevel (defaults to info)", default='info')
  args = parser.parse_args()
  logger.setLevel(args.loglevel.upper())
  
  dataverse2csv()

if __name__ == '__main__':
  main(sys.argv[1:])