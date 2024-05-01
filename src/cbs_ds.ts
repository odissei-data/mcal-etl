import {Etl, Source, declarePrefix, environments, fromCsv, toTriplyDb, uploadPrefixes, when } from '@triplyetl/etl/generic'
import {addHashedIri, literal, iri, iris, objects, pairs, split, str, triple, nestedPairs } from '@triplyetl/etl/ratt'
// import { addIri, custom, iri, iris, lowercase, pairs, split, triple } from '@triplyetl/etl/ratt'
// import { logRecord } from '@triplyetl/etl/debug'
import { bibo, dcat, dct, a, xsd } from '@triplyetl/etl/vocab'

// import { validate } from '@triplyetl/etl/shacl'

// Declare prefixes.
const prefix_base = declarePrefix('https://kg.odissei.nl/')

const prefix = {
  graph: declarePrefix(prefix_base('graph/')),
  dsv: declarePrefix('https://w3id.org/dsv-ontology#'),
  odissei_kg_schema: declarePrefix(prefix_base('schema/')),
  codelib: declarePrefix(prefix_base('cbs_codelib/')),
  cbs_ds: declarePrefix(prefix_base('cbs/dataset/')),
  doi: declarePrefix('https://doi.org/'),
  orcid: declarePrefix('https://orcid.org/'),
  issn: declarePrefix('https://portal.issn.org/resource/ISSN/'),
  foaf: declarePrefix('http://xmlns.com/foaf/0.1/')
}

const destination = {
  defaultGraph: prefix.graph('datasets'),
  account: process.env.USER ?? "odissei",
  prefixes: prefix, 
  dataset:
    Etl.environment === environments.Acceptance
      ? 'odissei-kg-staging'
      : Etl.environment === environments.Testing
        ? 'odissei-kg-staging'
        : 'odissei-kg'
}

export default async function (): Promise<Etl> {
  const etl = new Etl(destination)
  etl.use(
    fromCsv(Source.file('cbs.csv')),
    //logRecord(),
    addHashedIri({
      prefix: prefix.odissei_kg_schema,
      content: ['DOI', str('schema')],
      key: '_DOIschema'
    }),
    addHashedIri({
      prefix: prefix.odissei_kg_schema,
      content: ['DOI', str('temporal')],
      key: '_DOItemporal'
    }),
    triple('DOI', a, iri(prefix.dsv, str('Dataset'))),
    triple('_DOIschema', a, iri(prefix.dsv, str('DatasetSchema'))),
    triple('_DOItemporal', a, dct.PeriodOfTime),
    triple('DOI', iri(prefix.dsv, str('datasetSchema')), iri('_DOIschema')),

    pairs('DOI', 
      [bibo.shortTitle, iri(prefix.cbs_ds,"alternativeTitle")],
      [dct.date, literal('publicationDate', xsd.date)]
    ),

    when('validTill', 
      nestedPairs(iri('DOI'), dct.temporal, iri('_DOItemporal'), 
        [dcat.startDate, 'validFrom'], 
        [dcat.endDate,   'validTill'])
    ),
  
    when('relatedSkosConcepts',
      split({
        content: 'relatedSkosConcepts',
        separator: ' ',
        key: '_relatedSkosConcepts'
      }),
      objects('DOI', dct.subject, iris('_relatedSkosConcepts'))
    ),

    // FIXME, this needs to have a dsv:Column object in between the schema and the variable
    // See https://doi.org/10.1145/3587259.3627559
    when('variables',
      split({
        content: 'variables',
        separator: ' ',
        key: '_variables'
      }),
      objects('_DOIschema', iri(prefix.dsv, str('hasVariable')), iris('_variables'))
    ),
    toTriplyDb(destination),
    uploadPrefixes(destination),
  )
  return etl
}