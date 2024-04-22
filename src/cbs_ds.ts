import {Etl, Source, declarePrefix, environments, fromCsv, toTriplyDb, uploadPrefixes, when } from '@triplyetl/etl/generic'
import {iri, iris, objects, pairs, split } from '@triplyetl/etl/ratt'
// import { addIri, custom, iri, iris, lowercase, pairs, split, triple } from '@triplyetl/etl/ratt'
import { logRecord } from '@triplyetl/etl/debug'
import { bibo, dct } from '@triplyetl/etl/vocab'

// import { validate } from '@triplyetl/etl/shacl'

// Declare prefixes.
const prefix_base = declarePrefix('https://kg.odissei.nl/')

const prefix = {
  graph: declarePrefix(prefix_base('graph/')),
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
    //fromCsv(Source.TriplyDb.asset(destination.account, destination.dataset, {name: 'cbs.csv'})),
    fromCsv(Source.file('cbs.csv')),
    logRecord(),
    pairs('DOI', 
      [bibo.shortTitle, iri(prefix.cbs_ds,"alternativeTitle")],
      [dct.date, "publicationDate"]
    ),
    when('relatedSkosConcepts',
      split({
        content: 'relatedSkosConcepts',
        separator: ' ',
        key: '_relatedSkosConcepts'
      }),
      objects('DOI', dct.subject, iris('_relatedSkosConcepts'))
    ),
    toTriplyDb(destination),
    uploadPrefixes(destination),
  )
  return etl
}