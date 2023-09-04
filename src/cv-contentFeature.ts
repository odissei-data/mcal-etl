import { Etl, Source, declarePrefix, Destination, environments, fromXlsx, toTriplyDb, uploadPrefixes, when } from '@triplyetl/etl/generic'
import { addIri, iri, pairs, triple } from '@triplyetl/etl/ratt'
import { logRecord } from '@triplyetl/etl/debug'
import { a, skos } from '@triplyetl/etl/vocab'

// Declare prefixes.
const prefix_base = declarePrefix('https://mcal.odissei.nl/')
const prefix_cv_base = declarePrefix(prefix_base('cv/'))

const prefix = {
  mcal: declarePrefix(prefix_base('schema/')),
  cf: declarePrefix(prefix_cv_base('contentFeature/v0.1/')),
}

const conceptSchemeDefinition = Source.url('https://raw.githubusercontent.com/odissei-data/vocabularies/main/mcal/ContentFeatureScheme.ttl')

const destination = {
  defaultGraph: prefix.cf, 
  opts: {overwrite: true, synchronizeServices: false },
  account: process.env.USER ?? "odissei",
  dataset:
    Etl.environment === environments.Acceptance
      ? 'mcal-acceptance'
      : Etl.environment === environments.Testing
        ? 'mcal-testing'
        : 'mcal'
}

export default async function (): Promise<Etl> {
  const etl = new Etl(destination)
    
  etl.use(
    // fromXlsx(Source.file(['../mcal-cleaning/Data/MCALSchema.xlsx']), { sheetNames: ['contentFeature']}),
    fromXlsx(Source.url('https://docs.google.com/spreadsheets/d/1zB1SFPpz5VDjlrh5LlJqmFXSggqXPODDPGHzLyL-Ef8/export?gid=1646162430')),

    logRecord(),
    when('skos:notation',
      addIri({ // Generate IRI for contentFeature skos:Concept:
        prefix: prefix.cf,
        content: 'skos:notation',
        key: '_ID'
      }),
      pairs('_ID', 
      [ a, skos.Concept ],
      [ skos.prefLabel, 'skos:prefLabel'],
      [ skos.notation, 'skos:notation'],
      [ skos.inScheme, iri(prefix.cf, '')]
      ),
      when('Description',
        triple('_ID', skos.scopeNote, 'Description')
      ),
      when('Example',
        triple('ID', skos.example, 'Example')
      ),
      when('skos:broader',
        triple('_ID', skos.broader, iri(prefix.cf, 'skos:broader'))
      ),
    ),
    toTriplyDb(destination),
    uploadPrefixes(destination)
  )
  await etl.copySource(conceptSchemeDefinition, Destination.TriplyDb.rdf(destination.account, destination.dataset));
  return etl
}
