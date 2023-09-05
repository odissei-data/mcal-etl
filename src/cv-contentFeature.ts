import { Etl, Source, declarePrefix, Destination, environments, fromXlsx, toTriplyDb, uploadPrefixes, when } from '@triplyetl/etl/generic'
import { addIri, iri, pairs, triple } from '@triplyetl/etl/ratt'
import { logRecord } from '@triplyetl/etl/debug'
import { a, skos } from '@triplyetl/etl/vocab'

// Declare prefixes.
const prefix = {
  cf: declarePrefix('https://mcal.odissei.nl/cv/contentFeature/v0.1/'),
}

// Declare input sources:
// Concept Scheme triples to be copied from github static file:
const conceptSchemeDefinition = Source.url('https://raw.githubusercontent.com/odissei-data/vocabularies/main/mcal/ContentFeatureScheme.ttl')

// Shared Google doc with MCAL scheme definitions, we need the download link for the contentFeature tab that has gid=1646162430
const MCALschema = 'https://docs.google.com/spreadsheets/d/1zB1SFPpz5VDjlrh5LlJqmFXSggqXPODDPGHzLyL-Ef8'
const contentFeatureTab = Source.url(MCALschema + '/export?gid=1646162430')

// Declare destination environment at TriplyDB:
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
    fromXlsx(contentFeatureTab),
    logRecord(),
    when('skos:notation',
      addIri({ // Generate IRI for current contentFeature skos:Concept :
        prefix: prefix.cf,
        content: 'skos:notation',
        key: '_ID'
      }),
      // generate mandatory triples:
      pairs('_ID', 
      [ a, skos.Concept ],
      [ skos.prefLabel, 'skos:prefLabel'],
      [ skos.notation, 'skos:notation'],
      [ skos.inScheme, iri(prefix.cf, '')]
      ),
      // generate optional triples:
      when('Description',
        triple('_ID', skos.scopeNote, 'Description')
      ),
      when('Example',
        triple('ID', skos.example, 'Example')
      ),
      when('skos:broader',
        triple('_ID', skos.broader, iri(prefix.cf, 'skos:broader')),
        triple(iri(prefix.cf, 'skos:broader'), skos.narrower, '_ID'),
      ),
      when('isTopConcept',
        triple(iri(prefix.cf, ''), skos.hasTopConcept, '_ID')
      )
    ),
    // publish at TriplyDB instance destination
    toTriplyDb(destination),
    uploadPrefixes(destination),
  )
  // add static skos:ConceptScheme triples  to destination
  await etl.copySource(conceptSchemeDefinition, Destination.TriplyDb.rdf(destination.account, destination.dataset));
  return etl
}
