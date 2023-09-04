import { Etl, Source, declarePrefix, environments, fromXlsx, toTriplyDb, when } from '@triplyetl/etl/generic'
import { addIri, iri, pairs, triple } from '@triplyetl/etl/ratt'
import { logRecord } from '@triplyetl/etl/debug'
import { a, skos } from '@triplyetl/etl/vocab'

// Declare prefixes.
const prefix_base = declarePrefix('https://mcal.odissei.nl/')
const prefix_cv_base = declarePrefix(prefix_base('cv/'))

const prefix = {
  orcid: declarePrefix('https://orcid.org/'),
  issn: declarePrefix('https://portal.issn.org/resource/ISSN/'),
  journal: declarePrefix(prefix_base('id/j/')),
  article: declarePrefix(prefix_base('id/a/')),
  data: declarePrefix(prefix_base('data/')),
  graph: declarePrefix(prefix_base('graph/')),
  mcal: declarePrefix(prefix_base('schema/')),
  cat: declarePrefix(prefix_cv_base('contentAnalysisType/v0.1/')),
  cf: declarePrefix(prefix_cv_base('contentFeature/v0.1/')),
  rqt: declarePrefix(prefix_cv_base('researchQuestionType/v0.1/')),
}

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
    fromXlsx(Source.file(['../mcal-cleaning/Data/MCALSchema.xlsx']), { sheetNames: ['contentFeature']}),
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
    toTriplyDb(destination)
  )
  return etl
}
