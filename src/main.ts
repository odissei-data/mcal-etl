import { Etl, Source, declarePrefix, environments, fromCsv, fromXlsx, toTriplyDb, when } from '@triplyetl/etl/generic'
import { addIri, iri, iris, pairs, split, triple } from '@triplyetl/etl/ratt'
import { logRecord } from '@triplyetl/etl/debug'
import { bibo, dct, a } from '@triplyetl/etl/vocab'
import { validate } from '@triplyetl/etl/shacl'

// Declare prefixes.
const prefix_base = declarePrefix('https://mcal.odissei.nl/')
const prefix = {
  orcid: declarePrefix('https://orcid.org/'),
  journal: declarePrefix(prefix_base('id/j/')),
  article: declarePrefix(prefix_base('id/a/')),
  data: declarePrefix(prefix_base('data/')),
  graph: declarePrefix(prefix_base('graph/')),
  schema: declarePrefix(prefix_base('schema/'))
}

const schema = {
  /**
   * Category: property
   *
   * Label: has material
   *
   * Description: Type of material
   */
  material: prefix.schema('material'),
  relevantForMCAL: prefix.schema('relevantForMcal')
}

const graph = {
  instances: prefix.graph('instances'),
  metadata: prefix.graph('metadata'),
  schema: prefix.graph('schema')
}

const destination = {
  account: Etl.environment === environments.Development ? undefined : 'odissei',
  dataset:
    Etl.environment === environments.Acceptance
      ? 'mcal-acceptance'
      : Etl.environment === environments.Testing
        ? 'mcal-testing'
        : 'mcal'
}

export default async function (): Promise<Etl> {
  const etl = new Etl({ defaultGraph: graph.instances })
  etl.use(
    fromCsv(Source.TriplyDb.asset('odissei', 'mcal', {name: 'Mcalentory.csv'})),
    addIri({ // Generate IRI for article, maybe use DOI if available?
      prefix: prefix.article,
      content: 'articleID',
      key: '_article'
    }),
    addIri({ // Generate IRI for journal, is there a persistant ID for journals?
      prefix: prefix.journal,
      content: 'journalID',
      key: '_journal'
    }),
    when(
      context => context.getString('orcid') != 'NA',
      split({
        content: 'orcid',
        separator: ',',
        key: '_orcids'
      }),
      triple('_article', dct.creator, iris(prefix.orcid, '_orcids'))
    ),
    pairs('_article',
      [a, bibo.AcademicArticle],
      [dct.title, 'title'],
      [dct.isPartOf, '_journal'],
      [dct.date, 'publicationDate'],
      [dct.relation, 'doi'],
      [dct.hasVersion, 'correspondingArticle'],
      [schema.relevantForMCAL, 'relevant']
    ),
    pairs('_journal',
      [a, bibo.Journal],
      [dct.title, 'journal']
    ),
    logRecord(),
    validate(Source.file('static/model.trig')),
    toTriplyDb(destination)
  )
  return etl
}
