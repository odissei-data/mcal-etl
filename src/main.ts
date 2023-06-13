import { Etl, Source, declarePrefix, environments, fromXlsx, toTriplyDb } from '@triplyetl/etl/generic'
import { addIri, iri, pairs } from '@triplyetl/etl/ratt'
import { logRecord } from '@triplyetl/etl/debug'
import { bibo, dct, a } from '@triplyetl/etl/vocab'
import { validate } from '@triplyetl/etl/shacl'

// Declare prefixes.
const prefix_base = declarePrefix('https://mcal.odissei.nl/')
const prefix = {
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
  material: prefix.schema('material')
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
    fromXlsx(
      Source.TriplyDb.asset('odissei', 'mcal',
        { name: '20230613_MCAL_Inventory_ContentAnalysis.xlsx' })
    ),
    addIri({
      prefix: prefix.data,
      content: 'journalID',
      key: '_journal'
    }),
    pairs(iri(prefix.data, 'articleID'),
      [a, bibo.AcademicArticle],
      [dct.title, 'title'],
      [dct.isPartOf, '_journal']
    ),
    pairs('_journal',
      [a, bibo.Journal],
      [dct.title, 'journal']
    ),
    validate(Source.file('static/model.trig')),
    toTriplyDb(destination)
  )
  return etl
}
