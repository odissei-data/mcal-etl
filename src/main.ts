import { a, addIri, environments, fromXlsx, iri, logRecord, pairs, Ratt as Etl, toTriplyDb } from '@triplydb/ratt'
import { bibo, dct } from '@triplydb/ratt/lib/vocab'

// Declare prefixes.
const prefix_base = Etl.prefixer('https://example.org/')
const prefix_id = Etl.prefixer(prefix_base('id/'))
const prefix = {
  id: prefix_id,
  graph: Etl.prefixer(prefix_id('graph/')),
}

const graph = {
  instances: prefix.graph('instances'),
}

const destination = {
  account:
    Etl.environment === environments.Development
      ? undefined
      : 'odissei',
  dataset:
    Etl.environment === environments.Acceptance
      ? 'mcal-acceptance'
      : Etl.environment === environments.Testing
        ? 'mcal-testing'
        : 'mcal',
}

export default async function (): Promise<Etl> {
  const etl = new Etl({ defaultGraph: graph.instances })
  etl.use(
    fromXlsx(Etl.Source.TriplyDb.asset('odissei', 'mcal', {
      name: '20220610_MCAL_Inventory_ContentAnalysis.xlsx',
    })),

    addIri({
      prefix: prefix.id,
      content: 'JournalID',
      key: '_journal',
    }),
    pairs(iri(prefix.id, 'ArticleID'),
      [a, bibo.AcademicArticle],
      [dct.title, 'Title Article'],
      [dct.isPartOf, '_journal'],
    ),
    pairs('_journal',
      [a, bibo.Journal],
      [dct.title, 'Journal'],
    ),

    logRecord(),

    //validateShacl(Etl.Source.file('static/model.trig')),
    toTriplyDb(destination),
  )
  return etl
}
