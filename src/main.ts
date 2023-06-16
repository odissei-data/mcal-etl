import { environments, fromJson, Etl, toTriplyDb, declarePrefix, Source } from '@triplyetl/etl/generic'
import { concat, iri, literal, pairs, str } from '@triplyetl/etl/ratt'
import { validate } from '@triplyetl/etl/shacl'
import { a, foaf, xsd } from '@triplyetl/etl/vocab'

// Declare prefixes.
const prefix_base = declarePrefix('https://example.org/')
const prefix_id = declarePrefix(prefix_base('id/'))
const prefix = {
  id: prefix_id,
  graph: declarePrefix(prefix_id('graph/')),
}

// Declare graph names.
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
  // Create an extract-transform-load (ETL) process.
  const etl = new Etl({ defaultGraph: graph.instances })
  etl.use(

    // Connect to one or more data sources.
    fromJson([{ firstName: 'J.', age: 32, something: 'c' }]),

    // Transformations change data in the Record.
    concat({
      content: ['firstName', str('Doe')],
      separator: ' ',
      key: '_fullName',
    }),

    // Assertions add linked data to the RDF store.
    pairs(iri(prefix.id, 'firstName'),
      [a, foaf.Person],
      [foaf.age, literal('age', xsd.nonNegativeInteger)],
      [foaf.name, '_fullName'],
    ),

    // Validation ensures that your instance data follows the data model.
    validate(Source.file('static/model.trig')),

    // Publish your data in TriplyDB.
    toTriplyDb(destination),

  )
  return etl
}
