// Import middlewares.
import { a, concat, environments, fromJson, iri, literal, pairs, Ratt as Etl, str, toTriplyDb, validateShacl } from '@triplydb/ratt'
// Import vocabularies.
import { foaf, xsd } from '@triplydb/ratt/lib/vocab'

// Declare prefixes.
const prefix_base = Etl.prefixer('https://example.org/')
const prefix_id = Etl.prefixer(prefix_base('id/'))
const prefix = {
  id: prefix_id,
  graph: Etl.prefixer(prefix_id('graph/')),
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
    validateShacl(Etl.Source.file('static/model.trig')),

    // Publish your data in TriplyDB.
    toTriplyDb(destination),

  )
  return etl
}
