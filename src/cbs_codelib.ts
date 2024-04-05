import {Etl, Source, declarePrefix, environments, when, toTriplyDb, uploadPrefixes, fromCsv } from '@triplyetl/etl/generic'
import { addIri, iri, iris, split, str, triple } from '@triplyetl/etl/ratt'
// import { addIri, custom, iri, iris, lowercase, pairs, split, triple } from '@triplyetl/etl/ratt'
import { logRecord } from '@triplyetl/etl/debug'
import { bibo, a, dct, dcm } from '@triplyetl/etl/vocab' // dct
// import { validate } from '@triplyetl/etl/shacl'

// Declare prefixes.
const prefix_base = declarePrefix('https://kg.odissei.nl/')

const prefix = {
  graph: declarePrefix(prefix_base('graph/')),
  odissei_kg_schema: declarePrefix(prefix_base('schema/')),
  codelib: declarePrefix(prefix_base('cbs_codelib/')),
  cbs_project: declarePrefix(prefix_base('cbs/project/')),
  doi: declarePrefix('https://doi.org/'),
  orcid: declarePrefix('https://orcid.org/'),
  issn: declarePrefix('https://portal.issn.org/resource/ISSN/')
}

const cbs_codelib = 'https://raw.githubusercontent.com/odissei-data/ODISSEI-code-library/main/Data/odissei-projects_CBS.csv'

const destination = {
  defaultGraph: prefix.graph('codelib'),
  account: process.env.USER ?? "odissei",
  prefixes: prefix, 
  dataset:
    Etl.environment === environments.Acceptance
      ? 'odissei-kg-staging'
      : Etl.environment === environments.Testing
        ? 'odissei-kg-staging'
        : 'odissei-kg'
}

/*
const getRdf = async (url: string) => {
  const ctx = new Context(new Etl())
  await loadRdf(Source.TriplyDb.rdf('odissei', 'mcal', {graphs: [url]}))(ctx, () => Promise.resolve())
  return ctx.store.getQuads({})
}
*/

export default async function (): Promise<Etl> {
  const etl = new Etl(destination)
  // const cat_quads =await getRdf("https://mcal.odissei.nl/cv/contentAnalysisType/v0.1/")
  
  etl.use(
    fromCsv(Source.url(cbs_codelib)),
    logRecord(),
    
    when('code',
      addIri({ // Generate IRI for article, use DOI for now
        content: 'code',
        key: '_IRI'
      }),
      triple('_IRI', a, dcm.Software),
      when('CBS_project_nr',
        triple('_IRI', iri(prefix.odissei_kg_schema, str('project')), iri(prefix.cbs_project, 'CBS_project_nr'))
      ),  
      when('title',
        triple('_IRI', dct.title, 'title')
      ),
      when('ShortTitle',
        triple('_IRI', bibo.shortTitle, 'ShortTitle')
      ),
      when(
        context => context.getString('orcid') != 'NA',
        split({
          content: 'orcid',
          separator: ',',
          key: '_orcids'
        }),
        triple('_IRI', dct.creator, iris('_orcids'))
      ),
    ),
    //validate(Source.file('static/model.trig'), {terminateOn:"Violation"}),
    toTriplyDb(destination),
    uploadPrefixes(destination),
  )
  return etl
}