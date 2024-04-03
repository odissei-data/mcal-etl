import {Etl, Source, declarePrefix, environments, fromXlsx, when, toTriplyDb, uploadPrefixes } from '@triplyetl/etl/generic'
import { addIri, iri, str, triple } from '@triplyetl/etl/ratt'
// import { addIri, custom, iri, iris, lowercase, pairs, split, triple } from '@triplyetl/etl/ratt'
import { logRecord } from '@triplyetl/etl/debug'
import { bibo, a, dct } from '@triplyetl/etl/vocab' // dct
// import { validate } from '@triplyetl/etl/shacl'

// Declare prefixes.
const prefix_base = declarePrefix('https://kg.odissei.nl/')

const prefix = {
  graph: declarePrefix(prefix_base('graph/')),
  odissei_kg_schema: declarePrefix(prefix_base('schema/')),
  cbs_project: declarePrefix(prefix_base('cbs/project/')),
  doi: declarePrefix('https://doi.org/'),
  orcid: declarePrefix('https://orcid.org/'),
  issn: declarePrefix('https://portal.issn.org/resource/ISSN/')
}

const cbs_zotero_bib = 'https://docs.google.com/spreadsheets/d/1JDjvKf3sf60e9_8v-ef0IkyCNxA9y0jlLuCBkcbM-fs/export?gid=1386315381'

const destination = {
  defaultGraph: prefix.graph('papers'),
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
    fromXlsx(Source.url(cbs_zotero_bib)),
    logRecord(),
    
    when('DOI',
      addIri({ // Generate IRI for article, use DOI for now
        prefix: prefix.doi,
        content: 'DOI',
        key: '_IRI'
      }),
      triple('_IRI', a, bibo.AcademicArticle),
      when('Projectnumber',
        triple('_IRI', iri(prefix.odissei_kg_schema, str('project')), iri(prefix.cbs_project, 'Projectnumber'))
      ),  
      when('Title',
        triple('_IRI', dct.title, 'Title')
      ),
      when('ShortTitle',
        triple('_IRI', bibo.shortTitle, 'ShortTitle')
      )

    ),
    //validate(Source.file('static/model.trig'), {terminateOn:"Violation"}),
    toTriplyDb(destination),
    uploadPrefixes(destination),
  )
  return etl
}