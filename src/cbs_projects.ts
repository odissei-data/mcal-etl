import {Etl, Source, declarePrefix, environments, when, toTriplyDb, uploadPrefixes, fromXlsx  } from '@triplyetl/etl/generic'
import { addIri, custom, iri, str, triple } from '@triplyetl/etl/ratt'
// import { addIri, custom, iri, iris, lowercase, pairs, split, triple } from '@triplyetl/etl/ratt'
 //import { logRecord } from '@triplyetl/etl/debug'
 //import { xsd } from '@triplyetl/etl/vocab' // dct
// import { validate } from '@triplyetl/etl/shacl'

// Declare prefixes.
const prefix_base = declarePrefix('https://kg.odissei.nl/')

const prefix = {
  graph: declarePrefix(prefix_base('graph/')),
  odissei_kg_schema: declarePrefix(prefix_base('schema/')),
  codelib: declarePrefix(prefix_base('cbs_codelib/')),
  cbs_pr: declarePrefix(prefix_base('cbs/project/')),
  cbs_organisation: declarePrefix(prefix_base('cbs/organisation/')),
  cbs_dataset: declarePrefix(prefix_base('cbs/dataset/')),
  doi: declarePrefix('https://doi.org/'),
  orcid: declarePrefix('https://orcid.org/'),
  issn: declarePrefix('https://portal.issn.org/resource/ISSN/'),
  foaf: declarePrefix('http://xmlns.com/foaf/0.1/')
}

// ETL input data: spreadsheets with CBS project information taken from HTML overview page at:
// https://www.cbs.nl/nl-nl/onze-diensten/maatwerk-en-microdata/microdata-zelf-onderzoek-doen/instellingen-en-projecten
const cbs_projects_before = 'https://www.cbs.nl/-/media/cbs-op-maat/zelf-onderzoek-doen/projecten_met_bestanden_einddatum_voor_2024.xlsx'
const cbs_projects_after = 'https://www.cbs.nl/-/media/cbs-op-maat/zelf-onderzoek-doen/projecten_met_bestanden_einddatum_na_2023.xlsx'

const destination = {
  defaultGraph: prefix.graph('projects'),
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
    fromXlsx([Source.url(cbs_projects_before), Source.url(cbs_projects_after)]),
    
    
  
    
    when('Projectnummer',
        addIri({ // Generate IRI for article, use DOI for now
            content: 'Projectnummer',
            prefix: prefix.cbs_pr,
            key: '_IRI'
        }),
        when('Bestandsnaam',
            triple('_IRI', iri(prefix.odissei_kg_schema, str('bestandsnaam')), iri(prefix.cbs_dataset, 'Bestandsnaam'))
        ),
        when('Instelling',
             triple('_IRI', iri(prefix.odissei_kg_schema, str('instelling')), iri(prefix.cbs_organisation, 'Instelling'))
        ),
         when('Startdatum',
            custom.change({
              change: value => new Date((value - 25569)*86400*1000),
              type: 'number',
              key: 'Startdatum',
            }),
            triple('_IRI', iri(prefix.odissei_kg_schema, str('startDate')), 'Startdatum')
        ),
        when('Einddatum',
            custom.change({
              change: value => new Date((value - 25569)*86400*1000),
              type: 'number',
              key: 'Einddatum',
            }),
            triple('_IRI', iri(prefix.odissei_kg_schema, str('endDate')), 'Einddatum')
        )
    
      ),
    

    
    //validate(Source.file('static/model.trig'), {terminateOn:"Violation"}),
    toTriplyDb(destination),
    uploadPrefixes(destination),
  )
  return etl
}

