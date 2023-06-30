import { Etl, Source, declarePrefix, environments, fromCsv, loadRdf, toTriplyDb, when } from '@triplyetl/etl/generic'
import { addIri, custom, iri, iris, lowercase, pairs, split, triple } from '@triplyetl/etl/ratt'
import { logRecord } from '@triplyetl/etl/debug'
import { bibo, dct, a } from '@triplyetl/etl/vocab'
import { validate } from '@triplyetl/etl/shacl'

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
  rqt: declarePrefix(prefix_cv_base('researchQuestionType/v0.1/')),
}

const mcal = {
  /**
   * Properties defined by MCAL because we could not 
   * find an awesome property somewhere else...
   */
  comparativeStudy: prefix.mcal('comparativeStudy'),
  contentFeature: prefix.mcal('contentFeature'),
  contentAnalysisType: prefix.mcal('contentAnalysisType'),
  contentAnalysisTypeAutomated: prefix.mcal('contentAnalysisTypeAutomated'),
  dataAvailableType: prefix.mcal('dataAvailableType'),
  dataAvailableLink: prefix.mcal('dataAvailableLink'),  
  fair: prefix.mcal('fair'),
  material: prefix.mcal('material'),
  openAccess: prefix.mcal('openAccess'),
  preRegistered: prefix.mcal('preRegistered'),
  relevantForMCAL: prefix.mcal('relevantForMcal'),
  reliability: prefix.mcal('reliability'),
  reliabilityType: prefix.mcal('reliabilityType'),
  researchQuestionType: prefix.mcal('researchQuestionType')
}

const graph = {
  instances: prefix.graph('instances')
}

const destination = {
  account: Etl.environment === environments.Development ? "jacco-van-ossenbruggen" : 'odissei',
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
    fromCsv(Source.TriplyDb.asset(destination.account, destination.dataset, {name: 'Mcalentory.csv'})),
    logRecord(),
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
    when(
      context => context.getString('contentFeatures') != 'NA',
      split({
        content: 'contentFeatures',
        separator: ',',
        key: '_contentFeatures'
      }),
      triple('_article', mcal.contentFeature, iris(prefix.mcal, '_contentFeatures'))
    ),
    when(
      context => context.getString('contentAnalysisType') != 'NA',
      split({
        content: 'contentAnalysisType',
        separator: ',',
        key: '_contentAnalysisTypes'
      }),
      custom.change({
        key: '_contentAnalysisTypes',
        type: 'unknown',
        change: value => {
          return (value as any).map((value:string) => {
            switch(value) {
              case 'Qualitative analysis': return 'CAT1';
              case 'Qualitative coding': return 'CAT1'; // to be checked with MCAL team
              case 'Quantitative analysis': return 'CAT2';
              case "Quantitative analysis with manual coding": return 'CAT3';
              case 'Automated analysis': return 'CAT4';
              case 'Other, please describe': return 'CAT0';
              case '?': return 'CAT0';
              default: return value;
            }
          })
        }
      }),
      triple('_article', mcal.contentAnalysisType, iris(prefix.cat, '_contentAnalysisTypes'))
    ), 
    when(
      context => context.getString('rq') != 'NA',
      split({
        content: 'rq',
        separator: ',',
        key: '_rqs'
      }),
      custom.change({
        key: '_rqs',
        type: 'unknown',
        change: value => {
          return (value as any).map((value:string) => {
            switch(value) {
              case 'descriptive': return 'RQT1';
              case 'explaining media content': return 'RQT2';
              case 'effects on citizens': return 'RQT3';
              case 'effects on policy/politics': return 'RQT4';
              case 'others, namely...': return 'RQT0';
              default: return value;
            }
          })
        }
      }),
      triple('_article', mcal.researchQuestionType, iris(prefix.rqt, '_rqs'))
    ),
    when(
      context=> context.getString('material') != 'NA',
      split({
        content: 'material',
        separator: ',',
        key: '_materials'
      }),
      triple('_article', mcal.material, '_materials')
    ),
    when(
      context=> context.getString('countries') != 'NA',
      split({
        content: 'countries',
        separator: ',',
        key: '_countries'
      }),
      triple('_article', dct.spatial, '_countries')
    ),
    when(
      context=> context.getString('dataAvailableType') != 'NA',
      split({
        content: 'dataAvailableType',
        separator: ',',
        key: '_dataAvailableTypes'
      }),
      triple('_article', mcal.dataAvailableType, '_dataAvailableTypes')
    ),
    when(
      context=> context.getString('dataAvailableLink') != 'NA',
      triple('_article', mcal.dataAvailableLink, 'dataAvailableLink')
    ),
    when(
      context => context.getString('doi') != 'NA',
      triple('_article', dct.relation, iri('doi'))
    ),
    when(
      context => context.getString('issn') != 'NA',
      triple('_journal', bibo.issn, iri(prefix.issn, 'issn'))
    ),
    custom.change({
      key: 'relevant', 
      type: 'string',
      change: value => { 
        switch(value) { 
          case 'yes': return true;
          case 'Yes': return true;
          default: return false
        }}}), 
    when(
      context=> context.getString('reliabilityType') != 'NA',
      lowercase({
        content: 'reliabilityType',
        key: '_reliabilityType'
      }),
      split({
        content: '_reliabilityType',
        separator: ',',
        key: '_reliabilityTypes'
      }),
      triple('_article', mcal.reliabilityType, '_reliabilityTypes')
    ),

    pairs('_article',
      [a, bibo.AcademicArticle],
      [dct.title, 'title'],
      [dct.isPartOf, '_journal'],
      [dct.date, 'publicationDate'],
      [dct.hasVersion, iri('correspondingArticle')],
      [dct.temporal, 'period'] ,
      [mcal.relevantForMCAL, 'relevant'],
      [mcal.comparativeStudy, 'comparativeStudy'],
      [mcal.reliability, 'reliability'],
      [mcal.contentAnalysisTypeAutomated, 'contentAnalysisTypeAutomated'],
      [mcal.fair, 'fair'],
      [mcal.preRegistered,'preRegistered'],
      [mcal.openAccess, 'openAccess']
    ),
    pairs('_journal',
      [a, bibo.Journal],
      [dct.title, 'journal']
    ),
    loadRdf(Source.TriplyDb.rdf('odissei','mcal',{graphs: ["https://mcal.odissei.nl/cv/contentAnalysisType/v0.1/"]})),
    loadRdf(Source.TriplyDb.rdf('odissei','mcal',{graphs: ["https://mcal.odissei.nl/cv/researchQuestionType/v0.1/"]})),
    
    validate(Source.file('static/model.trig'), {terminateOn:"Never"}),
    toTriplyDb(destination)
  )
  return etl
}