import { Context, Etl, Source, declarePrefix, environments, fromCsv, loadRdf, toTriplyDb, uploadPrefixes, when } from '@triplyetl/etl/generic'
import { addIri, custom, iri, iris, lowercase, pairs, split, triple } from '@triplyetl/etl/ratt'
// import { logRecord } from '@triplyetl/etl/debug'
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
  cf: declarePrefix(prefix_cv_base('contentFeature/v0.1/')),
  rqt: declarePrefix(prefix_cv_base('researchQuestionType/v0.1/')),
}

const mcal = {
  /**
   * Properties defined by MCAL because we could not 
   * find an awesome property somewhere else...
   */
  comparativeStudy: prefix.mcal('comparativeStudy'),
  contentFeatureConcept: prefix.mcal('contentFeatureConcept'),
  contentFeature: prefix.mcal('contentFeature'),  // link to skos:Concept version of string in contentFeatureConcept
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
  researchQuestion: prefix.mcal('researchQuestion'),
  researchQuestionType: prefix.mcal('researchQuestionType')  
}

const destination = {
  defaultGraph: prefix.graph('mcalentory'),
  account: process.env.USER ?? "odissei",
  prefixes: prefix, 
  dataset:
    Etl.environment === environments.Acceptance
      ? 'mcal-acceptance'
      : Etl.environment === environments.Testing
        ? 'mcal-testing'
        : 'mcal'
}

const getRdf = async (url: string) => {
  const ctx = new Context(new Etl())
  await loadRdf(Source.TriplyDb.rdf('odissei', 'mcal', {graphs: [url]}))(ctx, () => Promise.resolve())
  return ctx.store.getQuads({})
}


export default async function (): Promise<Etl> {
  const etl = new Etl(destination)
  const cat_quads =await getRdf("https://mcal.odissei.nl/cv/contentAnalysisType/v0.1/")
  const rq_quads = await getRdf("https://mcal.odissei.nl/cv/researchQuestionType/v0.1/")
  const cf_quads = await getRdf("https://mcal.odissei.nl/cv/contentFeature/v0.1/")

  etl.use(
    // fromCsv(Source.file(['../mcal-cleaning/Data/Mcalentory.csv'])),
    fromCsv(Source.TriplyDb.asset(destination.account, destination.dataset, {name: 'Mcalentory.csv'})),
    // logRecord(),
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
      context => context.getString('cf') != 'NA', // cf = simplified contentFeature column
      split({
        content: 'cf',
        separator: ',',
        key: '_cfs'
      }),
      custom.change({
        key: '_cfs',
        type: 'unknown',
        change: value => {
          return (value as any).map((value:string) => {
            switch(value) { 
              case '?': return 'CFE0';
              case 'actor visibility': return 'CFE2';
              case 'age': return 'CFE72';
              case 'anger': return 'CFE8';
              case 'association actors with issues': return 'CFE47';
              case 'associative framing': return 'CFE59';
              case 'attention for actors': return 'CFE29';
              case 'attention': return 'CFE26';
              case 'authoritativeness': return 'CFE22';
              case 'commercial characteristics': return 'CFE73';
              case 'concept visibility': return 'CFE5';
              case 'conflict framing': return 'CFE65';
              case 'date': return 'CFE11';
              case 'diversity': return 'CFE74';
              case 'document similarity': return 'CFE66';
              case 'domesticity': return 'CFE75';
              case 'entertainment coverage': return 'CFE34';
              case 'episodic and thematic framing': return 'CFE52';
              case 'episodic vs. thematic framing': return 'CFE76';
              case 'ethical and political contexts': return 'CFE57';
              case 'ethnic stereotypes?': return 'CFE77';
              case 'evaluation': return 'CFE25';
              case 'evaluations': return 'CFE31';
              case 'events': return 'CFE14';
              case 'frames': return 'CFE13';
              case 'framing': return 'CFE32';
              case 'generic frame': return 'CFE78';
              case 'generic frames': return 'CFE64'
              case 'genre': return 'CFE4';
              case 'humour relatedness': return 'CFE79';
              case 'identification features': return 'CFE80';
              case 'information availability': return 'CFE30';
              case 'interactivity': return 'CFE81';
              case 'issue attention': return 'CFE1';
              case 'issue communication': return 'CFE38';
              case 'issue developments': return 'CFE46';
              case 'issue position': return 'CFE45';
              case 'issue salience': return 'CFE62'
              case 'issue-specific frames': return 'CFE61';
              case 'journalistic characteristics': return 'CFE82';
              case 'leadership traits': return 'CFE49';
              case 'linguistic style': return 'CFE17';
              case 'media agenda diversity': return 'CFE83';
              case 'mertonian imperatives about science': return 'CFE84';
              case 'naming': return 'CFE6';
              case 'narrative': return 'CFE85';
              case 'negative attention': return 'CFE39';
              case 'negative campaigning': return 'CFE50';
              case 'news factors': return 'CFE86';
              case 'news topics': return 'CFE87';
              case 'news values': return 'CFE88';
              case 'non-verbal behavior': return 'CFE37';
              case 'nudity': return 'CFE89';
              case 'number of notifications': return 'CFE12';
              case 'other: brand placement': return 'CFE90';
              case 'other: communication strategies': return 'CFE67';
              case 'other: content overlap': return 'CFE91';
              case 'other: formal features of public service advertisements': return 'CFE92';
              case 'other: humour complexity': return 'CFE93';
              case 'other: journalistic references to the facebook ad library': return 'CFE94';
              case 'other: main topic of the news story': return 'CFE95';
              case 'other: news content': return 'CFE96';
              case 'other: news values': return 'CFE97';
              case 'other: newspaper position and ad characteristics': return 'CFE98';
              case 'other: objectivity': return 'CFE99';
              case 'other: political informaton': return 'CFE100';
              case 'other: presence of real-time marketing techniques': return 'CFE69';
              case 'other: product placement disclosure appearances': return 'CFE101';
              case 'other: retrieval cues': return 'CFE68';
              case 'other: sensationalism in storytelling': return 'CFE102';
              case 'other: sensationalist news features': return 'CFE103';
              case 'other: technology': return 'CFE104';
              case 'party positions': return 'CFE40';
              case 'party salience': return 'CFE63'
              case 'pejoration': return 'CFE45';
              case 'political information supply': return 'CFE55';
              case 'political personalities': return 'CFE56';
              case 'populism': return 'CFE23';
              case 'populist communication frames and frame elements': return 'CFE105';
              case 'populist communication': return 'CFE53';
              case 'populist content': return 'CFE106';
              case 'populist elements': return 'CFE107';
              case 'populist style': return 'CFE108';
              case 'preclearance policies': return 'CFE109';
              case 'presentation of social roles': return 'CFE36';
              case 'prevention vs. repressive framing': return 'CFE110';
              case 'primary actor': return 'CFE111';
              case 'product categories': return 'CFE112';
              case 'product characteristics': return 'CFE113';
              case 'prominence': return 'CFE21';
              case 'real-world developments': return 'CFE41';
              case 'sentiment': return 'CFE3';
              case 'shaming': return 'CFE7';
              case 'sources': return 'CFE9';
              case 'strategy framing': return 'CFE48';
              case 'subjectivity tone': return 'CFE18';
              case 'success and failure': return 'CFE43';
              case 'support and criticism': return 'CFE44';
              case 'symbolic framing': return 'CFE60';
              case 'tone': return 'CFE27';
              case 'topics': return 'CFE16';
              case 'type of source': return 'CFE114';
              case 'type': return 'CFE15';
              case 'types of news': return 'CFE28';
              case 'uncertainty in economic news': return 'CFE115';
              case 'valance framing': return 'CFE51';
              case 'valence': return 'CFE33';
              case 'visibility actors': return 'CFE35';
              case 'visibility and framing  of new political parties': return 'CFE116';
              case 'visibility': return 'CFE24';
              case 'visual self-presentation': return 'CFE117';
              default:
                console.error(value); // process.exit(1);
                return 'CFE0'
            }
          })
        }
      }),
      triple('_article', mcal.contentFeatureConcept, iris(prefix.cf, '_cfs'))
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
              case '?': return 'CAT0';
              case 'Other, please describe': return 'CAT0';
              case 'Qualitative analysis': return 'CAT1';
              case 'Qualitative coding': return 'CAT1'; // to be checked with MCAL team
              case 'Quantitative analysis': return 'CAT2';
              case "Quantitative analysis with manual coding": return 'CAT3';
              case 'Automated analysis': return 'CAT4';
              case 'Associative framing': return 'CAT5';
              case 'Symbolic framing': return 'CAT6';
              case 'Concept visibility': return 'CAT7';

              default: return value;
            }
          })
        }
      }),
      triple('_article', mcal.contentAnalysisType, iris(prefix.cat, '_contentAnalysisTypes'))
    ), 
    when(
      context => context.getString('rqType') != 'NA',
      split({
        content: 'rqType',
        separator: ',',
        key: '_rqTypes'
      }),
      custom.change({
        key: '_rqTypes',
        type: 'unknown',
        change: value => {
          return (value as any).map((value:string) => {
            switch(value) {
              case 'descriptive': return 'RQT1';
              case 'explaining media content': return 'RQT2';
              case 'effects on citizens': return 'RQT3';
              case 'effects on policy/politics': return 'RQT4';
              case 'effects on politics': return 'RQT4';
              case 'effects on companies': return 'RQT5';
              case 'others, namely...': return 'RQT0';
              default: return value;
            }
          })
        }
      }),
      triple('_article', mcal.researchQuestionType, iris(prefix.rqt, '_rqTypes'))
    ),
    when(
      context => context.isNotEmpty('researchQuestion'),
      triple('_article', mcal.researchQuestion, 'researchQuestion')
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
      [mcal.contentFeature, 'contentFeatures'],
      [mcal.fair, 'fair'],
      [mcal.preRegistered,'preRegistered'],
      [mcal.openAccess, 'openAccess']
    ),
    pairs('_journal',
      [a, bibo.Journal],
      [dct.title, 'journal']
    ),

    async (ctx, next) => {
      ctx.store.addQuads(cat_quads)
      ctx.store.addQuads(rq_quads)
      ctx.store.addQuads(cf_quads)
      return await next()
    },
    validate(Source.file('static/model.trig'), {terminateOn:"Violation"}),
    toTriplyDb(destination),
    uploadPrefixes(destination),
  )
  return etl
}