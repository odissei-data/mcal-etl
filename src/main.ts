import { Etl, Source, declarePrefix, environments, fromCsv, toTriplyDb, when } from '@triplyetl/etl/generic'
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
  researchQuestion: prefix.mcal('researchQuestion'),
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
    fromCsv(Source.file(['../mcal-cleaning/Data/Mcalentory.csv'])),
    // fromCsv(Source.TriplyDb.asset(destination.account, destination.dataset, {name: 'Mcalentory.csv'})),
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
      context => context.getString('cf') != 'NA',
      split({
        content: 'cf',
        separator: ',',
        key: '_contentFeatures'
      }),
      custom.change({
        key: '_contentFeatures',
        type: 'unknown',
        change: value => {
          return (value as any).map((value:string) => {
            switch(value) { 
              case '?': return 'CF0';
              case 'naming': return 'CFE6';
              case 'shaming': return 'CFE7';
              case 'anger': return 'CFE8';
              case 'sources': return 'CFE9';
              case 'genre': return 'CFE10';
              case 'date': return 'CFE11';
              case 'number of notifications': return 'CFE12';
              case 'frames': return 'CFE13';
              case 'events': return 'CFE14';
              case 'type': return 'CFE15';
              case 'topics': return 'CFE16';
              case 'linguistic style': return 'CFE17';
              case 'subjectivity tone': return 'CFE18';
              case 'sentiment': return 'CFE19';
              case 'issue attention': return 'CFE20';
              case 'prominence': return 'CFE21';
              case 'authoritativeness': return 'CFE22';
              case 'populism': return 'CFE23';
              case 'visibility': return 'CFE24';
              case 'evaluation': return 'CFE25';
              case 'attention': return 'CFE26';
              case 'tone': return 'CFE27';
              case 'types of news': return 'CFE28';
              case 'attention for actors': return 'CFE29';
              case 'information availability': return 'CFE30';
              case 'evaluations': return 'CFE31';
              case 'framing': return 'CFE32';
              case 'valence': return 'CFE33';
              case 'entertainment coverage': return 'CFE34';
              case 'visibility actors': return 'CFE35';
              case 'presentation of social roles': return 'CFE36';
              case 'non-verbal behavior': return 'CFE37';
              case 'issue communication': return 'CFE38';
              case 'negative attention': return 'CFE39';
              case 'party positions': return 'CFE40';
              case 'real-world developments': return 'CFE41';
              case 'party posistions': return 'CFE42';
              case 'success and failure': return 'CFE43';
              case 'support and criticism': return 'CFE44';
              case 'pejoration': return 'CFE45';
              case 'issue position': return 'CFE45';
              case 'issue developments': return 'CFE46';
              case 'association actors with issues': return 'CFE47';
              case 'strategy framing': return 'CFE48';
              case 'leadership traits': return 'CFE49';
              case 'negative campaigning': return 'CFE50';
              case 'valance framing': return 'CFE51';
              case 'episodic and thematic framing': return 'CFE52';
              case 'populist communication': return 'CFE53';
              case 'actor visibility': return 'CFE54';
              case 'political information supply': return 'CFE55';
              case 'political personalities': return 'CFE56';
              case 'ethical and political contexts': return 'CFE57';
              case 'concept visibility': return 'CFE58';
              case 'associative framing': return 'CFE59';
              case 'symbolic framing': return 'CFE60';
              case 'issue-specific frames': return 'CFE61';
              case 'issue salience': return 'CFE62'
              case 'party salience': return 'CFE63'
              case 'generic frames': return 'CFE64'
              case 'conflict framing': return 'CFE65';
              case 'document similarity': return 'CFE66';
              case 'other: communication strategies': return 'CFE67';
              case 'other: retrieval cues': return 'CFE68';
              case 'other: presence of real-time marketing techniques': return 'CFE69';
              case 'other: retrieval cues': return 'CFE70';
              case 'actor visibilty': return 'CFE71';
              case 'age': return 'CFE72';
              case 'and commercial characteristics': return 'CFE73';
              case 'diversity': return 'CFE71';
              case 'domesticity': return 'CFE71';
              case 'episodic vs. thematic framing': return 'CFE71';
              case 'ethnic stereotypes?': return 'CFE71';
              case 'generic frame': return 'CFE71';
              case 'humour relatedness': return 'CFE71';
              case 'identification features': return 'CFE71';
              case 'interactivity': return 'CFE71';
              case 'journalistic characteristics': return 'CFE71';
              case 'media agenda diversity': return 'CFE71';
              case 'mertonian imperatives about science': return 'CFE71';
              case 'narrative': return 'CFE71';
              case 'news factors': return 'CFE71';
              case 'news topics': return 'CFE71';
              case 'news values': return 'CFE71';
              case 'nudity': return 'CFE71';
              case 'other: brand placement': return 'CFE71';
              case 'other: content overlap': return 'CFE71';
              case 'other: formal features of public service advertisements': return 'CFE71';
              case 'other: humour complexity': return 'CFE71';
              case 'other: journalistic references to the facebook ad library': return 'CFE71';
              case 'other: main topic of the news story': return 'CFE71';
              case 'other: news content': return 'CFE71';
              case 'other: news values': return 'CFE71';
              case 'other: newspaper position and ad characteristics': return 'CFE71';
              case 'other: objectivity': return 'CFE71';
              case 'other: political informaton': return 'CFE71';
              case 'other: product placement disclosure appearances': return 'CFE71';
              case 'other: sensationalism in storytelling': return 'CFE71';
              case 'other: sensationalist news features': return 'CFE71';
              case 'other: technology': return 'CFE71';
              case 'populist communication frames and frame elements': return 'CFE71';
              case 'populist content': return 'CFE71';
              case 'populist elements': return 'CFE71';
              case 'populist style': return 'CFE71';
              case 'preclearance policies': return 'CFE71';
              case 'prevention vs. repressive framing': return 'CFE71';
              case 'primary actor': return 'CFE71';
              case 'product categories': return 'CFE71';
              case 'product characteristics': return 'CFE71';
              case 'type of source': return 'CFE71';
              case 'uncertainty in economic news': return 'CFE71';
              case 'visibility and framing  of new political parties': return 'CFE71';
              case 'visual self-presentation': return 'CFE71';
              default:
                console.error(value); // process.exit(1);
                return 'CF0'
            }
          })
        }
      }),
      triple('_article', mcal.contentFeature, '_contentFeatures')
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
      [mcal.fair, 'fair'],
      [mcal.preRegistered,'preRegistered'],
      [mcal.openAccess, 'openAccess']
    ),
    pairs('_journal',
      [a, bibo.Journal],
      [dct.title, 'journal']
    ),
    //loadRdf(Source.TriplyDb.rdf('odissei','mcal',{graphs: ["https://mcal.odissei.nl/cv/contentAnalysisType/v0.1/"]})),
    //loadRdf(Source.TriplyDb.rdf('odissei','mcal',{graphs: ["https://mcal.odissei.nl/cv/researchQuestionType/v0.1/"]})),
    validate(Source.file('static/model.trig'), {terminateOn:"Violation"}),
    toTriplyDb(destination)
  )
  return etl
}
