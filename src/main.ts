import { Etl, Source, declarePrefix, environments, fromCsv, toTriplyDb, when } from '@triplyetl/etl/generic'
import { addIri, custom, iri, iris, pairs, split, triple } from '@triplyetl/etl/ratt'
import { logRecord } from '@triplyetl/etl/debug'
import { bibo, dct, a } from '@triplyetl/etl/vocab'
import { validate } from '@triplyetl/etl/shacl'
import { scrypt, secureHeapUsed } from 'crypto'

// Declare prefixes.
const prefix_base = declarePrefix('https://mcal.odissei.nl/')
const prefix = {
  orcid: declarePrefix('https://orcid.org/'),
  journal: declarePrefix(prefix_base('id/j/')),
  article: declarePrefix(prefix_base('id/a/')),
  data: declarePrefix(prefix_base('data/')),
  graph: declarePrefix(prefix_base('graph/')),
  mcal: declarePrefix(prefix_base('schema/'))
}

const mcal = {
  /**
   * Category: property
   *
   * Label: has material
   *
   * Description: Type of material
   */
  material: prefix.mcal('material'),
  relevantForMCAL: prefix.mcal('relevantForMcal'),
  contentFeature: prefix.mcal('contentFeature'),
  researchQuestion: prefix.mcal('researchQuestion'),
  comparativeStudy: prefix.mcal('comparativeStudy'),
  reliability: prefix.mcal('reliability'),
  contentAnalysisTypeAutomated: prefix.mcal('contentAnalysisTypeAutomated'),
  fair: prefix.mcal('fair'),
  preRegistered: prefix.mcal('preRegistered'),
  openAccess: prefix.mcal('openAccess')
}

const graph = {
  instances: prefix.graph('instances')
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
    fromCsv(Source.TriplyDb.asset('odissei', 'mcal', {name: 'Mcalentory.csv'})),
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
      context=> context.getString('material') != 'NA',
      split({
        content: 'material',
        separator: ',',
        key: '_materials'
      }),
      triple('_article', mcal.material, '_materials')
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
    pairs('_article',
      [a, bibo.AcademicArticle],
      [dct.title, 'title'],
      [dct.isPartOf, '_journal'],
      [dct.date, 'publicationDate'],
      [dct.relation, 'doi'],
      [dct.hasVersion, iri('correspondingArticle')],
      [dct.temporal, 'period'] ,
      [dct.spatial, 'countries'],
      [mcal.relevantForMCAL, 'relevant'],
      [mcal.researchQuestion, 'rq'],
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
    logRecord(),
    validate(Source.file('static/model.trig')),
    toTriplyDb(destination)
  )
  return etl
}
