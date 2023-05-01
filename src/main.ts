import {
  Etl,
  Source,
  declarePrefix,
  environments,
  fromXlsx,
  toTriplyDb,
} from "@triplyetl/etl/generic";
import { addIri, iri, pairs } from "@triplyetl/etl/ratt";
import { logRecord } from "@triplyetl/etl/debug";
import { bibo, dct, a } from "@triplyetl/etl/vocab";

// Declare prefixes.
const prefix_base = declarePrefix("https://example.org/");
const prefix_id = declarePrefix(prefix_base("id/"));
const prefix = {
  id: prefix_id,
  graph: declarePrefix(prefix_id("graph/")),
};

const graph = {
  instances: prefix.graph("instances"),
};

const destination = {
  account: Etl.environment === environments.Development ? undefined : "odissei",
  dataset:
    Etl.environment === environments.Acceptance
      ? "mcal-acceptance"
      : Etl.environment === environments.Testing
      ? "mcal-testing"
      : "mcal",
};

export default async function (): Promise<Etl> {
  const etl = new Etl({ defaultGraph: graph.instances });
  etl.use(
    fromXlsx(
      Source.TriplyDb.asset("odissei", "mcal", {
        name: "20220610_MCAL_Inventory_ContentAnalysis.xlsx",
      })
    ),

    addIri({
      prefix: prefix.id,
      content: "JournalID",
      key: "_journal",
    }),
    pairs(
      iri(prefix.id, "ArticleID"),
      [a, bibo.AcademicArticle],
      [dct.title, "Title Article"],
      [dct.isPartOf, "_journal"]
    ),
    pairs("_journal", [a, bibo.Journal], [dct.title, "Journal"]),

    logRecord(),

    //validateShacl(Etl.Source.file('static/model.trig')),
    toTriplyDb(destination)
  );
  return etl;
}
