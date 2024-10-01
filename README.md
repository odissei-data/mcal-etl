# mcal

## 0. Software dependecies
We assume you have the following software packages installed and available in your command line interface:
- git
- node.js (should come with npm and npx)
- yarn

In order to be able to publish linked data to an online data catalog, TriplyEtl must first be configured.
This is done with the following steps:

```sh
yarn install
```

## 1. Create a TriplyDB API Token

**NOTE** *This step can be ommitted if you already created or provided your token during setup of your project*

Your TriplyDB API Token is your access key to TriplyDB. You can create one in TriplyDB using [this instructions](https://triply.cc/docs/api-token) or you can type (and follow the onscree instructions):

```sh
npx tools create-token
```

Once you have your token, open the file `.env` and write the following line:
`TRIPLYDB_TOKEN=<your-token-here>`

## 2. Developing your ETL

Once you have a token, you can start writing your ETL based on the example file `src/main.ts`.

### 2.1 Transpile

Your ETL is written in TypeScript, but the ETL will be executed in JavaScript.  The following command transpiles your TypeScript code into the corresponding JavaScript code:

```sh
npm run build
```

### 2.1.1 Continuous transpilation

Some developers do not want to repeatedly write the `npm run build` command.  By running the following command, transpilation is performed automatically whenever one or more TypeScript files are changed:

```sh
npm run dev
```

### 2.2 Run

The following command runs your ETL:

```sh
npx etl
```

If you create other ETL's with different filenames (eg. "`src/my-other-etl.js`"), you should run them using this command:

```sh
npx etl lib/my-other-etl
```

For MCAL, the main ETL depends on some other steps that need to be done before you can run the main ETL script.
As part of the main ETL we run a SHACL validation step that includes rules to see if the SKOS concepts URIs 
we use in the generated KG are actually declared as instances of skos:Concept. For this, we need to copy the 
corresponding vocabularies into the ETL environments. 
vocab that is generated in src/cv-contentFeature.ts, so you need to run this first if you start with an empty dataset:
```sh
npx etl lib/cv-contentFeature 
npx etl
```

## 3. Acceptance/Production mode

*Nite: this section migt  not be applicable if you do not use a [DTAP](https://en.wikipedia.org/wiki/Development,_testing,_acceptance_and_production) strategy.*

Every ETL must be able to run in at least two modes:

1. Acceptance mode: published to the user account of the person who runs the ETL or to an organization that is specifically created for publishing acceptance versions.
2. Production mode: published to the official organization and dataset location.

By default, ETLs should run in acceptance mode.  They should be specifically configured to run in production mode.

### 3.1 Command-line flags

One approach for switching from acceptance to production mode makes use of a command-line flag.

The Etl pipeline includes the following specification for the publication location.  Notice that the organization name is not specified:

```ts
destinations: {
  out: Destination.TriplyDb.rdf(datasetName, {overwrite: true})
},
```

With the above configuration, data will be uploaded to the user account that is associated with the current API Token.  Because API Tokens can only be created for users and not for organization, this never uploads to the production organization and always performs an acceptance mode run.

If you want to run the ETL in production mode, use the `--account` flag to explicitly set the organization name.  If, for example, you have to upload your data to the `organizationName` account, you should run the following command:

```bash
npx etl --account organizationName
```

This performs a production run of the same pipeline.

### 3.2 Environment variable

Another approach for switching from acceptance to production mode makes use of an environment variable.

Your Etl pipeline contains the following configuration:

```ts
destinations: {
  publish:
    Etl.environment === 'Production'
    ? Destination.TriplyDb.rdf(organizationName, datasetName, {overwrite: true})
    : Destination.TriplyDb.rdf(organizationName+'-'+datasetName, {overwrite: true})
},
```

Notice that acceptance runs are published under the user account that is associated with the current API Token.

This approach only works when the combined length of the organization name and the dataset name does not exceed 39 characters.

In order to run in production mode, set the following environment variable (or add it to your local `.env` file):

```sh
ENV="Production"
```

### 3.3 Gitlab CI/CD Pipelines

Your project comes with a file called `.gitlab-ci.yml`. This file can be used in Gitlab to create a scheduled pipeline.

For this to work you will at least need the following [variables](https://docs.gitlab.com/ee/ci/variables/) in your CI/CD setting (Settings → CI/CD → Variables):

| Type     | Key            | Value      | Options | Environments | Notes                                                        |
| -------- | -------------- | ---------- | ------- | ------------ | ------------------------------------------------------------ |
| Variable | HEAD           | false      | -       | All          | allows you to run the ETL for a limited ammount of source records |
| Variable | TIMEOUT        | false      | -       | All          | causes the ETL to timeout, eg "1 hour", "1 day", etc.        |
| Variable | ENV            | production | -       | All          | sets the DTAP environment to "production"                    |
| Variable | TRIPLYDB_TOKEN | [hidden]   | Masked  | All          | an [API token](https://triply.cc/docs/triply-api/#Creating-an-API-token) to interact with the TriplyDB Instance |

After you created these variables, you can create a Schedule (CI/CD → Schedules). In the Schedule page you can overwrite the project variables to match you usage scenario. In most cases you should override the `ENV` variable to `testing` or `acceptance` to run one of the other DTAP modes.

## 4. Optional features

This section documents features that are only used in some but not all projects.

### 4.1 Hard-code the account

It is possible to specify the TriplyDB account in which data should be published in the ETL script (`main.ts`).

Sometimes it is useful to be able to specify the TriplyDB account without changing the ETL code.  This can be done by specifying the following environment variable.  This can be done in the file that is already used to specify the API Token (`.env`).

```sh
TRIPLYDB_ACCOUNT=<account>
```

## 5. Setting up the pipeline

For information on how to set up your pipeline, you can visit the [corresponding page](https://git.triply.cc/triply/documentation/-/wikis/Setting-up-a-pipeline-for-an-ETL) on our internal documentation.
