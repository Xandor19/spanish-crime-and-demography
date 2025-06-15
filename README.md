# spanish-crime-and-demography

Spark-based (unnecessarily, for the sake of practicing) ETLs to clean and integrate publicly available data of crime, demography and economy of Spain

## Structure

The project follows a typical three-tier data architecture plus an additional "staging" zone. The latter is the entry point for data, in a raw, semi-structured and non-distributed format (individual plain text files). Ingest into the theoretical Datalake starts with its reformatting and distribution (again, data volumes don't require it, but for practicing purposes) in a staging-to-bronze step. Tier purposes, overly simplified as steps vary among the different sources, are as follow:

- Bronze: semi-structured data still without fixed schemas only with transformation to make it Parquet-compliant (for instance, column names format) and to allow for correct partitioning to be done.
- Silver: already structured data with consistent format among all partitions
- Gold: cross-source integration allowing to perform joint analysis over the ingested data

## Base Stack

The project has been developed in Databrics with Scala as main (and most likely only) programming language. Databricks runtime is 15.4 LTS which features:
- Scala 2.12 version
- Spark 3.5.0

Currently, ingest is designed to work within Azure ecosystem, which limits reproducibility. My plan is to migrate everything to basic Databricks, so it can be run in community or free plans.

## Current state

By the time, staging-silver processes for crime and basic demography data are finished. As stated before, the dataflow is designed to work within Azure, so, to run the project it is required to:

- Have an Azure Storage Account or Azure Data Lake Gen 2 with key-based access policy and four buckets:
    1. staging
    2. bronze
    3. silver
    4. gold
- Have an external step to parse incoming Excels into temporary CSV files inside a staging/tmp directory (in this case, a Azure Data Factory Copy Data activity was used).
- Have an external orchestrator to handle the notebooks run and parameters passing (in this case, Azure Data Factory handled the whole process, starting with the Copy Data mentioned earlier and calling the notebooks depending on which dataset was being ingested)
- Set up the Azure Databricks own secrets store (not Azure Key Vault)

## Repository how-to

The repository contains both the ETL processes as well as the data used for testing. While other time ranges could be directly obtained from the official sources, format inconsistencies and differences in which data was published must be taken into account. Structure is:

```
|-README.md -> this file containing basic information
|-data
|     |-<file-name>.xlsx -> Excel files with data, the names indicate origin dataset
|-notebooks
      |-<dataset><origin-layer><destination-layer>.scala -> Raw notebook files for the corresponding dataset and layer transitions
```
### Commit scopes

The repository uses conventional commits with scopes, which are:

- notebooks: changes made to the notebooks that perform the ETL processes
- resources: changes made to project resources other than code (e.g., test data; the scope is intentionally broad to cover other resource types)