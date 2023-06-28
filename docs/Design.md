# Design

* Create Field and Table metadata with class adc.Metadata()
* Create Arrow schema with Field Names and Types, and metadata value coming from class Metadata
* Create DataContract giving it a name, a schema and a direction.

* As part of the CICD pipeline, call `adc check`.
    * This will crawl the directory found in pyproject.toml file or env var X to build the ServiceCatalog object. The ServiceCatalog contains a mapping of all Contract Names found and their corresponding DataContract object
    * Will call method `generate_files` to create all Metadata Only Parquet Files for the Service
    * Will connect to the CatalogRepository and fetch all relevant existing DataContracts from other services
    * Will run the SchemaCompatibility check between all relevant DataContract and yield a compatibility report for each check

* If `adc check` doesn't raise any failures, then call `adc upload`
    * Will call method `upload_catalog_files` of CatalogRepository to replace the existing Service Data Contracts with the new ones coming from the PR/MR.
