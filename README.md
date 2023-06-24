# Data Contracts

## Context

Assume you have 2 services:

* Service 1 is responsible for creating a table A
* Service 2 requires using data from that same table A

You can modify Service 1 to change the format/schema/content of table A, like:

* Remove/Add a column
* Change the data type of a columns
    * Change from Integer to String
* Change the semantic meaning of a column
* Change the constraints on the values of a column
    * Column uniqueness
    * Column cannot be empty
    * Column allowed values

On the other hand, the "requirements" from Service 2 can be more or less strict
    * It only needs a subset of columns from table A
    * It requires a subset of the Allowed Values for a column
        * E.g. Service 2 only cares about properties in the COUNTRY = UK, so if COUNTRY = FRANCE is removed from table A, it doesn't care.

Given the possible scenarios above, a challenge that emerges is with knowing, at time of the MR/PR (merge/pull request), if the incoming changes to one of the Services will cause Service 2's table A expectations to not be met.
    * Service 1 will remove a column from table A, when that same column is required by Service 2.
    * Service 2 adds a new expectation of a Column X that is not being produced by Service 1

Additionally, even if "on paper" the two services agree regarding table A, it can happen that during execution, one of the services encounters data that do not match the expectations.
    * Service 1 says column X cannot be empty, Service 2 expects column X to not be empty, but column X has empty values when retrieved by Service 2. Service 1 may have emitted a Warning, but whether it stops Column X from being propagated is a whole different matter.

## Goal

The goal of Data Contracts is to allow different Services in a System to share their expectations and requirements on Tables, allowing those expectations and requirements to be compared and the system to bring awareness of potentially incompatible expectations from their Services.

One key aspect of achieving this is making sure there is a standardised way of declaring the expectations and requirements that a Service has on a Table.


## What is a Data Contract

A Data Contract is an artifact containing a collection of metadata about a Table. Such table can be a database table, or a "file based" table, like CSV, Excel or Parquet.

A Data Contract can refer to either an Incoming Table or an Outcoming Table. If the Data Contract refers to an Outcoming Table it can be called a Producer Data Contract. On the other hand, if the Data Contract refers to an Incoming Talbe it can be called a Consumer Data Contract.

The minimum metadata a Data Contract should contain is a list of columns in a table, alongside their description, data types and basic constraints. It could also contain more information, but those would most likely vary across the different implementations of Data Contracts.

For a system, there should be a centralised "data contracts catalog", that keeps track of all "producers" and "consumers" of Data Contracts and can perform "compatibility checks" between them.

In a well designed system, a table should only be "produced" by a single Service, whereas many Services may "consume" it. This leads to the constraint that "no 2 distinct Producer Data Contracts may refer to the same Table".

## Data Contract Implementation

Data Contracts can be represented in many ways. One way that I particularly like is "metadata only parquet files" where the Metadata comes from an Arrow Schema. The reasons I like this are the following:

* Arrow is becoming the standard way of manipulating data in a Service, and Arrow is itself a "universal standard" that has implementations in all mainstream programming languages like Python, Java, Rust, Javascript, Go, R, etc.
    * This means that the Data Contracts are agnostic of what programming language was used to create it.

* Arrow Schemas contain not only the list of columns and their types, but also allow the adition of Custom Metadata to both individual Columns and to the whole table.
    * This Custom Metadata can contain virtually anything. In particular, it can contain Column descriptions, the specification of column tests, etc, and within a System the distinct Services should agree on how to use the Custom Metadata

* You can very easily write an Arrow Schema in several languages and save it to a Metadata Only Parquet File

