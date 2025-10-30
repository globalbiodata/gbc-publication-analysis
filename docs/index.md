<div class="header-split">
  <div class="hs-left" markdown>
    <h1>GBC Publication Analysis</h1>
  </div>
  <div class="hs-right">
    <img src="assets/gbc-logo.png" alt="GBC logo">
  </div>
</div>

## 🧭 Introduction / Background

The [Global Biodata Coalition](https://globalbiodata.org) seeks to exchange knowledge and share strategies for supporting biodata resources. To develop an underlying evidence base to show the importance of biodata resources to the life sciences community at large, several data analyses were undertaken. All were based on mining published scientific literature, with the help of Europe PMC's APIs.

#### 1. A global inventory
The aim here is to identify publications that describe a resource and form a list of known biodata resources.

#### 2. Resource mentions
Here, we wish to capture the usage of these inventory resources by detecting mentions of their names & aliases in open-access full-text articles. This seeks to capture the more informal type of resource citation (outside of official publication references and/or data citations).

#### 3. Data citations
Finally, data from these resources can be cited directly by accession number (or other resource-dependent identifier). These are annotated as part of Europe PMC's text-mining service and have been imported into our database as an additional data source.

## API Summary

These pages provide a complete reference for the Global Biodata Coalition publication analysis toolkit — including the main Python modules, utility functions, and the underlying database schema.

- 🧩 **`globalbiodata` module**: Core ORM-style classes for interacting with publications, resources, accessions, and related data.
- 🧰 **Utility modules**: Helper functions for database connections, querying, parsing, and text-cleaning tasks. Requirements for many of our analysis workflows.
- 🛢️ **Database schema**: Overview of the MySQL schema and entity relationships used throughout the analysis.

Each section includes auto-generated documentation pulled from in-code docstrings, alongside examples and structural diagrams where applicable.
