# 🧭 Introduction / Background

The [Global Biodata Coalition](https://globalbiodata.org) seeks to exchange knowledge and share strategies for supporting biodata resources. To develop an underlying evidence base to show the importance of biodata resources to the life sciences comminity at large, several data analyses were undertaken. All were based on mining published scientific literature, with the help of Europe PMC's APIs.

### 1. A global inventory
The aim here is to identify publications the describe a resources, and form a list of known biodata resources.

### 2. Resource mentions
Here, we wish to capture the usage of these inventory resources by detecting mentions of their names & aliases in open-access full-text articles. This seeks to capture the more informal type of resource citation (outside of official publication references and/or data citations).

### 3. Data citations
Finally, data from these resources can be cited directly by accession number (or other resource-dependent identifier). These are annotated as part of Europe PMC's text-mining service and have been imported into our database as an additional data source.

# 🧱 Database Schema Overview

The project uses a relational schema to link publications, biodata resources, mentions, and data citations.
At a high level:

- `publication` — article metadata
- `resource` — biodata resources
- `resource_mention` — text mentions
- `accession` & `accession_publication` — data citations

👉 [View the full interactive schema diagram](https://drawsql.app/teams/gbc-4/diagrams/gcb-publication-analysis-uber-schema)
👉 [Read full schema documentation](https://your-username.github.io/gbc-publication-analysis/schema/)

### Schema diagram

![GBC database schema diagram](docs/gbc_schema_diagram.png)

# 🧰 Installation & Setup

To install all the required Python modules used by the scripts in this directory, follow the steps below:

## Prerequisites

Ensure you have Python 3.12 installed on your system. You can download it from [python.org](https://www.python.org/).

Install dependencies using
```
pip install -r requirements.txt
```

## Google Cloud Setup & Authentication

The GBC database is hosted on Google's Cloud Platform, so in order to interact with it, we must perform some prior setup.

1. **Install the Google Cloud SDK**

    https://cloud.google.com/sdk/docs/install-sdk

2. **Setup Application Default Credentials (ADC)**

    ```bash
    gcloud auth application-default login
    ```

3. **Install the python connector client**

    ```bash
    pip install "cloud-sql-python-connector[pymysql]"
    ```
    See https://github.com/GoogleCloudPlatform/cloud-sql-python-connector for more information


# 🧠 Project Structure

```
gbc-publication-analysis/
│
├── globalbiodata.py             # Core module with database object classes and helpers
├── gbc_analysis_schema.sql      # Schema definition file (MySQL)
├── gbc_analysis_schema.sqlite   # Schema definition file (SQLite3)
├── gbcutils/                    # Utility modules for parsing, normalisation, database connections, etc.
├── tests/                       # Unit tests for the main module
├── bin/                         # Scripts for analysis operations
└── README.md
```

# 🧑‍💻 Core Modules

## globalbiodata

This module contains a number of helpful methods and classes for interacting with GBC data types.

- Object classes for each database table (e.g. Publication, Resource, ResourceMention)
- Each class includes:
    - Fetch / write methods
    - Helper methods for processing
- Documented with Google-style docstrings — see code for full API

#### Example usage
```python
import globalbiodata as gbc
from gbcutils import gbc_db

(gcp_connector, db_engine, db_conn) = get_gbc_connection()

# Fetch a resource by name
resource = gbc.fetch_resource({'short_name':'test_resource'}, conn=db_conn)
print(resource.publications)

# Fetch all publications about this resource
resource_inventory_pubs = resource.publications

# Identify which resources a publication mentions
for inv_pub in resource_inventory_pubs:
    mentioned_resources = inv_pub.mentions()
```

## gbcutils

- `europepmc` : contains several helpers for interacting with Europe PMC's API's & data
- `db` : helper for opening connections to GBC's Google Cloud SQL instance
- `metadata` : helper methods for sharding & storing article metadata
- `scibert_classify` : helper methods for running ML classifications
