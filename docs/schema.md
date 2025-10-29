# 🧱 Database Schema

Below is the full schema diagram used in the GBC publication analysis project.

![GBC database schema](gbc_schema_diagram.png)

## Core Tables

### `resource`
- Describes a biodata resource
- Linked to URLs and associated connection statuses/online status
- Linked to publications by inventory, mentions and accessions/data citation
    - inventory links represented in the `resource_publication` table
    - mentions links represented in the `resource_mention` table
    - accession/data citation links represented in the `accession` and `accession_publication` tables
- Versioned by `version_id`, which is fully described in the `version` table
    - each workflow run/data source represented by a different version, so provenece is captured by this too
- `is_latest` represents the most recent version of the resource
- `is_gcbr` captures GCBR status
- Represented in API by [`Resource` object](api/globalbiodata_resource.md)

### `publication`
- Stores metadata for publications (title, journal, year, etc.)
- Affiliation country inferred upon import
- Linked to resources by inventory, mentions and accessions/data citation
    - inventory links represented in the `resource_publication` table
    - mentions links represented in the `resource_mention` table
    - accession/data citation links represented in the `accession` and `accession_publication` tables
- Linked to associated grants and grant agencies
- Represented in API by [`Publication` object](api/globalbiodata_publication.md)

### `resource_publication`
- Joins resource and publication tables, allowing a many-to-many type of relationship
- This link represents the inventory. i.e. a link here means that the publication _describes_ the resource.
- Represented in API as the `.publications` attribute of a [`Resource` object](api/globalbiodata_resource.md)

### `url`
- Describes URL of a resource
- Represented in API by [`URL` object](api/globalbiodata_url.md#globalbiodata.url.URL)


### `connection_status`
- Describes ping/connection information for a URL
- `is_online` inferred from ping return status
- Represented in API by [`ConnectionStatus` object](api/globalbiodata_url.md#globalbiodata.url.ConnectionStatus)

### `grant`
- Describes grants associated with resources/publications and their agencies
- Represented in API by [`Grant` object](api/globalbiodata_grant.md#globalbiodata.grant.Grant)

### `grant_agency`
- Records for grant_agencies (name, estimated country)
- `parent_agency_id` and `representative_agency_id` represents relationships between agencies
    - `parent_agency_id` introduces hierarchical relationships (i.e. agency X funds agency Y)
    - `representative_agency_id` groups different names/aliases for the _same_ agency (i.e. XYZ and X.Y.Z. are the same agency, and XYZ is the representative/canonical name for the whole group)
- Represented in API by [`GrantAgency` object](api/globalbiodata_grant.md#globalbiodata.grant.GrantAgency)

### `resource_grant`
- Joins resource and grant tables
- Allows a many-to-many relationship between the two
- Represented in API as the `.grants` attribute of a [`Resource` object](api/globalbiodata_resource.md)

### `publication_grant`
- Joins publication and grant tables
- Allows a many-to-many relationship between the two
- Represented in API as the `.grants` attribute of a [`Publication` object](api/globalbiodata_publication.md)

### `version`
- Describes the pipeline/process that identified the data
- Means of versioning & data provenece
- Represented in API by [`Version` object](api/globalbiodata_version.md)

### `accession`
- Holds data citations/accessions plus their associated metadata
- Links to resources via `resource_id`
- Versioned by `version_id`
- Represented in API by [`Accession` object](api/globalbiodata_accession.md)

### `accession_publication`
- Maps accessions to the publications they were found in
- Table to join accession and publication tables, allowing a many-to-many type of relationship
- Represented in API as the `.publications` attribute of an [`Accession` object](api/globalbiodata_accession.md)

### `resource_mention`
- Links resources to publications that have mentioned the resource's name in the article text
- Includes context, classifier confidence, etc.
- Represented in API by [`ResourceMention` object](api/globalbiodata_resource_mention.md)


## Note: additional tables
2 additional tables are present in the schema definition file, which are largely independent of this analysis work:

- `open_letter` : track the signatures of [GBC's open letter](https://globalbiodata.org/open-letter-campaign/)
- `wildsi` : imported data from the [WilDSI project](https://apex.ipk-gatersleben.de/apex/wildsi/r/wildsi/home), about sequence data usage

These tables **do not** have API classes.