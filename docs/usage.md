# Usage

## Create database connection
```python
import globalbiodata as gbc
import gbcutils.db as gbc_db

# fetch read-only connection to the GBC cloud SQL db
gcp, db_engine, db_conn = gbc_db.get_gbc_connection(readonly=True)
```

## Fetch publication by PubMed ID
```python
# fetch a publication by PubMed ID
pub = gbc.Publication.fetch_by_pubmed_id(33651556, conn=db_conn)
print("Publication search result:")
print(f"pubmed_id: {pub.pubmed_id}")
print(f"title: {pub.title}")
print(f"authors: {pub.authors}")
```

## Search for resources by name and inspect properties
```python
# fetch a resource
chebi = gbc.Resource.fetch_by_name('ChEBI', conn=db_conn)
print("ChEBI search result:")
print(f"short_name: {chebi.short_name}")
print(f"common_name: {chebi.common_name}")
print(f"full_name: {chebi.full_name}")
print(f"is_gcbr: {chebi.is_gcbr}")
print(f"is_online: {chebi.is_online()}") # note that this is a function call not a property

# check all inventory publications for your resource
# and find first published record of this resource
first_published, inv_pubmed_ids = None, []
for inv_pub in chebi.publications:
    inv_pubmed_ids.append(inv_pub.pubmed_id)
    if not first_published or inv_pub.publication_date < first_published.publication_date:
        first_published = inv_pub
print(f"All inventory publication PubMed IDs for {chebi.short_name}: {inv_pubmed_ids}")
print(f"First published record of {chebi.short_name}:")
print(f"\tpubmed_id: {first_published.pubmed_id}")
print(f"\ttitle: {first_published.title}")
print(f"\tpublication_date: {first_published.publication_date}")
```

## Find all publications citing a specific accession
```python
# find the PMC ID for all publications that contain the accession '0.9.4.1'
accessions = gbc.Accession.fetch_by_accession('0.9.4.1', conn=db_conn)
for accession in accessions:
    accession_pmc_ids = [ap.pmc_id for ap in accession.publications if ap is not None]
    print(f"accession: {accession.accession}; accession_pmc_ids: {accession_pmc_ids}")
```

## Fetch all publications mentioning a specific resource
```python
# fetch all publications mentioning a specific resource
allie = gbc.Resource.fetch_by_name('Allie', conn=db_conn)
resource_mentions = gbc.ResourceMention.fetch_by_resource_id(allie.id, conn=db_conn)
# all_pubs_with_mentions = set(mention.publication for mention in resource_mentions if mention.publication is not None)
for mention in resource_mentions:
    found_aliases = [alias.matched_alias for alias in mention.matched_aliases]
    print(f"pubmed_id: {mention.publication.pubmed_id}; match_count: {mention.match_count}; matched_aliases: {found_aliases}")
    print(f"\ttitle: {mention.publication.title}\n")
```