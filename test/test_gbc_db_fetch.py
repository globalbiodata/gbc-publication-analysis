import globalbiodata as gbc
import sqlalchemy as db

db_engine = db.create_engine('sqlite:///./test/test_data/gbc_pytest_db.sqlite')
db_conn = db_engine.connect()
metadata = db.MetaData()

# Test cases for fetching **Resources** from the GBC database
def test_resource_fetch_by_id():
    resource = gbc.fetch_resource({'id':'123'}, expanded=False, conn=db_conn)

    assert resource.id == 123
    assert resource.short_name == 'test_resource'
    assert resource.full_name == 'I am a Test Resource'

    assert type(resource.url) is gbc.URL
    assert resource.url.url == 'www.test-resource.org'
    assert type(resource.url.status) is list
    assert type(resource.url.status[0]) is gbc.ConnectionStatus
    assert resource.url.status[0].status == '404'
    assert resource.url.status[0].is_online == 0

    assert type(resource.version) is gbc.Version
    assert resource.version.name == 'v1.1.1'
    assert resource.version.user == 'carlac'
    assert resource.version.date == '2025-10-17'

    assert resource.publications is None # not expanded

def test_resource_expanded_fetch_by_id():
    resource = gbc.fetch_resource({'id':'123'}, expanded=True, conn=db_conn)

    assert resource.id == 123
    assert resource.short_name == 'test_resource'
    assert resource.full_name == 'I am a Test Resource'

    assert type(resource.url) is gbc.URL
    assert resource.url.url == 'www.test-resource.org'
    assert type(resource.url.status) is list
    assert type(resource.url.status[0]) is gbc.ConnectionStatus
    assert resource.url.status[0].status == '404'
    assert resource.url.status[0].is_online == 0
    assert resource.is_online() is False

    assert type(resource.version) is gbc.Version
    assert resource.version.name == 'v1.1.1'
    assert resource.version.user == 'carlac'
    assert resource.version.date == '2025-10-17'

    assert type(resource.publications) is list
    assert len(resource.publications) == 1
    assert type(resource.publications[0]) is gbc.Publication
    assert resource.publications[0].id == 321
    assert resource.publications[0].pubmed_id == 321123
    assert resource.publications[0].pmc_id == 'PMC321123'
    assert resource.publications[0].publication_date == '2025-01-10'
    assert resource.publications[0].title == 'Publication about test resource'
    assert resource.publications[0].authors == 'A. Guy, C. Lady'
    assert resource.publications[0].affiliation == 'One place; Another place'
    assert resource.publications[0].affiliation_countries == 'Here; There'
    assert resource.publications[0].citation_count == 123

def test_resource_fetch_by_id_from_resource_obj():
    # test here can be a bit more minimal as fetch by id is already tested above
    resource = gbc.Resource.fetch_by_id(123, conn=db_conn)
    assert resource.id == 123
    assert resource.short_name == 'test_resource'
    assert resource.full_name == 'I am a Test Resource'

    assert type(resource.url) is gbc.URL
    assert type(resource.version) is gbc.Version

def test_resource_fetch_by_short_name():
    resource = gbc.fetch_resource({'short_name':'test_resource'}, expanded=False, conn=db_conn)

    assert resource.id == 123
    assert resource.short_name == 'test_resource'
    assert resource.full_name == 'I am a Test Resource'

    assert type(resource.url) is gbc.URL
    assert resource.url.url == 'www.test-resource.org'
    assert type(resource.url.status) is list
    assert type(resource.url.status[0]) is gbc.ConnectionStatus
    assert resource.url.status[0].status == '404'
    assert resource.url.status[0].is_online == 0

    assert type(resource.version) is gbc.Version
    assert resource.version.name == 'v1.1.1'
    assert resource.version.user == 'carlac'
    assert resource.version.date == '2025-10-17'

def test_resource_fetch_by_name_from_resource_obj():
    # test here can be a bit more minimal as fetch by name is already tested above
    resources1 = gbc.Resource.fetch_by_name('test_resource', conn=db_conn)
    assert type(resources1) is list
    assert len(resources1) == 1
    assert resources1[0].id == 123
    assert resources1[0].short_name == 'test_resource'
    assert resources1[0].full_name == 'I am a Test Resource'
    assert type(resources1[0].url) is gbc.URL
    assert type(resources1[0].version) is gbc.Version


    resources2 = gbc.Resource.fetch_by_name('Test Resource', conn=db_conn)
    assert type(resources2) is list
    assert len(resources2) == 1
    assert resources2[0].id == 234
    assert resources2[0].short_name == 'TESTR'

def test_fetch_all_resources():
    resources = gbc.fetch_all_resources(expanded=False, conn=db_conn)

    assert type(resources) is list
    assert len(resources) == 2

    assert resources[0].id == 123
    assert resources[0].short_name == 'test_resource'
    assert resources[0].common_name is None
    assert resources[0].full_name == 'I am a Test Resource'
    assert type(resources[0].url) is gbc.URL
    assert resources[0].url.id == 123
    assert resources[0].url.url == 'www.test-resource.org'


    assert resources[1].id == 234
    assert resources[1].short_name == 'TESTR'
    assert resources[1].common_name == 'Test Resource'
    assert resources[1].full_name is None
    assert type(resources[1].url) is gbc.URL
    assert resources[1].url.id == 234
    assert resources[1].url.url == 'www.testr.co.uk'

def test_fetch_all_online_resources():
    resources = gbc.fetch_all_online_resources(expanded=False, conn=db_conn)

    assert type(resources) is list
    assert len(resources) == 1

    assert resources[0].id == 234
    assert resources[0].short_name == 'TESTR'
    assert resources[0].is_online() is True

    assert type(resources[0].url) is gbc.URL
    assert resources[0].url.id == 234
    assert resources[0].url.url == 'www.testr.co.uk'
    assert resources[0].url.is_online() is True

# Test cases for fetching **Versions** from the GBC database
def test_version_fetch_by_id():
    version = gbc.fetch_version({'id':1}, conn=db_conn)

    assert type(version) is gbc.Version
    assert version.id == 1
    assert version.name == 'v1.1.1'
    assert version.user == 'carlac'
    assert version.date == '2025-10-17'

def test_fetch_version_by_id_from_version_obj():
    version = gbc.Version.fetch_by_id(1, conn=db_conn)

    assert type(version) is gbc.Version
    assert version.id == 1
    assert version.name == 'v1.1.1'
    assert version.user == 'carlac'
    assert version.date == '2025-10-17'

def test_fetch_version_by_user():
    version = gbc.fetch_version({'user':'carlac'}, conn=db_conn)

    assert type(version) is list
    assert len(version) == 2

    assert type(version[0]) is gbc.Version
    assert version[0].id == 1
    assert version[0].name == 'v1.1.1'
    assert version[0].user == 'carlac'
    assert version[0].date == '2025-10-17'

    assert type(version[1]) is gbc.Version
    assert version[1].id == 2
    assert version[1].name == 'v1.1.2'
    assert version[1].user == 'carlac'
    assert version[1].date == '2025-10-17'

def test_fetch_all_versions():
    versions = gbc.fetch_all_versions(conn=db_conn)

    assert type(versions) is list
    assert len(versions) == 3

    assert type(versions[0]) is gbc.Version
    assert versions[0].id == 1
    assert versions[0].name == 'v1.1.1'
    assert versions[0].user == 'carlac'
    assert versions[0].date == '2025-10-17'

    assert type(versions[1]) is gbc.Version
    assert versions[1].id == 2
    assert versions[1].name == 'v1.1.2'
    assert versions[1].user == 'carlac'
    assert versions[1].date == '2025-10-17'

    assert type(versions[2]) is gbc.Version
    assert versions[2].id == 3
    assert versions[2].name == 'v1.1.3'
    assert versions[2].user == 'nobody'
    assert versions[2].date == '2025-01-01'

# Test cases for fetching **URLs and Connection Statuses** from the GBC database
def test_fetch_url_by_id():
    url = gbc.fetch_url({'id':234}, conn=db_conn)

    assert type(url) is gbc.URL
    assert url.id == 234
    assert url.url == 'www.testr.co.uk'

    assert type(url.status) is list
    assert type(url.status[0]) is gbc.ConnectionStatus
    assert url.status[0].status == '200'
    assert url.status[0].is_online == 1
    assert url.status[0].is_latest == 1

    assert type(url.status[1]) is gbc.ConnectionStatus
    assert url.status[1].status == '300'
    assert url.status[1].is_online == 1
    assert url.status[1].is_latest == 0

    assert url.status[2].status == '404'
    assert url.status[2].is_online == 0
    assert url.status[2].is_latest == 0

def test_fetch_all_urls():
    urls = gbc.fetch_all_urls(conn=db_conn)

    assert type(urls) is list
    assert len(urls) == 2

    assert type(urls[0]) is gbc.URL
    assert urls[0].id == 123
    assert urls[0].url == 'www.test-resource.org'
    assert type(urls[0].status) is list
    assert len(urls[0].status) == 1
    assert urls[0].is_online() is False
    assert urls[0].latest_connection_status().status == '404'
    assert urls[0].latest_connection_status().date == '2022-07-12'

    assert type(urls[1]) is gbc.URL
    assert urls[1].id == 234
    assert urls[1].url == 'www.testr.co.uk'
    assert type(urls[1].status) is list
    assert len(urls[1].status) == 3
    assert urls[1].is_online() is True
    assert urls[1].latest_connection_status().status == '200'
    assert urls[1].latest_connection_status().date == '2025-07-12'

# Test cases for fetching **Version** from the GBC database
def test_fetch_version_by_name():
    version = gbc.fetch_version({'name':'v1.1.2'}, conn=db_conn)

    assert type(version) is gbc.Version
    assert version.id == 2
    assert version.name == 'v1.1.2'
    assert version.user == 'carlac'
    assert version.date == '2025-10-17'

# Test cases for fetching **Publication** from the GBC database
def test_fetch_publication_by_id():
    publication = gbc.fetch_publication({'id': 321}, expanded=False, conn=db_conn)

    assert type(publication) is gbc.Publication
    assert publication.id == 321
    assert publication.title == 'Publication about test resource'
    assert publication.authors == 'A. Guy, C. Lady'
    assert publication.publication_date == '2025-01-10'
    assert publication.affiliation == 'One place; Another place'
    assert publication.affiliation_countries == 'Here; There'
    assert publication.citation_count == 123

    assert publication.grants is None  # not expanded

def test_fetch_publication_expanded_by_id():
    publication = gbc.fetch_publication({'id': 321}, expanded=True, conn=db_conn)

    assert type(publication) is gbc.Publication
    assert publication.id == 321
    assert publication.title == 'Publication about test resource'
    assert publication.authors == 'A. Guy, C. Lady'
    assert publication.publication_date == '2025-01-10'
    assert publication.affiliation == 'One place; Another place'
    assert publication.affiliation_countries == 'Here; There'
    assert publication.citation_count == 123

    assert type(publication.grants) is list
    assert len(publication.grants) == 1
    assert type(publication.grants[0]) is gbc.Grant
    assert publication.grants[0].id == 234
    assert publication.grants[0].ext_grant_id == 'DEF-234-Y'
    assert type(publication.grants[0].grant_agency) is gbc.GrantAgency
    assert publication.grants[0].grant_agency.id == 567
    assert publication.grants[0].grant_agency.name == 'Funder no. 2'
    assert publication.grants[0].grant_agency.country == 'There'

def test_fetch_publication_by_pubmed_id():
    publication = gbc.fetch_publication({'pubmed_id': 432234}, expanded=True, conn=db_conn)

    assert type(publication) is gbc.Publication
    assert publication.id == 432
    assert publication.title == 'Another publication about stuff'
    assert publication.authors == 'R. Bee'
    assert publication.publication_date == '2022-07-01'
    assert publication.affiliation == 'Heartsville'
    assert publication.affiliation_countries == 'Everywhere'
    assert publication.citation_count == 3

    assert type(publication.grants) is list
    assert len(publication.grants) == 1
    assert type(publication.grants[0]) is gbc.Grant
    assert publication.grants[0].id == 123
    assert publication.grants[0].ext_grant_id == 'ABC-123-Z'
    assert type(publication.grants[0].grant_agency) is gbc.GrantAgency
    assert publication.grants[0].grant_agency.id == 456
    assert publication.grants[0].grant_agency.name == 'Funder no. 1'
    assert publication.grants[0].grant_agency.country == 'Here'

def test_fetch_pub_by_id_from_publication_obj():
    # test here can be a bit more minimal as fetch by id is already tested above
    publication = gbc.Publication.fetch_by_id(321, conn=db_conn)

    assert type(publication) is gbc.Publication
    assert publication.id == 321
    assert publication.title == 'Publication about test resource'
    assert publication.authors == 'A. Guy, C. Lady'

def test_fetch_pub_by_pubmed_id_from_publication_obj():
    # test here can be a bit more minimal as fetch by pubmed id is already tested above
    publication = gbc.Publication.fetch_by_pubmed_id(432234, conn=db_conn)

    assert type(publication) is gbc.Publication
    assert publication.id == 432
    assert publication.title == 'Another publication about stuff'
    assert publication.authors == 'R. Bee'

def test_fetch_pub_by_pmc_id_from_publication_obj():
    # test here can be a bit more minimal as fetch by pmc id is already tested above
    publication = gbc.Publication.fetch_by_pmc_id('PMC321123', conn=db_conn)

    assert type(publication) is gbc.Publication
    assert publication.id == 321
    assert publication.title == 'Publication about test resource'
    assert publication.authors == 'A. Guy, C. Lady'

def test_fetch_all_publications():
    publications = gbc.fetch_all_publications(expanded=False, conn=db_conn)

    assert type(publications) is list
    assert len(publications) == 4

    assert type(publications[0]) is gbc.Publication
    assert publications[0].id == 321
    assert publications[0].title == 'Publication about test resource'
    assert publications[0].authors == 'A. Guy, C. Lady'
    assert publications[0].publication_date == '2025-01-10'
    assert publications[0].affiliation == 'One place; Another place'
    assert publications[0].affiliation_countries == 'Here; There'
    assert publications[0].citation_count == 123
    assert publications[0].grants is None  # not expanded

    assert type(publications[1]) is gbc.Publication
    assert publications[1].id == 432
    assert publications[1].title == 'Another publication about stuff'
    assert publications[1].authors == 'R. Bee'
    assert publications[1].publication_date == '2022-07-01'
    assert publications[1].affiliation == 'Heartsville'
    assert publications[1].affiliation_countries == 'Everywhere'
    assert publications[1].citation_count == 3
    assert publications[1].grants is None  # not expanded

    assert type(publications[2]) is gbc.Publication
    assert publications[2].id == 789
    assert publications[2].title == 'I mention accessions'
    assert publications[2].authors == 'Thing 1, Thing 2'
    assert publications[2].publication_date == '2025-01-10'
    assert publications[2].affiliation == 'Whosville'
    assert publications[2].affiliation_countries == 'Placeyland'

    assert type(publications[3]) is gbc.Publication
    assert publications[3].id == 890
    assert publications[3].title == 'I have resource mentions'
    assert publications[3].pubmed_id == 890098
    assert publications[3].pmc_id == 'PMC890098'


def test_fetch_publication_by_pmc_id():
    publication = gbc.fetch_publication({'pmc_id': 'PMC321123'}, expanded=True, conn=db_conn)

    assert type(publication) is gbc.Publication
    assert publication.id == 321
    assert publication.title == 'Publication about test resource'
    assert publication.authors == 'A. Guy, C. Lady'
    assert publication.publication_date == '2025-01-10'
    assert publication.affiliation == 'One place; Another place'
    assert publication.affiliation_countries == 'Here; There'
    assert publication.citation_count == 123

    assert type(publication.grants) is list
    assert len(publication.grants) == 1
    assert type(publication.grants[0]) is gbc.Grant
    assert publication.grants[0].id == 234
    assert publication.grants[0].ext_grant_id == 'DEF-234-Y'
    assert type(publication.grants[0].grant_agency) is gbc.GrantAgency
    assert publication.grants[0].grant_agency.id == 567
    assert publication.grants[0].grant_agency.name == 'Funder no. 2'
    assert publication.grants[0].grant_agency.country == 'There'


# Test cases for fetching **Grants & Grant Agencies** from the GBC database
def test_fetch_grant_by_id_list():
    grant = gbc.fetch_grant({'id':[123, 234]}, conn=db_conn)

    assert type(grant) is list
    assert len(grant) == 2

    assert type(grant[0]) is gbc.Grant
    assert grant[0].id == 123
    assert grant[0].ext_grant_id == 'ABC-123-Z'

    assert type(grant[0].grant_agency) is gbc.GrantAgency
    assert grant[0].grant_agency.id == 456
    assert grant[0].grant_agency.name == 'Funder no. 1'
    assert grant[0].grant_agency.country == 'Here'

    assert type(grant[1]) is gbc.Grant
    assert grant[1].id == 234
    assert grant[1].ext_grant_id == 'DEF-234-Y'

    assert type(grant[1].grant_agency) is gbc.GrantAgency
    assert grant[1].grant_agency.id == 567
    assert grant[1].grant_agency.name == 'Funder no. 2'
    assert grant[1].grant_agency.country == 'There'

def test_fetch_grant_by_ext_id_from_grant_obj():
    # test here can be a bit more minimal as fetch is already tested above
    grant = gbc.Grant.fetch_by_ext_id('ABC-123-Z', conn=db_conn)

    assert type(grant) is gbc.Grant
    assert grant.id == 123
    assert grant.ext_grant_id == 'ABC-123-Z'

    assert type(grant.grant_agency) is gbc.GrantAgency
    assert grant.grant_agency.id == 456
    assert grant.grant_agency.name == 'Funder no. 1'
    assert grant.grant_agency.country == 'Here'

def test_fetch_grant_agency_by_name():
    grant_agency = gbc.fetch_grant_agency({'name':'Funder no. 1'}, conn=db_conn)

    assert type(grant_agency) is gbc.GrantAgency
    assert grant_agency.id == 456
    assert grant_agency.name == 'Funder no. 1'
    assert grant_agency.country == 'Here'


def test_fetch_all_grant_agencies():
    grant_agencies = gbc.fetch_all_grant_agencies(conn=db_conn)

    assert type(grant_agencies) is list
    assert len(grant_agencies) == 3

    assert type(grant_agencies[0]) is gbc.GrantAgency
    assert grant_agencies[0].id == 123
    assert grant_agencies[0].name == 'Extra name'

    assert type(grant_agencies[1]) is gbc.GrantAgency
    assert grant_agencies[1].id == 456
    assert grant_agencies[1].name == 'Funder no. 1'
    assert grant_agencies[1].country == 'Here'

    assert type(grant_agencies[2]) is gbc.GrantAgency
    assert grant_agencies[2].id == 567
    assert grant_agencies[2].name == 'Funder no. 2'
    assert grant_agencies[2].country == 'There'

