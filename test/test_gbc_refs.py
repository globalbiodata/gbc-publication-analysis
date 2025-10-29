import globalbiodata as gbc
import sqlalchemy as db

db_engine = db.create_engine('sqlite:///./test/test_data/gbc_pytest_db.sqlite')
db_conn = db_engine.connect()
metadata = db.MetaData()

def test_fetch_accession_by_accession():
    accession_result_l = gbc.fetch_accession({'accession': 'acc1.123'}, conn=db_conn)

    assert type(accession_result_l) is list
    assert len(accession_result_l) == 1

    accession_result = accession_result_l[0]
    assert type(accession_result) is gbc.Accession
    assert accession_result.accession == 'acc1.123'

    assert type(accession_result.resource) is gbc.Resource
    assert accession_result.resource.id == 123
    assert accession_result.resource.short_name == 'test_resource'
    assert accession_result.resource.publications is None  # not expanded

    assert len(accession_result.publications) == 2
    assert accession_result.publications[0].id == 432
    assert accession_result.publications[0].grants is None # not expanded
    assert accession_result.publications[1].id == 789
    assert accession_result.publications[1].grants is None # not expanded

def test_fetch_accession_by_resource_id():
    accession_result = gbc.fetch_accession({'resource_id': 123}, conn=db_conn)

    assert type(accession_result) is list
    assert len(accession_result) == 2

    acc1 = accession_result[0]
    assert type(acc1) is gbc.Accession
    assert acc1.accession == 'acc1.123'
    assert acc1.resource.id == 123
    assert acc1.resource.short_name == 'test_resource'
    assert len(acc1.publications) == 2

    acc2 = accession_result[1]
    assert type(acc2) is gbc.Accession
    assert acc2.accession == 'acc2.123'
    assert acc2.resource.id == 123
    assert len(acc2.publications) == 1

def test_resource_accessions():
    resource = gbc.fetch_resource({'id': 123}, conn=db_conn)
    accessions = resource.accessions()

    assert type(accessions) is list
    assert len(accessions) == 2

    acc1 = accessions[0]
    assert type(acc1) is gbc.Accession
    assert acc1.accession == 'acc1.123'

    acc2 = accessions[1]
    assert type(acc2) is gbc.Accession
    assert acc2.accession == 'acc2.123'

def test_publication_accessions():
    publication = gbc.fetch_publication({'id': 789}, conn=db_conn)
    accessions = publication.accessions()

    assert type(accessions) is list
    assert len(accessions) == 3

    assert type(accessions[0]) is gbc.Accession
    assert accessions[0].accession == 'acc1.123'
    assert accessions[0].resource.id == 123

    assert type(accessions[1]) is gbc.Accession
    assert accessions[1].accession == 'acc2.123'
    assert accessions[1].resource.id == 123

    assert type(accessions[2]) is gbc.Accession
    assert accessions[2].accession == 'idA.234'
    assert accessions[2].resource.id == 234

def test_fetch_mentions_by_resource():
    mentions = gbc.fetch_resource_mention({'resource_id': 123}, conn=db_conn)

    assert type(mentions) is list
    assert len(mentions) == 1

    mention = mentions[0]
    assert type(mention) is gbc.ResourceMention
    assert type(mention.publication) is gbc.Publication
    assert type(mention.resource) is gbc.Resource
    assert type(mention.version) is gbc.Version

    assert mention.publication.id == 890
    assert mention.resource.id == 123
    assert mention.version.id == 2
    assert mention.match_count == 3
    assert mention.mean_confidence == 0.95

    assert type(mention.matched_aliases) is list
    assert len(mention.matched_aliases) == 2
    assert type(mention.matched_aliases[0]) is gbc.MatchedAlias
    assert mention.matched_aliases[0].matched_alias == 'R123'
    assert mention.matched_aliases[0].match_count == 2
    assert mention.matched_aliases[0].mean_confidence == 0.9

    assert type(mention.matched_aliases[1]) is gbc.MatchedAlias
    assert mention.matched_aliases[1].matched_alias == 'test_resource'
    assert mention.matched_aliases[1].match_count == 1
    assert mention.matched_aliases[1].mean_confidence == 1.0

def test_fetch_mentions_by_publication():
    mentions = gbc.fetch_resource_mention({'publication_id': 890}, conn=db_conn)

    assert type(mentions) is list
    assert len(mentions) == 1

    mention = mentions[0]
    assert type(mention) is gbc.ResourceMention
    assert type(mention.publication) is gbc.Publication
    assert type(mention.resource) is gbc.Resource
    assert type(mention.version) is gbc.Version

    assert mention.publication.id == 890
    assert mention.resource.id == 123
    assert mention.version.id == 2
    assert mention.match_count == 3
    assert mention.mean_confidence == 0.95

    assert type(mention.matched_aliases) is list
    assert len(mention.matched_aliases) == 2
    assert type(mention.matched_aliases[0]) is gbc.MatchedAlias
    assert mention.matched_aliases[0].matched_alias == 'R123'
    assert mention.matched_aliases[0].match_count == 2
    assert mention.matched_aliases[0].mean_confidence == 0.9

    assert type(mention.matched_aliases[1]) is gbc.MatchedAlias
    assert mention.matched_aliases[1].matched_alias == 'test_resource'
    assert mention.matched_aliases[1].match_count == 1
    assert mention.matched_aliases[1].mean_confidence == 1.0

def test_fetch_mentions_no_results():
    mentions = gbc.fetch_resource_mention({'publication_id': 9999}, conn=db_conn)

    assert mentions is None

def test_fetch_mentions_by_alias():
    mentions = gbc.fetch_resource_mention({'matched_alias': 'R123'}, conn=db_conn)

    assert type(mentions) is list
    assert len(mentions) == 1

    mention = mentions[0]
    assert type(mention) is gbc.ResourceMention
    assert mention.publication.id == 890
    assert mention.resource.id == 123
    assert mention.version.id == 2
    assert mention.match_count == 2
    assert mention.mean_confidence == 0.9

    assert type(mention.matched_aliases) is list
    assert len(mention.matched_aliases) == 1
    assert type(mention.matched_aliases[0]) is gbc.MatchedAlias
    assert mention.matched_aliases[0].matched_alias == 'R123'
    assert mention.matched_aliases[0].match_count == 2
    assert mention.matched_aliases[0].mean_confidence == 0.9


def test_fetch_mentions_from_resource():
    resource = gbc.fetch_resource({'id': 123}, conn=db_conn)
    mentions = resource.mentions()

    assert type(mentions) is list
    assert len(mentions) == 1

    mention = mentions[0]
    assert type(mention) is gbc.ResourceMention
    assert mention.publication.id == 890
    assert mention.resource.id == 123
    assert mention.version.id == 2
    assert mention.match_count == 3
    assert mention.mean_confidence == 0.95

    assert type(mention.matched_aliases) is list
    assert len(mention.matched_aliases) == 2
    assert type(mention.matched_aliases[0]) is gbc.MatchedAlias
    assert mention.matched_aliases[0].matched_alias == 'R123'
    assert mention.matched_aliases[0].match_count == 2
    assert mention.matched_aliases[0].mean_confidence == 0.9

    assert type(mention.matched_aliases[1]) is gbc.MatchedAlias
    assert mention.matched_aliases[1].matched_alias == 'test_resource'
    assert mention.matched_aliases[1].match_count == 1
    assert mention.matched_aliases[1].mean_confidence == 1.0

def test_resource_referenced_by():
    resource = gbc.fetch_resource({'id': 123}, conn=db_conn)
    publications = resource.referenced_by()

    assert type(publications) is list
    assert len(publications) == 3

    assert type(publications[0]) is gbc.Publication
    assert publications[0].id == 432

    assert type(publications[1]) is gbc.Publication
    assert publications[1].id == 789

    assert type(publications[2]) is gbc.Publication
    assert publications[2].id == 890

def test_publication_references():
    publication = gbc.fetch_publication({'id': 789}, conn=db_conn)
    resources = publication.references_resources()

    assert type(resources) is list
    assert len(resources) == 2

    assert type(resources[0]) is gbc.Resource
    assert resources[0].id == 123

    assert type(resources[1]) is gbc.Resource
    assert resources[1].id == 234