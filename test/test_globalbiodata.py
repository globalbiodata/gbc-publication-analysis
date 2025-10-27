import globalbiodata as gbc
import json
# import pytest

#-------------------------------------#
# Test GBC classes                    #
#-------------------------------------#


# Tests for URL and ConnectionStatus classes
def test_URL_no_status():
    given = {
        'url': 'www.test.org',
        'url_country': 'Testland',
        'url_coordinates': (12.34, 56.78),
        'wayback_url': None
    }

    expected = {
        'url': 'www.test.org',
        'url_country': 'Testland',
        'url_coordinates': (12.34, 56.78),
        'wayback_url': None
    }

    result = gbc.URL(given)
    assert result.id is None
    assert result.url == expected['url']
    assert result.url_country == expected['url_country']
    assert result.url_coordinates == expected['url_coordinates']
    assert result.wayback_url == expected['wayback_url']
    assert result.status == []

def test_URL_with_str_status():
    given = {
        'id': 123,
        'url': 'www.test.org',
        'url_status': '200',
        'connection_date': '2024-07-12'
    }

    result = gbc.URL(given)
    print(result.status)
    assert result.id == 123
    assert result.url == 'www.test.org'
    assert type(result.status) is list
    assert len(result.status) == 1

    this_status = result.status[0]
    assert type(this_status) is gbc.ConnectionStatus
    assert this_status.status == '200'
    assert this_status.date == '2024-07-12'
    assert this_status.url_id == 123

def test_URL_with_obj_status():
    given = {
        'id': 123,
        'url': 'www.test.org',
        'status': gbc.ConnectionStatus({'url_id':123, 'status':'200', 'date':'2024-07-12'})
    }

    result = gbc.URL(given)
    print(result.status)
    assert result.id == 123
    assert result.url == 'www.test.org'
    assert type(result.status) is list
    assert len(result.status) == 1

    this_status = result.status[0]
    assert type(this_status) is gbc.ConnectionStatus
    assert this_status.status == '200'
    assert this_status.date == '2024-07-12'
    assert this_status.url_id == 123

def test_URL_with_dict_status():
    given = {
        'id': 123,
        'url': 'www.test.org',
        'status': [
            {'url_id':123, 'status':'200', 'date':'2024-07-12'},
            {'url_id':123, 'status':'404', 'date':'2024-07-11'}
        ]
    }

    result = gbc.URL(given)
    print(result.status)
    assert result.id == 123
    assert result.url == 'www.test.org'
    assert type(result.status) is list
    assert len(result.status) == 2

    status1 = result.status[0]
    assert type(status1) is gbc.ConnectionStatus
    assert status1.status == '200'
    assert status1.date == '2024-07-12'

    status2 = result.status[1]
    assert type(status2) is gbc.ConnectionStatus
    assert status2.status == '404'
    assert status2.date == '2024-07-11'
    assert status2.url_id == 123

# Tests for Version class
def test_Version():
    given = {
        'version_name': 'test version',
        'date': '2024-07-12',
        'version_user': 'tester',
        'additional_metadata': {'key1': 'value1', 'key2': 2}
    }

    result = gbc.Version(given)
    assert result.name == 'test version'
    assert result.date == '2024-07-12'
    assert result.user == 'tester'
    assert result.additional_metadata == {'key1': 'value1', 'key2': 2}

# Tests for Resource class
def test_Resource_minimal():
    given = {
        'short_name': 'TestResource',
        'common_name': 'Test Resource',
        'prediction_metadata': 'Some metadata',
        'is_gcbr': True,
        'is_latest': False
    }

    result = gbc.Resource(given)
    assert result.short_name == 'TestResource'
    assert result.common_name == 'Test Resource'
    assert result.prediction_metadata == 'Some metadata'
    assert result.is_gcbr
    assert not result.is_latest

def test_Resource_full():
    given = {
        'short_name': 'TestResource',
        'common_name': 'Test Resource',
        'prediction_metadata': 'Some metadata',
        'is_gcbr': True,
        'is_latest': False,
        'url': 'www.test.org',
        'url_country': 'Testland',
        'url_coordinates': (12.34, 56.78),
        'wayback_url': 'www.wayback.org/test',
        'status': [
            {'status':'200', 'date':'2024-07-12'}
        ],
        'version_name': 'test version',
        'version_date': '2024-07-12',
        'version_user': 'tester',
        'ext_grant_ids': 'G12345, G67890',
        'grant_agencies': 'Test Agency, Another Agency',
        'title': 'Test Title',
        'authors': 'Doe, J.; Smith, A.',
        'pubmed_id': '987654321',
    }

    result = gbc.Resource(given)
    assert result.short_name == 'TestResource'
    assert result.common_name == 'Test Resource'
    assert result.prediction_metadata == 'Some metadata'
    assert result.is_gcbr
    assert not result.is_latest

    assert type(result.url) is gbc.URL
    assert result.url.url == 'www.test.org'
    assert result.url.url_country == 'Testland'
    assert result.url.url_coordinates == (12.34, 56.78)
    assert result.url.wayback_url == 'www.wayback.org/test'
    assert type(result.url.status) is list
    assert len(result.url.status) == 1
    assert type(result.url.status[0]) is gbc.ConnectionStatus
    assert result.url.status[0].status == '200'
    assert result.url.status[0].date == '2024-07-12'

    assert type(result.version) is gbc.Version
    assert result.version.name == 'test version'
    assert result.version.date == '2024-07-12'
    assert result.version.user == 'tester'

    assert type(result.grants) is list
    assert len(result.grants) == 2
    print(result.grants[0].__dict__)
    assert result.grants[0].ext_grant_id == 'G12345'
    assert type(result.grants[0].grant_agency) is gbc.GrantAgency
    assert result.grants[0].grant_agency.name == 'Test Agency'
    assert result.grants[1].ext_grant_id == 'G67890'
    assert type(result.grants[1].grant_agency) is gbc.GrantAgency
    assert result.grants[1].grant_agency.name == 'Another Agency'

    assert type(result.publications) is list
    assert len(result.publications) == 1
    assert result.publications[0].title == 'Test Title'
    assert result.publications[0].authors == 'Doe, J.; Smith, A.'
    assert result.publications[0].pubmed_id == '987654321'

# Tests for Publication class
def test_Publication():
    given = {
        'title': 'Test Title',
        'authors': 'Doe, J.; Smith, A.',
        'pubmed_id': '987654321',
        'pmc_id': 'PMC123456',
        'affiliation': 'Test University; Another Institute',
        'affiliation_countries': 'Testland; Anotherland',
        'ext_grant_ids': 'G12345, G67890',
        'grant_agencies': 'Test Agency, Another Agency',
    }

    result = gbc.Publication(given)
    assert result.title == 'Test Title'
    assert result.authors == 'Doe, J.; Smith, A.'
    assert result.pubmed_id == '987654321'
    assert result.pmc_id == 'PMC123456'
    assert result.affiliation == 'Test University; Another Institute'
    assert result.affiliation_countries == 'Testland; Anotherland'

    assert type(result.grants) is list
    assert len(result.grants) == 2
    print(result.grants[0].__dict__)
    assert result.grants[0].ext_grant_id == 'G12345'
    assert type(result.grants[0].grant_agency) is gbc.GrantAgency
    assert result.grants[0].grant_agency.name == 'Test Agency'
    assert result.grants[1].ext_grant_id == 'G67890'
    assert type(result.grants[1].grant_agency) is gbc.GrantAgency
    assert result.grants[1].grant_agency.name == 'Another Agency'


# Tests for Accession class
def test_Accession():
    given = {
        'accession': 'ABC123',
        'resource_short_name': 'TestResource',
        'resource_url': 'www.test.org',
        'resource_is_gcbr': True,
        'publication_title': 'Test Title',
        'publication_authors': 'Doe, J.; Smith, A.',
        'publication_pubmed_id': '987654321',
        'version_name': 'test version',
        'version_date': '2024-07-12',
        'version_user': 'tester',
        'additional_metadata': {'key1': 'value1', 'key2': 2},
        'version_additional_metadata': {'key3': 'value3', 'key4': 4}
    }

    result = gbc.Accession(given)
    assert result.accession == 'ABC123'
    assert type(result.resource) is gbc.Resource
    assert result.resource.short_name == 'TestResource'
    assert result.resource.url.url == 'www.test.org'
    assert result.resource.is_gcbr
    assert result.additional_metadata == {'key1': 'value1', 'key2': 2}

    assert type(result.publications) is list
    assert len(result.publications) == 1
    assert result.publications[0].title == 'Test Title'
    assert result.publications[0].authors == 'Doe, J.; Smith, A.'
    assert result.publications[0].pubmed_id == '987654321'

    assert type(result.version) is gbc.Version
    print(result.version.__dict__)
    assert result.version.name == 'test version'
    assert result.version.date == '2024-07-12'
    assert result.version.user == 'tester'
    assert result.version.additional_metadata == {'key3': 'value3', 'key4': 4}

# Tests for ResourceMention class
def test_ResourceMention_str():
    given = {
        'resource_short_name': 'TestResource',
        'resource_url': 'www.test.org',
        'resource_is_gcbr': True,
        'publication_title': 'Test Title',
        'publication_authors': 'Doe, J.; Smith, A.',
        'publication_pubmed_id': '987654321',
        'version_name': 'test version',
        'version_date': '2024-07-12',
        'version_user': 'tester',
        'version_additional_metadata': {'key3': 'value3', 'key4': 4},
        'matched_alias': 'TestResource alias',
        'match_count': 5,
        'mean_confidence': 0.95,
    }

    result = gbc.ResourceMention(given)
    assert result.matched_alias == 'TestResource alias'
    assert result.match_count == 5
    assert result.mean_confidence == 0.95

    assert type(result.resource) is gbc.Resource
    assert result.resource.short_name == 'TestResource'
    assert result.resource.url.url == 'www.test.org'
    assert result.resource.is_gcbr

    assert type(result.publication) is gbc.Publication
    assert result.publication.title == 'Test Title'
    assert result.publication.authors == 'Doe, J.; Smith, A.'
    assert result.publication.pubmed_id == '987654321'

    assert type(result.version) is gbc.Version
    print(result.version.__dict__)
    assert result.version.name == 'test version'
    assert result.version.date == '2024-07-12'
    assert result.version.user == 'tester'
    assert result.version.additional_metadata == {'key3': 'value3', 'key4': 4}

def test_ResourceMention_obj():
    given_resource = {
        'short_name': 'TestResource',
        'common_name': 'Test Resource',
        'prediction_metadata': 'Some metadata',
        'is_gcbr': True,
        'is_latest': False
    }
    given_publication = {
        'title': 'Test Title',
        'authors': 'Doe, J.; Smith, A.',
        'pubmed_id': '987654321',
    }
    given_version = {
        'version_name': 'test version',
        'date': '2024-07-12',
        'version_user': 'tester',
        'additional_metadata': {'key1': 'value1', 'key2': 2}
    }
    given = {
        'resource': gbc.Resource(given_resource),
        'publication': gbc.Publication(given_publication),
        'version': gbc.Version(given_version),
        'matched_alias': 'TestResource alias',
        'match_count': 5,
        'mean_confidence': 0.95,
    }

    result = gbc.ResourceMention(given)
    assert result.matched_alias == 'TestResource alias'
    assert result.match_count == 5
    assert result.mean_confidence == 0.95

    assert type(result.resource) is gbc.Resource
    assert result.resource.short_name == 'TestResource'
    assert result.resource.common_name == 'Test Resource'
    assert result.resource.is_gcbr

    assert type(result.publication) is gbc.Publication
    assert result.publication.title == 'Test Title'
    assert result.publication.authors == 'Doe, J.; Smith, A.'
    assert result.publication.pubmed_id == '987654321'

    assert type(result.version) is gbc.Version
    print(result.version.__dict__)
    assert result.version.name == 'test version'
    assert result.version.date == '2024-07-12'
    assert result.version.user == 'tester'
    assert result.version.additional_metadata == {'key1': 'value1', 'key2': 2}

#-------------------------------------#
# End of class tests                  #
#-------------------------------------#


#-------------------------------------#
# Test GBC helper functions           #
#-------------------------------------#

def test_extract_fields_by_type():
    given = {'resource_short_name': 'test', 'resource_url': 'www.test.com', 'other_field': 1}
    expected = {'short_name': 'test', 'url': 'www.test.com'}

    result = gbc.extract_fields_by_type(given, 'resource')
    assert result == expected



# Load a sample EuropePMC result for testing
epmc_result = json.load(open('test/test_data/epmc_result.json', 'r'))

# Mock locationtagger place class for testing
class FakePlace:
    def __init__(self, countries=None, regions=None, cities=None):
        self.countries = countries or []
        self.country_regions = regions or {}
        self.country_cities = cities or {}

def test_new_publication_from_EuropePMC_result(monkeypatch):
    # Patch the location tagger to return a fixed fake place,
    # without actually calling the external service
    def fake_find_locations(text):
        if "Dublin, Ireland" in text:
            return FakePlace(countries=["Ireland"])
        elif "Denver, United States" in text:
            return FakePlace(countries=["United States"])
        else:
            return FakePlace()

    monkeypatch.setattr(gbc.locationtagger, "find_locations", fake_find_locations)

    result = gbc.new_publication_from_EuropePMC_result(epmc_result)
    assert type(result) is gbc.Publication
    assert result.pubmed_id == '54321'
    assert result.pmc_id == 'PMC12345'
    assert result.title == "Very interesting article."
    assert result.authors == "Hide I, Padgett WL, Jacobson KA, Daly JW."
    assert result.affiliation == "Center for Craic, Dublin, Ireland; Department of Pharmacology, Denver, United States"
    assert result.affiliation_countries == "Ireland; United States"
    assert result.publication_date == "2022-02-01"
    assert result.citation_count == 78
    assert result.keywords == "'Membranes'; 'enzymology'; 'Animals'; 'Phenethylamines'; 'pharmacology'"

    assert type(result.grants) is list
    assert len(result.grants) == 2
    assert result.grants[0].ext_grant_id == "Z01 YTHO"
    assert type(result.grants[0].grant_agency) is gbc.GrantAgency
    assert result.grants[0].grant_agency.name == "NIH"
    assert result.grants[1].ext_grant_id == "Z99 MKAY"
    assert type(result.grants[1].grant_agency) is gbc.GrantAgency
    assert result.grants[1].grant_agency.name == "EU"

def test_clean_affiliations():
    # Test that affiliations are cleaned of extra spaces and semicolons
    given_1 = "  University of Test  ; Trento;; Italy test@email.com "
    expected_1 = "University of Test, Trento, Italy"

    result_1 = gbc._clean_affiliation(given_1)
    assert result_1 == expected_1

    # Test postcode removal and country abbreviation expansion
    given_2 = "Dept. of Biology, University of Test, Test City, UK, AB12 3CD"
    expected_2 = "Dept. of Biology, University of Test, Test City, United Kingdom"

    result_2 = gbc._clean_affiliation(given_2)
    assert result_2 == expected_2

def test_find_country(monkeypatch):
    # Patch the location tagger to return a fixed fake place,
    # without actually calling the external service
    def fake_locationtagger_find_locations(text):
        if "Some Dept., Dublin2" in text:
            return FakePlace(regions={"Ireland": ["Leinster"]})
        elif "Some Dept., Portland" in text:
            return FakePlace(regions={"United States": ["Oregon"], "Canada": ["Ontario"]})
        elif "Some Dept., Denver" in text:
            return FakePlace(cities={"United States": ["Denver"]})
        elif "Some Dept., Cambridge" in text:
            return FakePlace(cities={"United Kingdom": ["Cambridge"], "United States": ["Cambridge"]})
        elif "University of Oregon" in text:
            return FakePlace(countries=["United States"])
        elif "University of Cambridge" in text:
            return FakePlace(countries=["United Kingdom"])

    class FakeClient: # for googlemaps.Client monkeypatch
        def __init__(self, key=None):
            pass
        def find_place(self, address, input_type, fields=None):
            if "Portland" in address:
                return {"candidates": [{"formatted_address": "University of Oregon, Portland, United States"}]}
            if "Cambridge" in address:
                return {"candidates": [{"formatted_address": "University of Cambridge, United Kingdom"}]}
            return {"candidates": []}

    monkeypatch.setattr(gbc.locationtagger, "find_locations", fake_locationtagger_find_locations)
    monkeypatch.setattr(gbc.googlemaps, "Client", lambda key=None: FakeClient())

    # Test region-based disambiguation
    given_1 = "Some Dept., Dublin2, Ireland"
    expected_1 = (["Ireland"], 'locationtagger')  # Should resolve to Ireland based on region
    result_1 = gbc._find_country(given_1, google_maps_api_key='fake_key')
    assert result_1 == expected_1

    # Test Google Maps region fallback
    given_2 = "Some Dept., Portland, USA"
    expected_2 = (["United States"], 'GoogleMaps')  # Should resolve to United States based on Google Maps
    result_2 = gbc._find_country(given_2, google_maps_api_key='fake_key')
    assert result_2 == expected_2

    # Test city-based disambiguation
    given_3 = "Some Dept., Denver"
    expected_3 = (["United States"], 'locationtagger')  # Should resolve to United States based on city
    result_3 = gbc._find_country(given_3, google_maps_api_key='fake_key')
    assert result_3 == expected_3

    # Test Google Maps city fallback
    given_4 = "Some Dept., Cambridge"
    expected_4 = (["United Kingdom"], 'GoogleMaps')  # Should resolve to United Kingdom based on Google Maps
    result_4 = gbc._find_country(given_4, google_maps_api_key='fake_key')
    assert result_4 == expected_4