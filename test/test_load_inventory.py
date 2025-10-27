from bin.load_inventory import uniq_with_order, explode_record, split_record_data

def test_uniq_with_order():
    input_str = "apple, banana, apple, orange, banana, grape"
    to_remove = ["banana"]
    expected_output = "apple; orange; grape"
    assert uniq_with_order(input_str, to_remove) == expected_output

def test_explode_record():
    record = {
        'pubmed_id': '1234,5678,0123',
        'title': 'TestDB: a test database',
        'short_name': 'TestDB',
        'url': 'http://testdb.com'
    }

    expected_records = [{
        'pubmed_id': '1234',
        'title': 'TestDB: a test database',
        'short_name': 'TestDB',
        'url': 'http://testdb.com'
        }, {
        'pubmed_id': '5678',
        'title': 'TestDB: a test database',
        'short_name': 'TestDB',
        'url': 'http://testdb.com'
        }, {
        'pubmed_id': '0123',
        'title': 'TestDB: a test database',
        'short_name': 'TestDB',
        'url': 'http://testdb.com'
        }
    ]

    result = explode_record(record)

    assert result == expected_records

def test_split_record_data_A():
    record_A = {
        'pubmed_id': '12345678',
        'title': 'TestDB: a test database',
        'authors': 'Doe J, Smith A, Doe J, Smith B',
        'affiliation_countries': 'Testland, Examplestan, Testland, Testland',
        'short_name': 'TestDB',
        'short_name_prob': '0.95',
        'common_name': 'TestDB Common',
        'common_name_prob': '0.90',
        'full_name': 'Test Database Full',
        'full_name_prob': '0.85',
        'url': 'http://testdb.com',
        'url_status': '200',
    }

    expected_A = {
        'pubmed_id': '12345678',
        'title': 'TestDB: a test database',
        'authors': 'Doe J; Smith A; Smith B',
        'affiliation_countries': 'Testland; Examplestan',
        'url': 'http://testdb.com',
        'online': True,
        'url_status': '200',
        'short_name': 'TestDB',
        'common_name': 'TestDB Common',
        'full_name': 'Test Database Full',
        'resource_prediction_metadata': {
            'short_name_prob': '0.95',
            'common_name_prob': '0.90',
            'full_name_prob': '0.85'
        }
    }

    result_A = split_record_data(record_A)

    assert result_A == expected_A

def test_split_record_data_B():
    record_B = {
        'pubmed_id': '87654321',
        'title': 'Another TestDB: a test database',
        'authors': 'Doe J, Smith A, Doe J, Smith B',
        'affiliation_countries': 'Testland, Examplestan, Testland, Testland',
        'short_name': 'AnTestDB',
        'short_name_prob': '0.95',
        'common_name': 'AnTestDB Common',
        'common_name_prob': '0.90',
        'full_name': 'AnTest Database Full',
        'full_name_prob': '0.85',
        'url': 'http://antestdb.com',
        'url_status': "HTTPConnectionPool(host='147.8.74.24', port=80): Max retries exceeded with url: /16SpathDB (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7f6f246776d0>, 'Connection to 147.8.74.24 timed out. (connect timeout=5)'))",
    }

    expected_B = {
        'pubmed_id': '87654321',
        'title': 'Another TestDB: a test database',
        'authors': 'Doe J; Smith A; Smith B',
        'affiliation_countries': 'Testland; Examplestan',
        'url': 'http://antestdb.com',
        'online': False,
        'url_status': "HTTPConnectionPool(host='147.8.74.24', port=80): Max retries exceeded with url: /16SpathDB (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7f6f246776d0>, 'Connection to 147.8.74.24 timed out. (connect timeout=5)'))",
        'short_name': 'AnTestDB',
        'common_name': 'AnTestDB Common',
        'full_name': 'AnTest Database Full',
        'resource_prediction_metadata': {
            'short_name_prob': '0.95',
            'common_name_prob': '0.90',
            'full_name_prob': '0.85'
        }
    }

    result_B = split_record_data(record_B)

    assert result_B == expected_B