import sys
from datetime import datetime
import sqlalchemy as db
import json

class URL:
    """
    URL and associated information

    `id`: Database ID for URL
    `url`: URL
    `url_country`: Country of URL
    `url_coordinates`: Coordinates of URL
    `status`: ConnectionStatus object(s)
    `wayback_url`: URL for Wayback Machine
    """
    id:int
    url:str
    url_country:str
    url_coordinates:str
    url_status:str
    status:[]
    wayback_url:str

    def __init__(self, u):
        self.id = u.get('id')
        self.url = u.get('url')
        self.url_country = u.get('url_country')
        self.url_coordinates = u.get('url_coordinates')
        self.wayback_url = u.get('wayback_url')
        
        cs = [ConnectionStatus({'url_id':self.id, 'status':u.get('url_status'), 'date':u.get('connection_date')})]
        self.status = cs
    
    def __str__(self):
        url_str = ', '.join([
            f"id={self.id}", f"url={self.url}", f"url_country={self.url_country}",
            f"url_coordinates={self.url_coordinates}", f"wayback_url={self.wayback_url}",
            f"status=[{', '.join([s.__str__() for s in self.status])}]"
        ])
        return f"URL({url_str})"

    def write(self, conn=None, engine=None, debug=False):
        conn_statuses = self.status
        d = self.__dict__
        del d['status']
        new_url_id = insert_into_table('url', d, conn=conn, engine=engine, debug=debug)
        self.id = new_url_id
        for c in conn_statuses:
            c.url_id = self.id
            c.write(conn=conn, engine=engine, debug=debug)
        self.status = conn_statuses
        return self.id

class Prediction:
    """
    Prediction information

    `id` : Database ID for Prediction
    `name` : Name of prediction pipeline/type
    `date` : Date of run
    `user` : User who ran prediction pipeline
    `additional_metadata` : Additional prediction data in JSON format
    """
    id:int
    name:str
    date:str
    user:str
    additional_metadata:{}

    def __init__(self, p):
        self.id = p.get('id')
        self.name = p.get('prediction_name') or p.get('name')
        self.date = p.get('prediction_date') or p.get('date')
        self.user = p.get('prediction_user') or p.get('user')
        self.additional_metadata = p.get('additional_prediction_metadata') or p.get('additional_metadata')

    def __str__(self):
        pred_str = f"Prediction(id={self.id}, name={self.name}, date={self.date}, user={self.user}, additional_metadata={self.additional_metadata})"
        return pred_str
    
    def write(self, conn=None, engine=None, debug=False):
        new_prediction_id = insert_into_table('prediction', self.__dict__, conn=conn, engine=engine, debug=debug)
        self.id = new_prediction_id
        return self.id


class Resource:
    """
    Biodata Resource description

    `id`: Database ID for resource
    `short_name`: Short name of resource
    `common_name`: Common name of resource
    `full_name`: Full name of resource
    `url`: URL object
    `prediction`: Prediction object
    `prediction_metadata`: Additional prediction metadata in JSON format
    `publications`: Publication object(s)
    `grants`: Grant object(s)
    `is_gcbr`: boolean value describing Core Biodata Resource status
    `is_latest`: boolean value describing whether this is the most current version of the resource
    """
    id:int
    short_name:str
    common_name:str
    full_name:str
    url:URL
    prediction:Prediction
    prediction_metadata:str
    publications:[]
    grants:[]
    is_gcbr:bool
    is_latest:bool

    def __init__(self, r):
        if r.get('id'):
            self.id = r.get('id')
            del r['id'] # remove to avoid passing to other objects
        self.short_name = r.get('short_name')
        self.common_name = r.get('common_name')
        self.full_name = r.get('full_name')
        self.url = URL(r)
        self.prediction = r.get('prediction') or Prediction(r)
        self.prediction_metadata = r.get('resource_prediction_metadata') or r.get('prediction_metadata')
        self.publications = r.get('publications') or [Publication(r)]
        self.is_gcbr = r.get('is_gcbr')
        self.is_latest = r.get('is_latest')

        if r.get('grants'):
            self.grants = [Grant(r)] if type(r.get('grants')[0]) == str else r.get('grants')
        elif r.get('ext_grant_ids') and r.get('grant_agencies'):
            self.grants = [Grant({'ext_grant_id':g, 'grant_agency':ga}) for g, ga in zip(r.get('ext_grant_ids').split(','), r.get('grant_agencies').split(','))]
        else:
            self.grants = []
        
        self.id = r.get('id')


    def __str__(self):
        resource_str = ', '.join([
            f"id={self.id}", f"short_name={self.short_name}", f"common_name={self.common_name}", f"full_name={self.full_name}",
            f"is_gcbr={self.is_gcbr}", f"is_latest={self.is_latest}", f"url={self.url.__str__()}", 
            f"prediction={self.prediction.__str__()}", f"prediction_metadata={self.prediction_metadata}",
            f"publications=[{', '.join([p.__str__() for p in self.publications])}]",
            f"grants=[{', '.join(g.__str__() for g in self.grants)}]"
        ])
        return f"Resource({resource_str})"
    
    def write(self, conn=None, engine=None, debug=False):
        url_id = self.url.write(conn=conn, engine=engine, debug=debug)
        self.url.id = url_id

        prediction_id = self.prediction.write(conn=conn, engine=engine, debug=debug)
        self.prediction.id = prediction_id

        # set is_latest to 0 for other versions of this resource
        if self.is_latest:
            lconn = engine.connect()
            lconn.execute(db.text(f"UPDATE resource SET is_latest = 0 WHERE short_name = '{self.short_name}'"))
            lconn.commit()

        resource_cols = {
            'id':self.id, 'short_name':self.short_name, 'common_name':self.common_name, 'full_name':self.full_name, 
            'url_id':self.url.id, 'prediction_id':self.prediction.id, 'prediction_metadata':self.prediction_metadata, 
            'is_gcbr':self.is_gcbr, 'is_latest':self.is_latest
        }
        new_resource_id = insert_into_table('resource', resource_cols, conn=conn, engine=engine, debug=debug)
        self.id = new_resource_id

        for p in self.publications:
            new_pub_id = p.write(conn=conn, engine=engine, debug=debug)
            p.id = new_pub_id
            # create links between resource and publication tables
            insert_into_table('resource_publication', {'resource_id':new_resource_id, 'publication_id':new_pub_id}, conn=conn, engine=engine, debug=debug)

        for g in self.grants:
            new_grant_id = g.write(conn=conn, engine=engine, debug=debug)
            g.id = new_grant_id
            # create links between resource and grant tables
            insert_into_table('resource_grant', {'resource_id':new_resource_id, 'grant_id':new_grant_id}, conn=conn, engine=engine, debug=debug)

        return self.id

class ConnectionStatus:
    """
    
    `url_id`: Database ID for URL
    `status`: Code returned from connection
    `date`: Date of connection
    `is_online`: boolean describing whether return code indicates resource is online
    `is_latest`: boolean value describing whether this is the most recent connection attempt
    """
    url_id:int
    status:str
    date:str
    is_online:bool
    is_latest:bool

    def __init__(self, c):
        self.url_id = c.get('url_id')
        self.status = str(c.get('status'))
        self.date = c.get('connection_date') or c.get('date')
        self.is_latest = c.get('is_latest', 0)

        if not self.date:
            self.date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.is_latest = 1

        if c.get('is_online') == None:
            self.is_online = self.status[:18] not in ['404', '500', 'HTTPConnectionPool']
        else:
            self.is_online = c.get('is_online')
        
    def __str__(self):
        status_str = f"ConnectionStatus(url_id={self.url_id}, status={self.status}, date={self.date}, is_online={self.is_online}, is_latest={self.is_latest})"
        return status_str
    
    def write(self, conn=None, engine=None, debug=False):
        # update is_latest to 0 for other connection statuses
        if self.is_latest:
            lconn = engine.connect()
            lconn.execute(db.text(f"UPDATE connection_status SET is_latest = 0 WHERE url_id = {self.url_id}"))
            lconn.commit()

        insert_into_table('connection_status', self.__dict__, conn=conn, engine=engine, debug=debug)


class Publication:
    """

    `id`: Database ID for Publication
    `title`: Title of publication
    `pubmed_id`: PubMed ID
    `pmc_id`: PubMed Central ID
    `publication_date`: Date of publication
    `authors`: Authors of publication
    `affiliation`: Affiliation of authors
    `affiliation_countries`: Countries of affiliation
    `citation_count`: Number of citations
    `keywords`: Keywords/Mesh terms
    """
    id:int
    title:str
    pubmed_id:int
    pmc_id:str
    publication_date:str
    authors:str
    affiliation:str
    affiliation_countries:str
    citation_count:int
    keywords:str

    def __init__(self, p):
        self.id = p.get('id')
        self.title = p.get('publication_title') or p.get('title')
        self.pubmed_id = p.get('pubmed_id')
        self.pmc_id = p.get('pmc_id')
        self.publication_date = p.get('publication_date')
        self.authors = p.get('authors')
        self.affiliation = p.get('affiliation')
        self.affiliation_countries = p.get('affiliation_countries')
        self.citation_count = p.get('citation_count')
        self.keywords = p.get('keywords')

    def __str__(self):
        pub_str = ', '.join([
            f"id={self.id}", f"title={self.title}", f"pubmed_id={self.pubmed_id}", f"pmc_id={self.pmc_id}",
            f"publication_date={self.publication_date}", f"authors={self.authors}", f"affiliation={self.affiliation}", 
            f"affiliation_countries={self.affiliation_countries}",f"citation_count={self.citation_count}", 
            f"keywords={self.keywords}"
        ])
        return f"Publication({pub_str})"
    
    def write(self, conn=None, engine=None, debug=False):
        new_pub_id = insert_into_table('publication', self.__dict__, conn=conn, engine=engine, debug=debug)
        self.id = new_pub_id
        return self.id

class Grant:
    """
    `id`: Database ID for Grant
    `ext_grant_id`: External grant ID
    `grant_agencies`: GrantAgency object(s)
    """
    id:int
    ext_grant_id:str
    grant_agency:str

    def __init__(self, g):
        self.id = g.get('id')
        self.ext_grant_id = g.get('ext_grant_id')

        if type(g.get('grant_agency')) == str:
            self.grant_agency = GrantAgency({'name': g.get('grant_agency')})
        elif type(g.get('grant_agency')) == GrantAgency:
            self.grant_agency = g.get('grant_agency')
        else:
            raise ValueError(f"Grant Agency must be a string or GrantAgency object. Got: {g.get('grant_agency')} (type:{type(g.get('grant_agency'))}).")

    def __str__(self):
        grant_str = f"Grant(id={self.id}, ext_grant_id={self.ext_grant_id}, grant_agency={self.grant_agency})"
        return grant_str
    
    def write(self, conn=None, engine=None, debug=False):
        new_ga_id = self.grant_agency.write(conn=conn, engine=engine)
        self.grant_agency.id = new_ga_id
        g_cols = {'id':self.id, 'ext_grant_id':self.ext_grant_id, 'grant_agency_id':self.grant_agency.id}
        new_g_id = insert_into_table('grant', g_cols, conn=conn, engine=engine, debug=debug)
        self.id = new_g_id
        return self.id

class GrantAgency:
    """
    `id`: Database ID for GrantAgency
    `name`: Name of grant agency
    `parent_agency_id`: Parent agency ID (to show hierarchy of agencies)
    """
    id:int
    name:str
    parent_agency_id:int
    
    def __init__(self, ga):
        self.id = ga.get('id')
        self.name = ga.get('name')
        self.parent_agency_id = ga.get('parent_agency_id')

    def __str__(self):
        grant_agency_str = f"GrantAgency(id={self.id}, name={self.name}, parent_agency_id={self.parent_agency_id})"
        return grant_agency_str
    
    def write(self, conn=None, engine=None, debug=False):
        new_ga_id = insert_into_table('grant_agency', self.__dict__, conn=conn, engine=engine, debug=debug)
        self.id = new_ga_id
        return self.id


def fetch_id_from_unique_keys(table, data, conn, debug=False):
    # detect unique keys for table and use to query id
    inspector = db.inspect(conn)
    uniq_col_names = inspector.get_unique_constraints(table.name)[0]["column_names"]
    wheres = [table.columns.get(ucn) == data[ucn] for ucn in uniq_col_names]
    if debug:
        print(f"--> finding ids with cols: {', '.join(uniq_col_names)}")
    
    select = db.select(table.c.id).where(db.and_(*wheres))
    result = conn.execute(select).fetchone()
    if result is None:
        raise ValueError(f"Entity not found in table {table.name} with unique keys: {data}")
    return result[0]

def stringify_data(data):
    for k, v in data.items():
        if type(v) == list:
            data[k] = '; '.join(v)
    return data

def insert_into_table(table_name, data, conn=None, engine=None, debug=False):
    metadata_obj = db.MetaData()
    table = db.Table(table_name, metadata_obj, autoload_with=engine)
    data = stringify_data(data)

    if debug:
        print(f"\n--> Inserting into table: {table_name}")
        print(data)

    if conn is None:
        conn = engine.connect()

    trans = conn.begin()  # Begin a transaction
    try:
        result = conn.execute(table.insert().values(data).prefix_with('IGNORE'))
        trans.commit()  # Commit the transaction

        if (result.inserted_primary_key[0] == 0): 
            # entity already existed and was ignored - find id of entity to return
            existing_id = fetch_id_from_unique_keys(table, data, conn)
            if debug:
                print(f"Entity already exists. Fetched id: {existing_id}")
            this_id = existing_id
        else:
            new_resource_id = result.inserted_primary_key[0]
            if debug:
                print(f"New entity added. Inserted id: {result.inserted_primary_key[0]}")
            this_id = new_resource_id

    except Exception as e:
        trans.rollback()  # Rollback the transaction if an error occurs
        sys.stderr.write(f"Transaction rolled back due to: {e}\n")
        raise
    finally:
        conn.close()  # Close the connection
    
    return this_id
