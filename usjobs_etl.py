import json
import logging
import time
import requests
from datetime import datetime
from dbsqlite import DbSqlite

DB = DbSqlite("usjobs.db")


def etl_handler_us_jobs():
    """
    This function is the main etl handler which handle whole process
    """
    # fetch parameters from a parameter store, text file in this exercise
    with open('params.txt') as p:
        params_json = json.load(p)

    DB.db_connect()
    # @todo: Add a check if database doesn't exist create the tables and database
    # step 1: execute all DDL queries and create tables
    ddl_queries()
    DB.db_commit()
    # step 2: batch insert data fetched from codelist calls into lookup tables
    fetch_lookup_tables_data(params_json)
    # step 3: in this step we are calling search api and upsert data into Dim tables and append into fact table
    search_api_call('Data Engineer', params_json)
    # @todo: Repeat above function for all the key words
    # @todo: the visualization: Data is populated into fact and dim tables. We can run a select using pandas
    # dataframe and use matplotlib.pyplot to visualize and show case time series.
    DB.db_close()


def string_to_epoch(date_string):
    """
    This method is meant to convert string timestamp to epoch(integer timestamp).
    SQLite doesn't support timestamp datatype.
    """
    try:
        timestamp = int(datetime.strptime(date_string,"%Y-%m-%dT%H:%M:%S.%f").timestamp())
    except ValueError:
        timestamp = int(datetime.strptime(date_string,"%Y-%m-%dT%H:%M:%S").timestamp())
    return timestamp


def get_request_api_call(url, params=None, headers=None, verify=None,
                           timeout=60, retry_attempts=3, wait_time=2):
    """Executes a GET request to a given API

    :param url: URL of the API
    :type url: str
    :param params: URL path params, defaults to None
    :type params: dict, optional
    :return: Response from API
    :rtype: response
    """
    # Initialize attempts variable
    attempts = 0

    while attempts <= retry_attempts:
        try:
            response = requests.get(url, params=params, headers=headers, verify=verify, timeout=timeout)
            logging.info('Successfully executed get request')
            break
        except Exception as exc:
            if attempts < retry_attempts:
                logging.warning('Get request failed. Will retry' + str(exc))
            else:
                logging.error('Could not make get request')
                raise Exception('Exception while making get request')
            attempts += 1
            time.sleep(wait_time)
    return response


def fetch_lookup_tables_data(params_json):
    """
    This method is meant to call all codelist calls and fetch metadata
    """
    key_hdr = {"Authorization-Key": params_json.get('Authorization-Key')}
    #fetch job category data
    url_category = params_json.get('job_category')
    resp = get_request_api_call(url=url_category, headers=key_hdr)
    #@todo: add other metadata related calls based on different codelists
    res_json = json.loads(resp.text)
    result_jobcategory = res_json["CodeList"][0]["ValidValue"]
    list_dict_jobcategory = [{
      "Code" : i["Code"],
      "Value" : i["Value"],
      "JobFamily" : i["JobFamily"],
      "LastModified" : string_to_epoch(i["LastModified"]),
      "IsDisabled" : i["IsDisabled"]
    } for i in result_jobcategory]

    DB.execute_many('''insert or replace into lookupjobcategory
    (jobcategorycode,jobcategoryname,jobcategorylastmodified,jobcategorydisabled,jobfamily)
    Values (:Code,:Value,:LastModified,:IsDisabled,:JobFamily)
    ''', list_dict_jobcategory)
    #@todo: add the same response process for other lookup calls
    DB.db_commit()


def search_api_call(key_word, params_json):
    """
    This method will generate search query and calls the API based on key word and process data in batch.

    """
    page_number = 1
    while True:
        params_dict = {"Keyword": key_word, "ResultsPerPage": params_json.get('ResultsPerPage'), "Page": page_number}
        key_hdr = {"Authorization-Key": params_json.get('Authorization-Key')}
        url_search = params_json.get('search_url')
        #     print(params_dict)
        resp = get_request_api_call(url=url_search, params=params_dict, headers=key_hdr)
        number_results = process_result_search(resp, key_word)
        page_number = page_number + 1
        if number_results == 0:
            break


def process_result_search(resp, key_word):
    """
    This method get search API response as an input and process json
    """
    res_json = json.loads(resp.text)
    result_search = res_json["SearchResult"]["SearchResultItems"]
    list_dict_search_res = [{
        "MatchedObjectId": i["MatchedObjectId"],
        "PositionID": i["MatchedObjectDescriptor"]['PositionID'],
        "PositionTitle": i["MatchedObjectDescriptor"]['PositionTitle'],
        "OrganizationName": i["MatchedObjectDescriptor"]['OrganizationName'],
        "JobGrade": i["MatchedObjectDescriptor"]['JobGrade'][0],
        "MinimumRange": i["MatchedObjectDescriptor"]['PositionRemuneration'][0]['MinimumRange'],
        "MaximumRange": i["MatchedObjectDescriptor"]['PositionRemuneration'][0]['MaximumRange'],
        "RateIntervalCode": i["MatchedObjectDescriptor"]['PositionRemuneration'][0]['RateIntervalCode'],
        "PositionStartDate": i["MatchedObjectDescriptor"]['PositionStartDate'],
        "LowGrade": i["MatchedObjectDescriptor"]['UserArea']['Details']['LowGrade'],
        "HighGrade": i["MatchedObjectDescriptor"]['UserArea']['Details']['HighGrade'],
        "OrganizationCodes": i["MatchedObjectDescriptor"]['UserArea']['Details']['OrganizationCodes'],
        "SecurityClearance": i["MatchedObjectDescriptor"]['UserArea']['Details']['SecurityClearance'],
    } for i in res_json["SearchResult"]["SearchResultItems"]]

    # @todo: run execute_many method and populate data into fact and DimJob and reljobcategory tables
    return res_json["SearchResult"]["SearchResultCount"]


def ddl_queries():
    """
    This method includes the create table sql queries
    """
    sql_organization = """
    create or replace table dimorganization
    (
    organizationcode text primary key,
    organizationname text,
    organizationacronym text,
    parentorganizationcode text,
    organizationlastmodified int,
    organizationisdisabled text
    );
   """
    sql_rel_job_category = """
    create or repalce table reljobcategory
    (
    jobobjectid int,
    jobcategory text,
    refreshedtime int
    );
    """
    sql_lookup_job = """
    create or replace table LookUpJobCategory
    (
    jobcategorycode text primary key,
    jobcategoryname text,
    jobcategorylastmodified int,
    jobcategorydisabled text,
    jobfamily text
    );
    """
    sql_dim_jobs = """
    create or relace table dimjobs
    (
    objectid int primary key,
    positionid text,
    minimumrange int,
    maximumrange int,
    rateintervalcode text,
    positionstartdate int,
    positionenddate int,
    publicationstartdate int,
    applicationclosedate int
    lowgrade int,
    highgrade int,
    organizationcodes text
    )
    ;
    """
    sql_fact_jobs = """
    create or replace table FactJobSearch
    (keyword text,
    timestamp int,
    matchedobjectid int
    );
    """
    DB.execute_sql(sql_organization)
    DB.execute_sql(sql_rel_job_category)
    DB.execute_sql(sql_lookup_job)
    DB.execute_sql(sql_dim_jobs)
    DB.execute_sql(sql_fact_jobs)