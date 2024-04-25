from json import dumps, loads
from time import perf_counter

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from requests import request
from sqlalchemy.engine import create_engine

standard_color_pallete = '\033[1;37m'


class BigquerySource():
    '''# Bigquery querying tool
    \n 1. Method to create google connection with any bigquery project
    \n 2. see which datasets are available
    \n 3. see which tables from each dataset is available
    \n 4. query that with infos gathered from 2nd and 3rd methods.
    \n ## Connects to Bigquery db with engine 1 - google bigquery API or engine 2 - sqlalchemy API
    \n\tgoogle_conn(self, gpswd = gcp_credential, engine = 1)
    \n ## Obtain which dataset_id are available
    \n\t datasets_info(self, listed = False, frame = False):
    \n ## Use dataset_id obtained from datasets_info function or other means to evaluate its properties and tables
    \n\t tables_info(self, dataset_ids = [] , listed = False, dicted = True):
    \n ## Querying for any specific format
    \n\t querying(self, Select='', From='', Clauses='', Query = """SELECT * FROM 'analytics_295503703.events_*'"""):
    '''
    __slots__ = 'url', 'url2', 'client', 'eng'

    def __init__(self, url2 = None, url='https://www.googleapis.com/auth/cloud-platform'):
        self.url = url
        self.url2 = url2

    def google_conn(self, gcp_credential, engine: int = 1, file: bool = False):
        '''
        Establish connection with GCP big query with json credentials (ACCOUNT SERVICE)
        \n gpswd: Account service credentials to access Big Query
        \n engine: which package engine to use.
            \n\t 1 - google.cloud.bigquery
            \n\t 2 - SQLalchemy
        \n file: gcp credential is a json file path or a string credential'''
        start = perf_counter()
        self.eng = engine
        fail = (f'\t \033[1;31m BQ Connection failed! {standard_color_pallete}')
        success = print(f'\t \033[1;32m BQ Connection successful! {standard_color_pallete}')
        if engine == 1:
            print('started engine 1')
            if file is False:
                try:
                    json_credential = loads(gcp_credential, strict=False)
                    credentials = service_account.Credentials.from_service_account_info(json_credential,
                                                                                        scopes=[self.url])
                    self.client = bigquery.Client(credentials=credentials,
                                                  project=credentials.project_id)
                    print(f'{success}')
                    print('\033[1;33m Connecting with google.bigquery: {:.2f}'.format(perf_counter() - start),
                          f's {standard_color_pallete}')
                except ConnectionError(f'Are you using a file or a path? check it: {gcp_credential[:10]}'):
                    print(f'{fail}')
            else:
                try:
                    credentials = service_account.Credentials.from_service_account_file(gcp_credential,
                                                                                        scopes=[self.url])
                    self.client = bigquery.Client(credentials=credentials,
                                                  project=credentials.project_id)
                    print(f'{success}')
                    print('\033[1;33m Connecting with google.bigquery: {:.2f}'.format(perf_counter() - start),
                          f's {standard_color_pallete}')
                except ConnectionError(f'Are you using a file or a path? check it: {gcp_credential[:10]}'):
                    print(f'{fail}')
        else:
            print('started engine 2')
            try:
                self.client = create_engine(self.url2, credentials_path=gcp_credential)
                print(f'\t \033[1;32m  BQ Connection successful! {standard_color_pallete}')
                print('\033[1;33m Connecting with sqlalchemy took: {:.2f}'.format(perf_counter() - start),
                      f's {standard_color_pallete}')
            except ConnectionError:
                print(f'\t \033[1;31m BQ Connection failed! {standard_color_pallete}')

    def datasets_info(self, listed: bool = False, frame: bool = False):
        '''Obtain which dataset_id are available
        listed: Create a list (True) or dictionary (False) object (not appliable for engine 2)
        frame: add a pandas dataframe object to return
        \t\nInfo here: [Information Schema tables](https://cloud.google.com/bigquery/docs/information-schema-datasets)
        '''
        start = perf_counter()
        if self.eng == 1:
            datasets = list(self.client.list_datasets())
            if listed is False:
                info = {'dataset_id': []}
                for dataset in datasets:
                    info['dataset_id'].append(dataset.dataset_id)
            else:
                info = list()
                for dataset in datasets:
                    info.append(
                        {'dataset_id': dataset.dataset_id})
            if frame is True:
                info = pd.DataFrame(info)
            print('\033[1;33m Dataset info took: {:.2f}'.format(perf_counter() - start),
                  f's {standard_color_pallete}')
            return info
        else:
            query = "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA;"
            result = self.client.execute(query).fetchall()
            if frame is True:
                result = pd.DataFrame(result)
            print('\033[1;33m Dataset info took: {:.2f}'.format(perf_counter() - start),
                  f's {standard_color_pallete}')
            return result

    def tables_info(self, dataset_ids: list = [], frame: bool = False):
        '''Use dataset_id obtained from datasets_info function or other means to evaluate its properties and tables
        \nclient: google_conn()
        \ndataset_ids: a list of one or more dataset_IDs
        \nframe: add a pandas dataframe object to return
        \t\nInfo here: [Information Schema tables](https://cloud.google.com/bigquery/docs/information-schema-tables)
        '''
        start = perf_counter()
        if self.eng == 1:
            try:
                if dataset_ids == [] or type(dataset_ids) != list:
                    raise ValueError('No list of ID or IDs was given')
                else:
                    datatable = dict()
                    for dataset_id in dataset_ids:
                        dataset = self.client.get_dataset(dataset_id)
                        tables = self.client.list_tables(dataset)
                        datatable.update({dataset_id: [table.table_id for table in tables]})

                if frame is True:
                    datatable = pd.DataFrame.from_dict(datatable, orient='index').transpose()
                print('\033[1;33m Tables info took: {:.2f}'.format(perf_counter() - start),
                      f's {standard_color_pallete}')
                return datatable

            except ValueError:
                print(f'\033[1;31m error in dataset_id {standard_color_pallete}')
        else:
            try:
                info = list()
                if dataset_ids == [] or type(dataset_ids) != list:
                    raise ValueError('No list of ID or IDs was given')
                else:
                    for dataset in dataset_ids:
                        query = f"SELECT * FROM {dataset}.INFORMATION_SCHEMA.TABLES;"
                        info.append(self.client.execute(query).fetchall())
                    print('\033[1;33m Tables info took: {:.2f}'.format(perf_counter() - start),
                          f's {standard_color_pallete}')
                    return info
            except ValueError:
                print(f'\033[1;31m error in dataset_id {standard_color_pallete}')

    def querying(self, Select: str = '', From: str = '', Clauses: str = '',
                 Query: str = """SELECT * FROM `somedb`"""):
        '''
        \n Select: Write your select statement 
        \n From: Write your from statetment 
        \n Clauses: Write any clase you find necessary 
        \n Query: Build you own query. Can be changed to get your own query.
        \n Use datasets_info and tables_info to gather more intel of were you want to query
        \t\n querying info here: [Making Bigquery queries](https://cloud.google.com/bigquery/docs/running-queries)
        '''
        start = perf_counter()
        if self.eng == 1:
            if Select != '':
                sql = f"""{Select}{' '}{From}{' '}{Clauses}"""
                script = self.client.query(sql)
                query = script.result()
                query = query.to_dataframe()
                print('building blocks query done')
                return query
            else:
                try:
                    script = self.client.query(Query)
                    query = script.result()
                    query = query.to_dataframe()
                    return query
                except SyntaxError:
                    print(f'\033[1;31m No query done google api {standard_color_pallete}')
            print('\n \033[1;33m Google API Querying time: {:.2f}'.format(perf_counter() - start),
                  f's {standard_color_pallete}')
        else:
            if Select != '':
                query = f"""{Select}{' '}{From}{' '}{Clauses}"""
                result = self.client.execute(query).fetchall()
            else:
                try:
                    result = self.client.execute(Query).fetchall()
                except SyntaxError:
                    print(f'\033[1;31m No query done sqlalchemy {standard_color_pallete}')
            print('\n \033[1;33m SqlAlchemy Querying time: {:.2f}'.format(perf_counter() - start),
                  f's {standard_color_pallete}')
            return result


class DremioSource():
    '''# Dremio querying tool
        \n ## login
        \n \t Generate a token necessary to use the next methods.
        \n To login is necessary the following inputs when instantiating (constructing) the class:

            server_ip: url or dns
            user: the name of the user within Dremio server
            pswd: its password

        \n ## create_job_id
        \n \t Execute a job within Dremio and return its Id.
        \n Every task given to Dremio generates a job and it uses SQL (as a instruction)

            sql_query: a SQL command

        \n ## query_status
        \n \t Shows job id status. Necessary to get status and row count to gather the results.
        \n all it needs is the job_id obtained from create_job_id method
        \n ## query_single_result
        \n \t Returns the executed job id results. Limited to 500 each query with a offset parameter
        \n using the job_id it can retrieve the result - if any - from the job_id.

            job_id: the hash code given when you create a job
            offset: where to start the retrieving of results (default 0)
            limit: how many items to retrieve (default 100, max 500 per request)

        \n ## query_results_all
        \n \t Returns all the data from the desired query.
        \n everything from before just in one method.
        Also de advantage of a single command retrieve all data from a datasource.

            sql_query: a SQL command
            all: retrieve everything (True) of just a few (False)
            offset: where to start the retrieving of results (default 0) -> only works when all is False
            limit: how many items to retrieve (default 100, max 500 per request) -> only works when all is False
    '''

    slots = 'token', 'server_ip', 'headers'

    def __init__(self, server_ip: str, user: str, pswd: str):
        self.token = self.login(server_ip, user, pswd)
        self.server_ip = server_ip
        self.headers = {
            'Authorization': '_dremio' + self.token,
            'Content-Type': 'application/json'
        }

    def __del__(self):
        print('Deleting instance.')

    def login(self, server_ip: str, user: str, pswd: str) -> str:
        """
            Login provides access and must have Token id to further query into Dremio data sources.
        """
        url = f'{server_ip}/apiv2/login'
        headers = {'Content-Type': 'application/json'}
        auth = dumps({
            "userName": f"{user}",
            "password": f"{pswd}"
            })
        r = request("POST", url, headers=headers, data=auth).json()
        try:
            return r['token']
        except Exception:
            raise ConnectionError('Invalid credential used to login.')

    def create_job_id(self, sql_query: str) -> str:
        """
            Select job is a SQL query job order that is given to Dremio. \n
            Necessary to request a job run and retrieve its id.
        """
        url = f"{self.server_ip}/api/v3/sql"
        payload = dumps({"sql": sql_query}, indent=4)
        r = request('POST', url=url, headers=self.headers, data=payload).json()['id']
        return r

    def query_status(self, job_id: str) -> dict:
        '''
            Query status revels if the given job is finished or else. \n
            It`s necessary to run query_status for selects bigger than 500 rows. \n
            This is due to query_single_result function needing the total number of rows to return.
        '''
        url = f"{self.server_ip}/api/v3/job/{job_id}"
        r = request('GET', url=url, headers=self.headers).json()
        return r

    def query_single_result(self, job_id: str, offset: int = 0, limit: int = 500) -> dict:
        '''
            Query results methods. This makes the request to return the desired job_id results
        '''
        if limit > 500:
            raise ValueError('maximum returnable results for each request is 500 items')
        url = f"{self.server_ip}/api/v3/job/{job_id}/results?offset={offset}&limit={limit}"
        r = request('GET', url=url, headers=self.headers).json()
        return r

    def __query_checker(self, job_id: str) -> dict:
        '''
            Private method responsable for checking if the given job/query is finished or running.
        '''
        completion = perf_counter()
        while True:
            qs = self.query_status(job_id)
            js = qs['jobState']
            if js == 'CANCELED' or js == 'FAILED':
                raise ValueError('Job did not worked. Motive:', qs['errorMessage'])
            if js == 'COMPLETED':
                print('\033[1;33m query run time: {:.2f}'.format(perf_counter() - completion),
                      f's {standard_color_pallete}')
                break
        return qs

    def __fetch_all_results(self, job_id: str, rowCount: int) -> list:
        '''
            Private method created to run query_single_result multiples times with all possible results
        '''
        qr = list()
        offsets = (rowCount // 500) + 1
        for row_start in range(offsets):
            qr += self.query_single_result(job_id, row_start * 500, 500)['rows']
        return qr

    def query_results_all(self, sql_query: str, all: bool = True,
                          offset: int = 0, limit: int = 500) -> list:
        '''
            Used to return all query results or to make a single query
            without the need of calling methods query_status, create_job_id and query_single_result
        '''
        completion = perf_counter()
        job_id = self.create_job_id(sql_query)
        job_status = self.__query_checker(job_id)

        row_count = job_status['rowCount']

        if job_status['jobState'] == 'COMPLETED' and all is True:
            qr = self.__fetch_all_results(job_id, row_count)
        elif job_status['jobState'] == 'COMPLETED' and all is False:
            qr = self.query_single_result(job_id, offset, limit)
        else:
            raise AssertionError(job_status)
        print('\033[1;33m Total time to fetch user data: {:.2f}'.format(perf_counter() - completion),
              f's {standard_color_pallete}')
        return qr
