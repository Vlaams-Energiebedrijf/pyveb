## Custom Tableau client
import requests
import os
import logging
import json
import urllib3
from collections import namedtuple
urllib3.disable_warnings()
from typing import Dict, List, Any

Wb = namedtuple('Wb', ['site', 'name', 'project_name', 'id', 'luid', 'tables', 'datasources', 'tags'])
Table = namedtuple('Table', ['connection_type', 'database', 'schema', 'table'])
Datasource = namedtuple('Datasource', ['name', 'id', 'luid', 'last_refresh', 'last_incremental_update', 'last_update'])
Tags = namedtuple('Tags', ['id', 'name' ])
WbExt = namedtuple('WbExt', ['site', 'project_name', 'workbook_name', 'url', 'workbook_id', 'owner_id', 'owner_name', 'owner_role', 'created_at', 'updated_at', 'description', 'tables', 'datasources', 'tags'])


class tableauRestClient: 


    def __init__(self, site_name:str = 'VEB', **kwargs:str) -> None:
        """
            Initialize the class and set the specified Tableau connection parameters.

            Parameters:
                kwargs: Keyword arguments to overwrite environment variables.

            Raises:
                KeyError: If a required parameter is not provided in either the environment or kwargs dict.
         """
        variables = {}
        for v in ['TABLEAU_PAT', 'TABLEAU_PAT_NAME', 'TABLEAU_URL', 'TABLEAU_VERSION']:
            if v.lower() in kwargs:
                variables[v.lower()] = kwargs[v.lower()]
            elif v in os.environ:
                variables[v.lower()] = os.environ[v]
            else:
                logging.critical(f"Key '{v}' not found in environment or kwargs")

        for var in variables:
            setattr(self, var.lower(), variables[var.lower()])
        self.site_name = site_name
        self.headers = {
            'accept': 'application/json',
            'content-type': 'application/json'
        }
        self.auth_token, self.site_id = self._get_auth_token()
        self.headers['X-tableau-auth'] = self.auth_token
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
    def _get_auth_token(self) -> str:
        url_signin = f'{self.tableau_url}/api/{self.tableau_version}/auth/signin'
        body ={
        "credentials": {
            "personalAccessTokenName": self.tableau_pat_name,
            "personalAccessTokenSecret": self.tableau_pat,
            "site": {
                "contentUrl": self.site_name
                }
            }
        }
        req = requests.post(url_signin, json=body, headers=self.headers, verify=False)
        req.raise_for_status()
        response = json.loads(req.content)
        return response["credentials"]["token"], response["credentials"]["site"]["id"]

    def retrieve_all_workbooks(self) -> List[Dict[str, Any]]:
        endpoint = f'api/{self.tableau_version}/sites/{self.site_id}/workbooks?pageSize=100&pageNumner=1'
        url = f'{self.tableau_url}/{endpoint}'
        response = self.session.get(url)
        response.raise_for_status()
        all_workbooks = []
        try:
            all_workbooks.extend(json.loads(response.content)['workbooks']['workbook'])
        except KeyError:
            logging.critical('No workbooks found, returning empty list of workbooks')
            return all_workbooks
        pagination = json.loads(response.content)['pagination']

        page_number = int(pagination['pageNumber'])
        page_size = int(pagination['pageSize'])
        total_size = int(pagination['totalAvailable'])
        
        while  page_number * page_size < total_size: 
            endpoint = f'api/{self.tableau_version}/sites/{self.site_id}/workbooks?pageSize=100&pageNumber={page_number+1}'
            url = f'{self.tableau_url}/{endpoint}'
            response = self.session.get(url)
            response.raise_for_status()
            all_workbooks.extend(json.loads(response.content)['workbooks']['workbook'])
            pagination = json.loads(response.content)['pagination']
            page_number = int(pagination['pageNumber'])
            page_size = int(pagination['pageSize'])
            total_size = int(pagination['totalAvailable'])

        return all_workbooks

    def retrieve_all_users(self) -> Dict[str, Any]:
        """
            returns a dictionary of all user items {name, site_role} excluding guests
        """
        endpoint = f'api/{self.tableau_version}/sites/{self.site_id}/users?pageSize=100&pageNumner=1'
        url = f'{self.tableau_url}/{endpoint}'
        response = self.session.get(url)
        all_users = []
        try:
            all_users.extend(json.loads(response.content)['users']['user'])
        except KeyError:
            logging.critical('No users found, returning empty list of users')
            return {}
        pagination = json.loads(response.content)['pagination']

        page_number = int(pagination['pageNumber'])
        page_size = int(pagination['pageSize'])
        total_size = int(pagination['totalAvailable'])
        
        while  page_number * page_size < total_size: 
            endpoint = f'api/{self.tableau_version}/sites/{self.site_id}/users?pageSize=100&pageNumber={page_number+1}'
            url = f'{self.tableau_url}/{endpoint}'
            response = self.session.get(url)
            all_users.extend(json.loads(response.content)['users']['user'])
            pagination = json.loads(response.content)['pagination']
            page_number = int(pagination['pageNumber'])
            page_size = int(pagination['pageSize'])
            total_size = int(pagination['totalAvailable'])

        ## GET ALL USERS
        all_users_dict = {}
        for user in all_users: 
            if user['siteRole'] != 'Guest':
                all_users_dict[user['id']] = {
                    'name': user['fullName'], 
                    'site_role': user['siteRole']
                }

        return all_users_dict
    
    def retrieve_all_sites(self) -> List[str]:
        endpoint = f'api/{self.tableau_version}/sites'
        url = f'{self.tableau_url}/{endpoint}'
        response = self.session.get(url)
        return json.loads(response.content)
        
    def retrieve_metadata(self, graphql_query, variables=None) -> Dict:

        metadata_url = f'{self.tableau_url}/api/metadata/graphql'

        try:
            graphql_query = json.dumps({"query": graphql_query, "variables": variables})
        except Exception as e:
            raise Exception("Must provide a string")
 
        parameters = {}
        parameters["headers"] = {}
        parameters["headers"]["x-tableau-auth"] = self.auth_token
        parameters["headers"]["content-type"] = "application/json"
        # parameters["headers"]["User-Agent"] = "Tableau Server Client/3.21"
        parameters["data"] = graphql_query
        response = requests.Session().post(metadata_url, **parameters)
        return response

    def retrieve_custom_sql(self,  graphql_query: str) -> List[Dict[str, Any]] :
        results = self._fetch_custom_sql(graphql_query)
        workbooks_custom = []
        for custom_sql_table in results.json()['data']['customSQLTablesConnection']['nodes']:
            custom_sql_table['site'] = self.site_name
            workbooks_custom.append(custom_sql_table)
        return workbooks_custom

    def _fetch_custom_sql(self, graphql_query: str):
        return self.retrieve_metadata(graphql_query)

    def retrieve_native_wb(self, graphql_query: str) -> List[Dict[str, Any]]:
        results = self._fetch_native_wb(graphql_query)
        workbooks_native = []
        for native_sql_table in results.json()['data']['workbooksConnection']['nodes']:
            native_sql_table['site'] = self.site_name
            workbooks_native.append(native_sql_table)
        return workbooks_native

    def _fetch_native_wb(self, graphql_query:str) -> Dict[str, Any]:
        return self.retrieve_metadata(graphql_query)

    def enrich_basic_wb(self, wb: Wb) -> WbExt:
        workbook_id = wb.luid
        endpoint = f'api/{self.tableau_version}/sites/{self.site_id}/workbooks/{workbook_id}'
        url = f'{self.tableau_url}/{endpoint}'
        response = self.session.get(url)
        rest_wb = json.loads(response.content).get('workbook', {})
        all_users_dict = self.retrieve_all_users()
        webpage = rest_wb.get('webpageUrl', None)
        owner_id = rest_wb.get('owner', {}).get('id', None)
        if webpage:
            url = f'{self.tableau_url}/#/{webpage.split("/#/")[-1]}'
        else:
            url = None
        owner_name = all_users_dict.get(owner_id, {}).get('name', None)
        owner_role = all_users_dict.get(owner_id, {}).get('site_role', None)
        return WbExt(wb.site, wb.project_name, wb.name, url, wb.luid, owner_id, owner_name, owner_role, rest_wb.get('createdAt', None), rest_wb.get('updatedAt', None), rest_wb.get('description', None), wb.tables, wb.datasources, wb.tags )

    def close_session(self) -> None:
        endpoint = f'api/{self.tableau_version}/auth/signout'
        signout_url = f'{self.tableau_url}/{endpoint}'
        req = requests.post(signout_url, data=b'', headers=self.headers, verify=False)
        req.raise_for_status()
        # logging.critical('succesfully logged out')
        return

    @classmethod
    def parse_meta_workbook_into_wb(cls, obj) -> Wb :
        """
            parse method intended to be used with graphql queries like: 

            workbooksConnection(first: 10000){
                nodes {
                id
                luid
                name
                containsUnsupportedCustomSql
                projectName
                containerName
                
                embeddedDatasources {
                    upstreamTables {
                    database {
                        name
                    }
                    name
                    schema
                    fullName
                    connectionType
                    }
                }

                upstreamDatasources {
                    name
                    id
                    luid
                    extractLastRefreshTime
                    extractLastIncrementalUpdateTime
                    extractLastUpdateTime
                }

                tags {
                    id
                    name
                }
                }
            }
            }
        """
        list_tables = []
        if obj['embeddedDatasources']:
            for x in obj['embeddedDatasources']:
                for y in x['upstreamTables']:
                    t = Table(y['connectionType'], y['database']['name'], y['schema'], y['name'])
                    list_tables.append(t)
        list_datasources = []
        if obj['upstreamDatasources']:
            for d in obj['upstreamDatasources']:
                ds = Datasource(d['name'], d['id'], d['luid'], d['extractLastRefreshTime'], d['extractLastIncrementalUpdateTime'], d['extractLastUpdateTime'])
                list_datasources.append(ds)
        list_tags = []
        if obj['tags']:
            for t in obj['tags']:
                tag = Tags(t['id'], t['name'])
                list_tags.append(tag)

        parsed_wb = Wb(obj['site'], obj['name'], obj['projectName'], obj['id'], obj['luid'], list_tables, list_datasources, list_tags )
        
        return parsed_wb
    
    @classmethod
    def parse_meta_sql_into_wb(cls, obj) -> Wb:
        """
            parse method intended to be used with graphql queries like: 

            customSQLTablesConnection(first: 10000){
                    nodes {

                    database  {
                        name
                    }

                    tables {
                        name
                        schema
                        connectionType
                        fullName
                    }

                    downstreamWorkbooks {
                        luid
                        name
                        id
                        projectName
                        

                    }
                    }
                }
        """
        list_tables = []
        list_wbs = []
        for x in obj['tables']:
            try:
                t = Table(x['connectionType'], obj['database']['name'], x['schema'], x['name'])
                list_tables.append(t)
            except:
                pass
        for wb in obj['downstreamWorkbooks']:
            try:
                parsed_wb = Wb(obj['site'], wb['name'], wb['projectName'], wb['id'], wb['luid'], list_tables, None, None)
                list_wbs.append(parsed_wb)
            except:
                pass
        return list_wbs
    
