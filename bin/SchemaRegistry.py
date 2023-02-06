
# ----------------------------------------------------------------------------------------------------
# Interface to Schema Registry
# ----------------------------------------------------------------------------------------------------    

from RestApi import RestApi
import yaml


class SchemaRegistry():

    def __init__(self, ymlconf) -> None:

        with open(ymlconf, 'r') as f:

            config = yaml.safe_load(f)
            self.base_url = config['schemaregistry']['endpoint'].strip().rstrip('/')   + "/api/v1/"

        self.api = RestApi(ymlconf)


    # Get schema JSON text and attached metadata in Schema Registry
    def getschema (self, schemaname):

        headers  = {"accept": "application/json" }

        # Get  metadata
        url      = self.base_url + "schemaregistry/schemas/" + schemaname

        response = self.api.http_get(url = url, headers = headers).json()

        schema = {
           "type"           : response['schemaMetadata']['type'],
           "schemaGroup"    : response['schemaMetadata']['schemaGroup'],
           "name"           : response['schemaMetadata']['name'],
           "description"    : response['schemaMetadata']['description'],
           #"compatibility"  : response['schemaMetadata']['compatibility'],
           #"validationLevel": response['schemaMetadata']['validationLevel'],
           #"evolve"         : response['schemaMetadata']['evolve']
           }

        # Get schema text 
        url      = self.base_url + "schemaregistry/schemas/" + schemaname + "/versions/latest?branch=MASTER"

        response = self.api.http_get(url=url, headers=headers).json()

        schema['schemaText'] = response['schemaText']

        return(schema)


    # Update schame definition in schema registry
    def addschema (self, schema):

        headers  = {"accept": "application/json", "Content-Type" : "application/json" }


        # Create schema metadata
        url      = self.base_url + "schemaregistry/schemas"
  
        payload = {
               'type': schema['type'],
               'schemaGroup': schema['schemaGroup'],
               'name': schema['name'],
               'description': schema['description'],
               'compatibility': 'BACKWARD',
               'validationLevel': 'ALL',
               'evolve': True
               }
        

        response = self.api.http_post(url=url, headers=headers, payload=payload)

        # Add schema text => Not checked for updates
        url      = self.base_url + "schemaregistry/schemas/" + schema['name'] + "/versions"

        payload = {
               'schemaText': schema['schemaText']
               }        

        response = self.api.http_post(url=url, headers=headers, payload=payload)

        return(response.status_code)



   # Convert Qlik schema registry format to SSB schema registry format (name-value -> name)
    def confluent2cloudera (self, schemaname):

        confluent_name = schemaname + "-value"

        sc = self.getschema(confluent_name)

        sc['name'] = schemaname

        response = self.addschema(sc)


        return(response)

