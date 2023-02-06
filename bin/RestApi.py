
# ----------------------------------------------------------------------------------------------------
# Interface to Rest API
# ----------------------------------------------------------------------------------------------------
import yaml
import requests

from requests.auth import HTTPBasicAuth

class RestApi:

    def __init__(self, ymlconf) -> None:

        with open(ymlconf, 'r') as f:

            config = yaml.safe_load(f)

            self.user =  config['user']
            password =  config['password']
            

        self.auth = HTTPBasicAuth(self.user, password ) 


    def http_get(self, url, headers):

        try:
           response = requests.get(url=url, headers=headers, auth=self.auth)
           if response.status_code != 200: raise (Exception('HTTP Error'))

        except Exception:
            print ('url = GET ' + url)
            print (response.json())
            raise
        
        return (response)


    def http_post(self, url, headers, payload):

        try: 
           response = requests.post(url=url, headers=headers, json=payload, auth=self.auth)
           response.raise_for_status()
           # if response.status_code not in [200, 201]: raise (Exception('HTTP Error'));

        except Exception as e:
            print ('url = POST ' + url)
            print (payload)
            print(response.json())
            raise
        
        return (response)

        requests.post(url=url, headers=headers, json=payload)


