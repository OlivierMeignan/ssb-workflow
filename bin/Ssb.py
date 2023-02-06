from RestApi import RestApi
import yaml


# ----------------------------------------------------------------------------------------------------
# Interface to SSB
# ----------------------------------------------------------------------------------------------------
class Ssb():

    def __init__(self, ymlconf) -> None:
        
        with open(ymlconf, 'r') as f:

            config = yaml.safe_load(f)
            self.base_url = config['ssb']['endpoint'].strip().rstrip('/')  + "/api/v1/"
        
        self.api = RestApi(ymlconf)

        

    def execute_sql (self, jobname, sql):

        url      = self.base_url + "ssb/sql/execute"
        headers  = {"accept": "application/json", "Content-Type" : "application/json" }

        payload  = { "sql": sql, "job_parameters": {  "job_name": jobname}, "execute_in_session": "true", "add_to_history": "true" }

        response = self.api.http_post(url=url, headers=headers, payload=payload)

        return (response.json())


    # list SSB workflow jobs
    def listjobs (self):

        headers  = {"accept": "application/json" }

        # Get  metadata
        url      = self.base_url + "ssb/jobs" 

        response = self.api.http_get(url = url, headers = headers).json()


        return(response)