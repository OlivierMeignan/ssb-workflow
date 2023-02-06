
import matplotlib.pyplot as plt
import networkx as nx

import yaml
import os

from Ssb import Ssb
from SchemaRegistry import SchemaRegistry


APPNAME = "SSBWKF"
HOMEDIR = os.path.dirname(__file__) + "/../"
CONF    = HOMEDIR + "etc/config.yml"
WKFDIR  = HOMEDIR + "workflows/"



# ----------------------------------------------------------------------------------------------------
# Operator abstraction of Ssb, SchemaRegistry, ... inferfaces which are used by the Runner.
# Complexity of external operations is masked by the Operator which exposes to Runner an simple view 
# ----------------------------------------------------------------------------------------------------    
class Operator():

    def __init__(self):

        self.ssb = Ssb(CONF)
        self.sc  = SchemaRegistry(CONF)

    # Abstract of schema registry "confluent2cloudera"  which accepts only one parameter
    def confluent2cloudera(self, jobname, command):

        return(self.sc.confluent2cloudera(command))

    # Abtract of listjobs() because it takes only 1 positional argument but 3 were given in execute function
    def listjobs(self, jobname, command):

        return(self.ssb.listjobs() )


    # Launch particular function according to the jobtype
    def execute(self, jobname='', jobtype='', command=''):

        switch = {
            'sql'                : self.ssb.execute_sql,
            'confluent2cloudera' : self.confluent2cloudera,
            'listjobs'           : self.listjobs
        }

        return(switch[jobtype](jobname, command))




# ----------------------------------------------------------------------------------------------------
# Schedule and launch jobs sequenced in workflow files
# ----------------------------------------------------------------------------------------------------   
class Runner():

    # Load graph structure which represents hierarchy of jobs and workflows
    def __init__(self) -> None:

       self.G = nx.Graph()
       self.nodes = {}
       self.edges = []

    # Internal function to init runner
    def init_runner(self) -> None:

       for fname in os.listdir(WKFDIR):      # Look up file names (fname") from directory  "workflows"all(iterable)
        
           with open(WKFDIR + fname, 'r') as f: 
               workflow = yaml.safe_load(f)  # load Yaml workflow file 
            
               node = fname[0:-4]            # A nodename in the Graph is the Worflow filename => file extension (.yml) is removed

               self.nodes[node] = None       # Init nodes list

               try:                          # Loop Successors in workflow file to infer nodes and edges of the  Graph
                  for wkfname in workflow['succ']:
                     node_succ = wkfname['workflow']               # Node successor (= workflow file successor)
                     self.edges.append( (node, node_succ) )        # If node has a successor we create an edge between nodes
               except KeyError: pass

       
       # We load nodes and edge into the Graph object
       self.G.add_nodes_from(self.nodes.keys())
       self.G.add_edges_from(self.edges)


    # Launch jobs according their priority in the Graph
    # ---------------------------------------------------------------------------------------------------- 
    def run(self):

        self.init_runner()
       
        if (self.G.number_of_edges()  == 0 ):   # Handle a specif case which raises and error when node (ie workflow file) doesn't have a successor
            a = self.nodes
        else: 
            h, a = nx.hits(self.G)              # We use  hits.authority function of the Graph to sort out priorities for launching jobs with the correct sequence
        

        op = Operator()                         # An abstraction of  calls to interface (ssb, schemaregistry, ...) 


        # We read workflows files according to the hierachy of the graph (=> sorted a.items = hits.authorities sorted out)
        # Workflows files contains sequences of jobs so we loop into workflows files to launch jobs
        for wkfname in dict(sorted(a.items(), key=lambda item: item[1])).keys():

            with open(WKFDIR + wkfname + '.yml', 'r') as f:    # Open workflow file
                workflow = yaml.safe_load(f)
                #x=workflow['succ'][0]['workflow']
                idx = 0

                len_idx = len(workflow['jobs'])                # Number of jobs in the workflow file

                for job in workflow['jobs']:                   # Loop for Job tuples in Workflow file (ex: (job name, sql))
                    idx = idx + 1                              # Sequence of the job. We start at job[1]

                    if idx < len_idx:
                        idxsucc = str(idx+1)                   # Next idx for the job
                    else:
                        wkfsucc_array = []                     # Next workflow whan at last idx or idx if no next workflow
                        for wkfsucc in workflow.get('succ', []):
                            wkfsucc_array.append(wkfsucc['workflow'])
                        idxsucc = str(idx) if (wkfsucc_array == []) else '__'.join(wkfsucc_array)
                        
                

                              # if at last job idx, next job idx is the next  workflow if exists
                        
                    jname, jtype = tuple(job)                  # The Job tuple in Workflow file (ex: ("jobname:", "sql:"))
                    
                                                               #  Jobname is a tag so later we can rebuild the Graph based
                                                               # on jobnames in SSB 
                    tag = APPNAME + '_' + wkfname + '_' + str(idx) + '_' + idxsucc + '_'  + job[jname]

                    command = job[jtype]                       # We Ex: sql or convert2confluent
                    
                    print ("Execute : " + tag + ' -> ' + jtype + ':' + command[:20])
                    op.execute (tag, jtype, command)           # Execute Job

     


    # On inti job status 
    # ---------------------------------------------------------------------------------------------------- 
    def status(self):

        op = Operator()                         # An abstraction of  calls to interface (ssb, schemaregistry, ...) 
        edges = {}
        response = op.execute(jobtype='listjobs')

        for tag in (response['jobs']):
            
            if ( APPNAME in tag['name'] ):
               newtag = tag['name'].replace ('__', ',')
               (appname, wkfname, idx, succs, jobname ) = newtag.split('_')
               
               if ',' in succs:
                   for succ in succs.split(','):
                      edges[wkfname + '_' + jobname + '_' + idx] = succ
               else:
                   edges[wkfname + '_' + jobname + '_' + idx] = succs
 

        return(0)




    #print(G.degree())
    # ---------------------------------------------------------------------------------------------------- 
    def draw(self):

       nx.draw(self.G, with_labels=True, font_weight='bold')

       plt.show()


    
   
# ----------------------------------------------------------------------------------------------------
# The application
# ----------------------------------------------------------------------------------------------------        
def main():

    r = Runner()

    r.status()
    #r.run()

    #r.draw()


if __name__ == "__main__":
    main()

