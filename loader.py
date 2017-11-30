
import sys, getopt, os, json, time, datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk

# set UTF-8 encoding
reload(sys)
sys.setdefaultencoding('UTF8')
es = Elasticsearch(timeout=60)

# set variables

# set to True if the input data files are tab-delimited, with a record key in position 0 and a json string in position 1
keys_in_datafile = False

# the elasticsearch index name
index_name = "activity_streams"

# the elasticsearch index document type
doc_type = "activities"

# the directory that holds the file or files of input data
# datapath = "cdm/"+doc_type
datapath = "activities/"

# elasticsearch shards and replicas
shards = 1
replicas = 2

# the mapping file ... leave blank to let Elasticsearch create its own mapping based on the input data
# mapping_file = "Merged-Recon/reconciliation_mapping.json"
# mapping_file = "cdm/mapping-"+doc_type+".json"
mapping_file = "activities_mapping/activities_mapping.json"
#mapping_file = ""

# do you want to create a new index?  Set to True, otherwise False
create_index = True

# create the index?
if create_index:
  print("Creating index \"%s\"" % (index_name))
  res = es.indices.create(index = index_name)
  print("Create response: %s" % (res))

# if a mapping file was specified, apply it to the index
# look for mapping file
if len(mapping_file) > 0:
  if os.path.isfile(mapping_file): 
    
    # the mapping file was found
    print("Adding mapping from %s " % (mapping_file))
    
    # open the file and read its contents into a string
    with open(mapping_file) as mf:
      mapping = mf.read().replace('\n', '')
    
    # if the mapping string has content
    if len(mapping) > 0:
    
      # encode the string as JSON
      mapping_json = json.loads(str(mapping))
      
      # add the mapping to the index and document type
      res = es.indices.put_mapping( index=index_name, doc_type=doc_type, body=mapping_json)
      print("Add mapping response: '%s'" % (res))
      
    else:
      print("The mapping file %s had no content" % (mapping_file))
      
  else:
    print("A mapping file not found at %s" % (mapping_file))

# look for input files and bulk, load them

print("Looking for input files in %s" % (datapath))

for f in os.listdir(datapath):

  print("Reading started for  %s/%s" % (datapath,f))

  with open(datapath + "/"+f) as fl:
    actions = []
    actions_inc = 1
    actions_limit = 2000
    
    # for each line in the file
    for line in fl:
      line = line
      key = ""
      json_obj = {}
      
      if keys_in_datafile:
        arr = line.rstrip().split('\t')
        key = arr[0]
        if len(arr) > 1:
          #print(arr[1])
          json_obj = json.loads(arr[1].strip())
        else:
          print("WARNING: expected value in arr[1] when splitting this line on tabs: "+line)
      else:
        json_obj = json.loads(line)
        
      # if the json object is not empty ...
      if json_obj > 0:
      
        json_data = json.dumps(json_obj)
        actions_inc += 1
        action = {
          "_index": index_name,
          "_type": doc_type,
          "_source": json_data
        } 
        if len(key) > 0:
          action["_id"] = key
        actions.append(action)
        if actions_inc == actions_limit:
          bulk(es, actions)
          actions = []
          actions_inc = 0
    
    # after processing all lines in the file, if there are any left-over actions in the actions list, load them
    if len(actions) > 0:
      bulk(es, actions)
  
  #print("Reading finished for %s/%s" % (datapath,f))
    
  #print("Bulk update started for  %s/%s" % (datapath,f))
  #bulk(es, actions)
  #print("Bulk update finished for  %s/%s" % (datapath,f))

