from __future__ import print_function
import dateutil.parser
import datetime
import json
import uuid
import rdflib
import rdflib_jsonld
from rdflib import Graph, plugin
from rdflib.serializer import Serializer
from rdflib.parser import Parser

# @context: "https://www.w3.org/ns/activitystreams",

def createManifestUri(data):
    data_lines = data.readlines()
    for line in data_lines:
        host, site_id, item_id, date = line.split(',')
        manifest_uri = 'https://cdm' + host + '.contentdm.oclc.org/digital/iiif-info' + site_id + '/' + item_id + '/manifest.json'
        dt = dateutil.parser.parse(date).isoformat()
        createActivityStream(manifest_uri, dt)

def createActivityStream(manifest_uri, dt):
    activity_stream = {}
    activity_id = str(uuid.uuid4())
    activity_stream['summary'] = 'Manifest created'
    activity_stream['type'] = 'Create'
    activity_stream['id'] = 'http://52.204.112.237:3051/activity/' + activity_id
    activity_stream['actor'] = 'http://viaf.org/viaf/145425848'
    activity_stream['object'] = {}
    activity_stream['object']['id'] = manifest_uri
    activity_stream['object']['type'] = "sc:Manifest"
    activity_stream['endTime'] = dt
    activity_stream_json = json.dumps(activity_stream)
    print(activity_id, activity_stream_json, sep='\t') 
    
def createRdf(activity_stream_json):
    graph = Graph()
    graph.parse(data=activity_stream_json, format='json-ld')
    triples = graph.serialize(format='nt') 
    print(triples.strip())
    graph.close()
    

if __name__ == "__main__":
    data = open('partners.txt', 'r')
    createManifestUri(data)