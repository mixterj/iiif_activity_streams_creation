from __future__ import print_function
import dateutil.parser
import datetime
import json
import uuid
import urllib2
import sys
import argparse
import time

# this code is designed to take a IIIF Collection URI and parse through it to produce a set of Activity Stream Activities
# The code requires a IIIF Collection URI with pages of Collecitons that have Manifests on them.
# The data also requires a field that contains a date modified field in it
# The code might not work with all IIIF Collections depending on how they are structured

#  to run the script (bare minimum arguments) ...
#  python create-activity-stream.py -collection_url [url] -modified_field [metadata field]  
#  e.g.,
#  python create-activity-stream.py -collection_url 'https://d.lib.ncsu.edu/collections/catalog/manifest?f%5Bformat%5D%5B%5D=Still+image&q=basketball' -modified_field 'dcterms:modified'

def main():
    
    #set global variables
    global initial_creation_field
    global modified_field
    global base_uri_prefix
    global creator
    global f
    
    # open file for Activity Stream output
    f = open('activities/as.txt', 'w+')
    
    # set the arguments for the script
    parser = argparse.ArgumentParser()
    parser.add_argument('-collection_url', help='The URI for the IIIF Collection', required=True)
    parser.add_argument('-created_field', help='Field that has the initial manifest creation date in it. Use "None" if not present', required=False)
    parser.add_argument('-modified_field', help='Field that has the manifest modification date creation in it', required=True)
    parser.add_argument('-base_uri_prefix', help='URI prefix for the resulting Activity Stream Activities. Use "None" if not present', required=False)
    parser.add_argument('-creator', help='URI for the creator responsible for modifying the Manifest. Use "None" if not present', required=False)
    args = parser.parse_args()

    # convert args to variables
    collection_url = args.collection_url
    initial_creation_field = args.created_field
    modified_field = args.modified_field
    base_uri_prefix = args.base_uri_prefix
    creator = args.creator
    
    # send the collection URL off for processing
    parse_collection(collection_url)

def parse_collection(collection_url):
    # grab the the IIIF Collection data and process the first page
    collection_response = urllib2.urlopen(collection_url)
    collection_data = json.loads(collection_response.read())
    collection_first_page_url = (collection_data['first'])
    parse_collection_pages(collection_first_page_url)
    
def parse_collection_pages(collection_first_page_url):
    # Process the first page of the Collection
    collection_page_response = urllib2.urlopen(collection_first_page_url)
    collection_page_data = json.loads(collection_page_response.read())
    # grab each Manifest URI and go grab the data
    for manifest in collection_page_data['manifests']:
        manifest_url = manifest['@id']
        parse_manifest_pages(manifest_url)
    
    # Recursive function to continue processing next pages in the collection page 
    if 'next' in collection_page_data:
        #time.sleep(3)
        parse_collection_pages(collection_page_data['next'])

def parse_manifest_pages(manifest_url):
    # Process the individual Manifest pages
    manifest_page_response = urllib2.urlopen(manifest_url)
    manifest_data = json.loads(manifest_page_response.read())
    create_event(manifest_data)

def create_event(manifest_data):
    # Create an Activity Stream Activity
    activity_stream = {}
    activity_id = str(uuid.uuid4())
    activity_stream['summary'] = 'Manifest updated'
    activity_stream['type'] = 'Update'
    # build an Activity Stream Activity URI
    activity_stream['id'] = build_activity_uri(base_uri_prefix, activity_id)   
    # Check to see if there is a creator argument and if so create an 'actor' property
    if creator:
        activity_stream['actor'] = creator
    activity_stream['object'] = {}
    activity_stream['object']['id'] = manifest_data['@id']
    activity_stream['object']['type'] = "sc:Manifest"
    activity_stream['endTime'] = dateutil.parser.parse(manifest_data[modified_field]).isoformat()
    activity_stream_json = json.dumps(activity_stream)
    # send the Activty off to writing
    write_activity(activity_id, activity_stream_json)
    # If there is an initial creation argument produce an Initial Creation Event Activity Stream
    # This would be a one-off process if you did not already have a Creation Activity
    if initial_creation_field:
        initial_creation(manifest_data, activity_stream, activity_id) 

def initial_creation(manifest_data, activity_stream, activity_id):
    # If the argument value is 'None' a default date will be used
    if initial_creation_field == 'None':
        activity_stream['summary'] = 'Manifest created'
        activity_stream['type'] = 'Create'
        activity_stream['endTime'] = dateutil.parser.parse('2017-01-01').isoformat()
    else:
    # if the argument value is a field that field value will be used for the initial Creation Date
        activity_stream['summary'] = 'Manifest created'
        activity_stream['type'] = 'Create'
        activity_stream['endTime'] = dateutil.parser.parse(manifest_data[initial_creation_field]).isoformat()
    activity_stream_json = json.dumps(activity_stream)
    # send the activity off for writing
    write_activity(activity_id, activity_stream_json)
        
    
def build_activity_uri(base_uri_prefix, activity_id):
    # if the base URI prefix for Activities is set use it
    if base_uri_prefix:
        activity_uri = base_uri_prefix + activity_id
    else:
    # Otherwise use a default example.org prefix
        activity_uri = 'http://example.org/activity/' + activity_id
    return activity_uri

def write_activity(activity_id, activity_stream_json):
    # produce a TSV output for printing and indexing into ElasticSearch
    data = (activity_id + '\t' + activity_stream_json + '\n')
    f.write(data)
    
if __name__ == "__main__":
        main()