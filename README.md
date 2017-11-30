# iiif Activity Streams Creation
Python code for creating and loading IIIF Activity Streams data into ElasticSearch

The create-activity-stream.py script will convert IIIF Collection Manifest data into Activity Streams. The loader.py will take the Activity Streams data and load it into ElasticSearch using the provided ElasticSearch Index Mapping file. Sample Activity Streams data is provided in the activities folder and the ElasticSearch mapping file is located in the mapping folder.

## Developing
This is a proof of concept and provided 'as is'.

## Prerequisites
* [Python](https://www.python.org/)
* [Python ElasticSearch module](https://elasticsearch-py.readthedocs.io/en/master/)
* [ElasticSearch](https://www.elastic.co/products/elasticsearch)

## Usage
* run create-activity-stream.py to create Activity Streams data
* run loader.py to load Activity Streams data into ElasticSearch

