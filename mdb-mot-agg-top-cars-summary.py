#!/usr/bin/env python
##
# Performs an aggregation against the 'MOT' UK annual vehicle test result data
# to dislplay the most popular older car makes in the UK over the last few
# years, plus the most and least popular car models for each of these car
# makes. The MOT data set should first have been downloaded
# https://data.gov.uk/dataset/anonymised_mot_test) and then loaded into a
# MongoDB database called 'mot' in a collection called 'testresults', using
# the Python script 'mdb-mot-import-csv.py' (provided in the same directory as
# this script). The aggregation typically takes a few minutes to complete and
# display the results.
#
# An MOT is a UK annual check on a vehicle, and is mandatory for all cars over
# 3 years old, see: https://en.wikipedia.org/wiki/MOT_test
#
# Suggested index creation command, to run first, in the Mongo Shell:
#   > use mot
#   > db.testresults.ensureIndex({Make: 1, Model: 1})
#
# Usage (ensure py script is executable):
#   $ ./mdb-mot-agg-top-cars-summary.py
#
# Output:
# * The list of the most popular older car makes in the UK over the last few
# years
# * The most and least popular car models for each of these car the most
# popular car makes
# (see base of this file for an example of the aggregation output)
#
# Prerequisite:
# * Install PyMongo driver, eg:
#   $ sudo pip install pymongo
##
from datetime import datetime
from pymongo import MongoClient
from bson.son import SON
from pprint import pprint


# Constants
MONGODB_URL = 'mongodb://localhost:27017/'
SHOW_EXPLAIN_PLAN = False


####
# Perform a MongoDB Aggregation on the MOT Vehicle records.
####
def mot_vehicle_aggregate():
    client = MongoClient(MONGODB_URL)
    db = client.mot
    print 'Aggregation starting {0}'.format(datetime.now())
    pipeline = [
        {'$match': {
            '$and': [
                {'Make': {'$ne': 'UNCLASSIFIED'}},
                {'Model': {'$ne': 'UNCLASSIFIED'}}
            ]
        }},
        {'$group': {
            '_id': {'Make': '$Make', 'Model': '$Model'},
            'ModelTotal': {'$sum': 1}
        }},
        {'$sort': SON([('ModelTotal', -1)])},
        {'$group': {
            '_id': '$_id.Make',
            'MakeTotal': {'$sum': '$ModelTotal'},
            'ModelTypes': {'$sum': 1},
            'MostPopularModelName': {'$first': '$_id.Model'},
            'MostPopularModelQty': {'$first': '$ModelTotal'},
            'LeastPopularModelName': {'$last': '$_id.Model'},
            'LeastPopularModelQty': {'$last': '$ModelTotal'}
        }},
        {'$sort': SON([('MakeTotal', -1)])},
        {'$limit': 5},
        {'$project': {
            '_id': 0,
            'Make': '$_id',
            'MakeTotal': 1,
            'ModelTypes': 1,
            'MostPopularModelName': 1,
            'MostPopularModelQty': 1,
            'LeastPopularModelName': 1,
            'LeastPopularModelQty': 1
        }},
    ]

    if (SHOW_EXPLAIN_PLAN):
        result = db.command('aggregate', db.testresults.name,
                            pipeline=pipeline, explain=True)
    else:
        result = list(db.testresults.aggregate(pipeline))

    pprint(result)
    print 'Aggregation finished {0}'.format(datetime.now())
    db.close


####
# Main
####
if __name__ == '__main__':
    mot_vehicle_aggregate()


"""
EXAMPLE OUTPUT:

[{u'LeastPopularModelName': u'MUSTANG SHELBY GT 350',
  u'LeastPopularModelQty': 1,
  u'Make': u'FORD',
  u'ModelTotal': 17583655,
  u'ModelTypes': 5031,
  u'MostPopularModelName': u'FOCUS',
  u'MostPopularModelQty': 4587737},
 {u'LeastPopularModelName': u'VAUXHALL ASTRA',
  u'LeastPopularModelQty': 1,
  u'Make': u'VAUXHALL',
  u'ModelTotal': 13631260,
  u'ModelTypes': 2788,
  u'MostPopularModelName': u'CORSA',
  u'MostPopularModelQty': 3738141},
 {u'LeastPopularModelName': u'COVIN',
  u'LeastPopularModelQty': 1,
  u'Make': u'VOLKSWAGEN',
  u'ModelTotal': 9344476,
  u'ModelTypes': 4138,
  u'MostPopularModelName': u'GOLF',
  u'MostPopularModelQty': 3039601},
 {u'LeastPopularModelName': u'HYMMER',
  u'LeastPopularModelQty': 1,
  u'Make': u'PEUGEOT',
  u'ModelTotal': 6696786,
  u'ModelTypes': 2368,
  u'MostPopularModelName': u'206',
  u'MostPopularModelQty': 1609294},
 {u'LeastPopularModelName': u'MEGANE DYNAMIQUE AUTO',
  u'LeastPopularModelQty': 1,
  u'Make': u'RENAULT',
  u'ModelTotal': 5907152,
  u'ModelTypes': 2855,
  u'MostPopularModelName': u'CLIO',
  u'MostPopularModelQty': 2207328}]
"""
