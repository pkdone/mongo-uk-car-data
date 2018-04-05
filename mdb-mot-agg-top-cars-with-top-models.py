#!/usr/bin/env python
##
# Performs an aggregation against the 'MOT' UK annual vehicle test result data
# to dislplay the most popular older car makes in the UK over the last few
# years, and for each make, the top 5 models with their amounts. The MOT data
# set should first have been downloaded
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
#   $ ./mdb-mot-agg-top-cars-with-top-models.py
#
# Output:
# * The list of the most popular older car makes in the UK over the last few
# years
# * For each of these top car makes, the list of top 5 models
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
            'Models': {'$push': {'Model': '$_id.Model',
                                 'Count': '$ModelTotal'}}
        }},
        {'$sort': SON([('MakeTotal', -1)])},
        {'$limit': 5},
        {'$project': {
            '_id': 0,
            'Make': '$_id',
            'MakeTotal': 1,
            'ModelTypes': 1,
            'Top5Models': {'$slice': ['$Models', 5]}
        }},
    ]

    if (SHOW_EXPLAIN_PLAN):
        result = db.command('aggregate', db.testresults.name,
                            pipeline=pipeline, explain=True)
    else:
        result = list(db.testresults.aggregate(pipeline))

    pprint(result)
    print 'Aggregation finished {0}'.format(datetime.now())


####
# Main
####
if __name__ == '__main__':
    mot_vehicle_aggregate()


"""
EXAMPLE OUTPUT:

[{u'Make': u'FORD',
  u'MakeTotal': 17583655,
  u'ModelTypes': 5031,
  u'Top5Models': [{u'Count': 4587737, u'Model': u'FOCUS'},
                  {u'Count': 4229725, u'Model': u'FIESTA'},
                  {u'Count': 2041555, u'Model': u'TRANSIT'},
                  {u'Count': 1649946, u'Model': u'MONDEO'},
                  {u'Count': 1379129, u'Model': u'KA'}]},
 {u'Make': u'VAUXHALL',
  u'MakeTotal': 13631260,
  u'ModelTypes': 2788,
  u'Top5Models': [{u'Count': 3738141, u'Model': u'CORSA'},
                  {u'Count': 3687760, u'Model': u'ASTRA'},
                  {u'Count': 1507308, u'Model': u'ZAFIRA'},
                  {u'Count': 1057062, u'Model': u'VECTRA'},
                  {u'Count': 533436, u'Model': u'MERIVA'}]},
 {u'Make': u'VOLKSWAGEN',
  u'MakeTotal': 9344476,
  u'ModelTypes': 4138,
  u'Top5Models': [{u'Count': 3039601, u'Model': u'GOLF'},
                  {u'Count': 1921150, u'Model': u'POLO'},
                  {u'Count': 1037135, u'Model': u'PASSAT'},
                  {u'Count': 618721, u'Model': u'TRANSPORTER'},
                  {u'Count': 284002, u'Model': u'TOURAN'}]},
 {u'Make': u'PEUGEOT',
  u'MakeTotal': 6696786,
  u'ModelTypes': 2368,
  u'Top5Models': [{u'Count': 1609294, u'Model': u'206'},
                  {u'Count': 960762, u'Model': u'207'},
                  {u'Count': 753241, u'Model': u'307'},
                  {u'Count': 372351, u'Model': u'107'},
                  {u'Count': 326932, u'Model': u'PARTNER'}]},
 {u'Make': u'RENAULT',
  u'MakeTotal': 5907152,
  u'ModelTypes': 2855,
  u'Top5Models': [{u'Count': 2207328, u'Model': u'CLIO'},
                  {u'Count': 1112296, u'Model': u'MEGANE'},
                  {u'Count': 487346, u'Model': u'SCENIC'},
                  {u'Count': 308642, u'Model': u'KANGOO'},
                  {u'Count': 292397, u'Model': u'LAGUNA'}]}]
"""
