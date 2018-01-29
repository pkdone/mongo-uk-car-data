#!/usr/bin/env python
##
# Performs a multi-faceted set of aggregations against the 'MOT' UK annual
# vehicle test result data, displaying summary statistics for various
# dimensions. The MOT data set should first have been downloaded
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
#   $ ./mdb-mot-agg-cars-facets.py
#
# Output:
# * Facet: CategorisedCarsByFuelType
# * Facet: BucketedCarMakesByAmountOfUniqueModels
# * Facet: TopCarsSummary
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
from parallel_facets_agg import aggregate_facets_in_parallel


# Constants
MONGODB_URL = 'mongodb://localhost:27017/'
EXECUTE_PARALLEL = True


####
# Perform a MongoDB Aggregation on the MOT Vehicle records.
####
def mot_vehicle_aggregate():
    client = MongoClient(MONGODB_URL)
    db = client.mot
    collection = db.testresults
    facets = {}
    facets.update(get_pipeline_cars_by_fuel_type())
    facets.update(get_pipeline_car_makes_by_amount_of_unique_models())
    facets.update(get_pipeline_top_cars_summary())
    pipeline = [{'$facet': facets}]
    print 'Aggregation pipeline to be executed:\n'
    pprint(pipeline)
    print '\nAggregation starting {0}'.format(datetime.now())

    if (EXECUTE_PARALLEL):
        print '(PARALLEL)\n'
        result = list(aggregate_facets_in_parallel(collection, pipeline))
    else:
        print '(SERIAL)\n'
        result = list(collection.aggregate(pipeline))

    pprint(result)
    print '\nAggregation finished {0}'.format(datetime.now())
    db.close


####
# Aggregation pipeline for: CategorisedCarsByFuelType
####
def get_pipeline_cars_by_fuel_type():
    return {u'CategorisedCarsByFuelType': [
        {'$group': {
            '_id': '$FuelType',
            'CarAmount': {'$sum': 1},
        }},
        {'$sort': SON([('CarAmount', -1)])},
        {'$project': {
            '_id': 0,
            'FuelType': '$_id',
            'CarAmount': 1
        }},
    ]}


####
# Aggregation pipeline for: BucketedCarMakesByAmountOfUniqueModels
#
# Add following to bucket-Auto/output below to include list of Makes in the
# output:
#  'Makes': {'$push': {'Make': '$_id', 'ModelTypesCount': '$ModelTypes'}}
####
def get_pipeline_car_makes_by_amount_of_unique_models():
    return {u'BucketedCarMakesByAmountOfUniqueModels': [
        {'$match': {
            '$and': [
                {'Make': {'$ne': 'UNCLASSIFIED'}},
                {'Model': {'$ne': 'UNCLASSIFIED'}}
            ]
        }},
        {'$group': {
            '_id': {'Make': '$Make', 'Model': '$Model'},
        }},
        {'$group': {
            '_id': '$_id.Make',
            'ModelTypes': {'$sum': 1},
        }},
        {'$bucketAuto': {
            'groupBy': '$ModelTypes',
            'buckets': 20,
            'granularity': '1-2-5',
            'output': {
                'CarMakesInBucket': {'$sum': 1},
            }
        }},
        {'$project': {
            '_id': 0,
            'MinUniqueModels': '$_id.min',
            'MaxUniqueModels': '$_id.max',
            'CarMakesInBucket': 1
        }},
    ]}


####
# Aggregation pipeline for: TopCarsSummary
####
def get_pipeline_top_cars_summary():
    return {u'TopCarsSummary': [
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
    ]}


####
# Main
####
if __name__ == '__main__':
    mot_vehicle_aggregate()


"""
EXAMPLE OUTPUT:

[{u'BucketedCarMakesByAmountOfUniqueModels': [{u'CarMakesInBucket': 6731,
                                               u'MaxUniqueModels': 2.0,
                                               u'MinUniqueModels': 0.5},
                                              {u'CarMakesInBucket': 1438,
                                               u'MaxUniqueModels': 5.0,
                                               u'MinUniqueModels': 2.0},
                                              {u'CarMakesInBucket': 677,
                                               u'MaxUniqueModels': 20.0,
                                               u'MinUniqueModels': 5.0},
                                              {u'CarMakesInBucket': 411,
                                               u'MaxUniqueModels': 10000.0,
                                               u'MinUniqueModels': 20.0}],
  u'CategorisedCarsByFuelType': [{u'CarAmount': 67548883,
                                  u'FuelType': u'PE'},
                                 {u'CarAmount': 44613240,
                                  u'FuelType': u'DI'},
                                 {u'CarAmount': 157687, u'FuelType': u'EL'},
                                 {u'CarAmount': 144734, u'FuelType': u'HY'},
                                 {u'CarAmount': 109554, u'FuelType': u'OT'},
                                 {u'CarAmount': 91706, u'FuelType': u'LP'},
                                 {u'CarAmount': 4893, u'FuelType': u'ED'},
                                 {u'CarAmount': 3496, u'FuelType': u'GB'},
                                 {u'CarAmount': 2456, u'FuelType': u'FC'},
                                 {u'CarAmount': 508, u'FuelType': u'CN'},
                                 {u'CarAmount': 307, u'FuelType': u'GA'},
                                 {u'CarAmount': 241, u'FuelType': u'ST'},
                                 {u'CarAmount': 185, u'FuelType': u'LN'},
                                 {u'CarAmount': 45, u'FuelType': u'GD'},
                                 {u'CarAmount': 6, u'FuelType': u''}],
  u'TopCarsSummary': [{u'LeastPopularModelName': u'MUSTANG SHELBY GT 350',
                       u'LeastPopularModelQty': 1,
                       u'Make': u'FORD',
                       u'MakeTotal': 17583655,
                       u'ModelTypes': 5031,
                       u'MostPopularModelName': u'FOCUS',
                       u'MostPopularModelQty': 4587737},
                      {u'LeastPopularModelName': u'VAUXHALL ASTRA',
                       u'LeastPopularModelQty': 1,
                       u'Make': u'VAUXHALL',
                       u'MakeTotal': 13631260,
                       u'ModelTypes': 2788,
                       u'MostPopularModelName': u'CORSA',
                       u'MostPopularModelQty': 3738141},
                      {u'LeastPopularModelName': u'COVIN',
                       u'LeastPopularModelQty': 1,
                       u'Make': u'VOLKSWAGEN',
                       u'MakeTotal': 9344476,
                       u'ModelTypes': 4138,
                       u'MostPopularModelName': u'GOLF',
                       u'MostPopularModelQty': 3039601},
                      {u'LeastPopularModelName': u'HYMMER',
                       u'LeastPopularModelQty': 1,
                       u'Make': u'PEUGEOT',
                       u'MakeTotal': 6696786,
                       u'ModelTypes': 2368,
                       u'MostPopularModelName': u'206',
                       u'MostPopularModelQty': 1609294},
                      {u'LeastPopularModelName': u'MEGANE DYNAMIQUE AUTO',
                       u'LeastPopularModelQty': 1,
                       u'Make': u'RENAULT',
                       u'MakeTotal': 5907152,
                       u'ModelTypes': 2855,
                       u'MostPopularModelName': u'CLIO',
                       u'MostPopularModelQty': 2207328}]}]
"""
