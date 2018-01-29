#!/usr/bin/env python
##
# Helper function for PyMongo clients to enable processing of aggregation
# faceted pipelines where each facet dimension is sent in parallel to MongoDB
# to be run by MongoDB's Aggregation Framework. Waits for all parallel
# aggregations to complete and then merges the results before returning.
#
# RESTRICTION: Top level pipeline must only contain one stage called $facet,
# which is usually the case, as normally, each facet's "sub-pipeline" will
# contain the multiple stages to be processed. Throws an error if this
# condition is not met.
#
# Prerequisite:
# * Install PyMongo driver, eg:
#   $ sudo pip install pymongo
##
from pymongo import MongoClient
from multiprocessing.pool import ThreadPool
from itertools import repeat


####
# Runs MongoDB's aggregation command against just one facet pipeline
####
def aggregate_facet(collection, facet_pipeline):
    return [facet_pipeline[0], list(collection.aggregate(facet_pipeline[1]))]


####
# Single arg wrapper function for performing aggregation on just one facet
# pipeline
####
def db_aggregate_wrap(args):
    return aggregate_facet(args[0], args[1])


####
# Processes each facet pipeline in parallel, waiting for and merging all
# results before returning. Throws error if top level pipeline is not a single
# element called '$facet'.
####
def aggregate_facets_in_parallel(collection, facets):
    if (len(facets) > 1):
        raise ValueError('For parallel facet processing the top level '
                         'pipeline must have just one element ("$facet"); '
                         'actual element count: %i' % len(facets))

    top_pipeline_element_key = list(facets[0].keys())[0]

    if (top_pipeline_element_key != '$facet'):
        raise ValueError('For parallel facet processing the top level '
                         'pipeline element must be "$facet"; actual element '
                         'value: "%s"' % top_pipeline_element_key)

    facet_list = facets[0]['$facet']
    # Ask for same number of worker threads as there are facets
    pool = ThreadPool(processes=len(facet_list))  
    facet_results = dict(pool.map(db_aggregate_wrap, zip(repeat(collection),
                         facet_list.items()), chunksize=1))
    pool.close()
    return [facet_results]
