#!/usr/bin/env python
##
# Helper function for PyMongo clients to enable processing of different
# aggregation faceted pipelines in parallel, where each facet's "sub-pipeline"
# is sent to MongoDB in a seperate client thread, to be processed by the 
# Aggregation Framework. Waits for all parallel aggregations to complete and 
# then merges the results before returning. Under the covers a pool of client
# threads is used, using Python's multiprocessing.pool.ThreadPool library.
#
# RESTRICTION: Top level pipeline must only contain one stage called $facet,
# which is often the case, where each facet's "sub-pipeline" contains the
# multiple stages to be processed. Throws an error if this condition is not 
# met.
#
# Prerequisite:
# * Install PyMongo driver, eg:
#   $ sudo pip install pymongo
##
from multiprocessing.pool import ThreadPool
from itertools import repeat


####
# Runs MongoDB's aggregation command against just one facet pipeline
# 'facet_pipeline' is array of two elements. Element 1 is facet name. Element
# 2 is the set of stages for the pipeline.
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
