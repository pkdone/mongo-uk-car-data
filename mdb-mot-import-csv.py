#!/usr/bin/env python
##
# Loads 'MOT' UK annual vehicle test result data from files into a MongoDB
# database. The MOT data set should be first downloaded from:
# https://data.gov.uk/dataset/anonymised_mot_test). This script assumes that a
# pipe ('|') is used to separate fields, one record per line (one line will
# result in one document inserted into a MongoDB collection). This script
# looks for all files in a named directory, where the files have the name
# 'test_result_NNNN.txt' (where NNNN is a 4 digit year, such as 2013). Users
# can run multiple instances of this script in parallel, each operating on a
# different subset of files (each subset would need to be grouped into
# separate directories). The whole data set will take a few hours to load into
# MongoDB.
#
# An MOT is a UK annual check on a vehicle, and is mandatory for all cars over
# 3 years old, see: https://en.wikipedia.org/wiki/MOT_test
#
# The MOT total data set is around 400m records (as of 2016 MOT published
# results), which, when imported into MongoDB, has a data size of about 100GB
# (approximate storage consumed is 30GB, when compressed on WiredTiger, with
# its default 'snappy' compression). Loading the full data set may take in the
# order of a whole day to load into MongoDB with this script.
#
# Usage (ensure py script is executable):
#   $ ./mdb-mot-import-csv.py /path/to/mot/data/year-folder
#
# Output:
# * A MongoDB collection with the namespace 'mot.testresults'
#
# Prerequisite:
# * Install PyMongo driver, eg:
#   $ sudo pip install pymongo
##
import os
import sys
import fnmatch
import csv
import time
import re
from datetime import datetime
from pymongo import MongoClient


# Constants
MONGODB_URL = 'mongodb://localhost:27017/'
INVALID_DATE = datetime.strptime('1970-01-01', '%Y-%m-%d')
TEST_FILENAME_MATCH = 'test_result_*.txt'
TEST_FILENAME_YEAR = '\d\d\d\d'


####
# Locate each CSV file in a given directory, establish its MOT year and then
# process the CSV file.
####
def mot_data_load():
    if len(sys.argv) < 2:
        raise Exception('Please specify the path to the folder containing the'
                        ' MOT documents to import.')
    else:
        importFolderPath = sys.argv[1]

        if not os.path.isdir(importFolderPath):
            raise Exception('Invalid or not found path for folder containing '
                            'MOT documents to import: %s' % importFolderPath)

    client = MongoClient(MONGODB_URL)
    db = client.mot
    print('DB initialised {0}'.format(datetime.now()))

    for filename in os.listdir(importFolderPath):
        if fnmatch.fnmatch(filename, TEST_FILENAME_MATCH):
            print 'Processing file {0}'.format(filename)
            motYear = int(re.search(TEST_FILENAME_YEAR, filename).group())
            process_csv_saving_to_db(db,
                                     os.path.join(importFolderPath, filename),
                                     motYear)

    print('DB finished {0}'.format(datetime.now()))


####
# For each row in the CSV, collects fields into a document and inserts this
# into a MongoDB collection.
####
def process_csv_saving_to_db(db, filename, motYear):
    data_file = open(filename)

    try:
        csvreader = csv.reader(data_file, delimiter='|')
        next(csvreader)  # Skip first header line of CSV file

        for fields in csvreader:
            document = {
                'MotYear': motYear,
                'TestId': get_int_val(fields[0]),
                'VehicleId': get_int_val(fields[1]),
                'TestDate': get_date_val(fields[2]),
                'TestClassId': get_int_val(fields[3]),
                'TestType': fields[4],
                'TestResult': fields[5],
                'TestMileage': get_int_val(fields[6]),
                'PostcodeRegion': fields[7],
                'Make': fields[8],
                'Model': fields[9],
                'Colour': fields[10],
                'FuelType': fields[11],
                'CylinderCapacity': get_int_val(fields[12]),
                'FirstUseDate': get_date_val(fields[13])
            }

            db.testresults.insert(document)
    finally:
        data_file.close()


####
# Get int value of string or return -1 if emtpy/null/not-int
####
def get_int_val(field):
    return -1 if not field else int(field) if field.isdigit() else -1


####
# Get date value of YYYY-MM-DD string or return 1970-01-01 if emtpy/null
####
def get_date_val(field):
    try:
        if not field:
            date_val = INVALID_DATE
        else:
            date_val = datetime.strptime(field, '%Y-%m-%d')
    except ValueError:
        print "Could not convert field '%s' to date" % field
        date_val = INVALID_DATE

    return date_val


####
# Main
####
if __name__ == '__main__':
    mot_data_load()
