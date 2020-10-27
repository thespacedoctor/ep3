#!/usr/bin/env python
# encoding: utf-8
"""
*The Classification Breakdowns*

:Author:
    David Young

.. todo::
    
    @review: when complete pull all general functions and classes into dryxPython
"""
from __future__ import print_function
import sys
import os
import csv
from docopt import docopt
from fundamentals.mysql import readquery
from fundamentals import tools
import ep3.database.housekeeping.flags.update_transientbucketsummaries_flags as utf
# from ..__init__ import *

###################################################################
# CLASSES                                                         #
###################################################################
# xt-class-module-worker-tmpx
# xt-class-tmpx

###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# LAST MODIFIED : June 18, 2014
# CREATED : June 18, 2014
# AUTHOR : DRYX
def followup_classification_breakdown_csv_export(
        dbConn,
        log,
        csvExportDirectory):
    """
    *classification breakdown csv export*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    

    **Return**

    - None
    

    .. todo::

        - @review: when complete, clean followup_classification_breakdown_csv_export function
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract function to another module
    """
    log.debug(
        'completed the ````followup_classification_breakdown_csv_export`` function')

    # Fix Classifictions
    classificationCleaner = {
        "Ic peculiar": "Ic-p",
        "I peculiar": "I-p",
        "Ia-pec": "Ia-p",
        "Ia peculiar": "Ia-p",
        "II peculiar": "II-p",
        "IIb peculiar": "IIb-p",
        "IIn peculiar": "IIn-p",
        "SN Ia": "Ia",
        "SN Ib/c": "Ibc",
        "SN II": "II",
        "type Ia": "Ia",
        "type II-P": "IIP"
    }
    for k, v in list(classificationCleaner.items()):
        sqlQuery = """
            update transientBucket set spectralType = "%(v)s" where replacedByRowId =0 and spectralType = "%(k)s"
        """ % locals()
        writequery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
        )
        sqlQuery = """
            update transientBucketSummaries set recentClassification = "%(v)s" where recentClassification = "%(k)s"
        """ % locals()
        writequery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
        )

    # Grab the classification names
    sqlQuery = """
        select distinct recentClassification from transientBucketSummaries s, pesstoObjects p where p.transientBucketId=s.transientBucketId and p.classifiedFlag=1 and p.transientBucketId=s.transientBucketId and p.classifiedFlag=1 and 
        recentClassification is not null and 
        recentClassification not like "%%NUL%%" order by recentClassification; 
    """ % locals()
    rows = readquery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn
    )

    print(rows)

    # grab the counts for the classifiactions
    classifications = []
    for row in rows:
        thisClassification = row["recentClassification"]
        sqlQuery = """
            select "%(thisClassification)s" as recentClassification, count(*) as count from transientBucketSummaries s, pesstoObjects p where recentClassification = "%(thisClassification)s" and p.transientBucketId=s.transientBucketId and p.classifiedFlag=1 and (p.marshallWorkflowLocation like "follow%%")
        """ % locals()
        counts = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn
        )
        for count in counts:
            print(count)
            classifications.append(count)

    topLevelClass = {
        "II": 0,
        "I": 0,
        "Ia": 0,
        "Ib": 0,
        "Ibc": 0,
        "Ic": 0,
        "IIn": 0,
        "IIb": 0
    }
    otherClass = {
        "AGN": 0,
        "variable star": 0
    }
    for row in classifications:
        print(row)
        for tc, v in list(topLevelClass.items()):
            pec = """%(tc)s-p""" % locals()
            humm = """%(tc)s?""" % locals()
            if pec == row["recentClassification"] or humm == row["recentClassification"]:
                topLevelClass[tc] += row["count"]
                row["count"] = 0
        for i in ["agn", "qso"]:
            if i in row["recentClassification"].lower() and row["recentClassification"] != "AGN":
                otherClass["AGN"] += row["count"]
                row["count"] = 0
        for i in ["cv", "variable star"]:
            if i in row["recentClassification"].lower() and row["recentClassification"] != "variable star":
                otherClass["variable star"] += row["count"]
                row["count"] = 0

    # Recount
    for row in classifications:
        for tc, v in list(topLevelClass.items()):
            if tc == row["recentClassification"]:
                row["count"] += v
        for c, v in list(otherClass.items()):
            if c == row["recentClassification"]:
                row["count"] += v

    classifications = list(classifications)
    from operator import itemgetter
    classifications = reversed(
        sorted(classifications, key=itemgetter('count')))
    # if sortDesc == "desc":
    #     listName = reversed(listName)

    with open(csvExportDirectory + "/pessto_followup_classification_breakdown.csv", 'wb') as csvFile:
        csvFileWriter = csv.writer(
            csvFile, dialect='excel', delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvFileWriter.writerow(["classification", "count"])
        for row in classifications:
            if row["count"] > 0:
                csvFileWriter.writerow(
                    [row["recentClassification"], row["count"]])

    csvFile.close()

    log.debug('completed the ``classification_breakdown_csv_export`` function')
    return None

# use the tab-trigger below for new function
# xt-def-with-logger

###################################################################
# PRIVATE (HELPER) FUNCTIONS                                      #
###################################################################

############################################
# CODE TO BE DEPECIATED                    #
############################################

if __name__ == '__main__':
    main()

###################################################################
# TEMPLATE FUNCTIONS                                              #
###################################################################
