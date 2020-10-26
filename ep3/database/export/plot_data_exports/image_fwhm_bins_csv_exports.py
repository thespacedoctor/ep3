# !/usr/bin/env python
# encoding: utf-8
"""
*Export the FWHM stats for the PESSTO EFOSC and SOFI Images*

:Author:
    David Young

:Date Created:
    June 18, 2014

.. todo::
    
    @review: when complete pull all general functions and classes into dryxPython
"""
from __future__ import print_function
################# GLOBAL IMPORTS ####################
import sys
import os
import csv
from docopt import docopt
import numpy as np
from dryxPython import logs as dl
from dryxPython import mysql as dms
from dryxPython import commonutils as dcu
from fundamentals import tools
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
def image_fwhm_bins_csv_exports(
        dbConn,
        log,
        csvExportDirectory):
    """
    *image fwhm bins csv exports*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger

    **Return:**
        - None

    .. todo::

        - @review: when complete, clean image_fwhm_bins_csv_exports function
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract function to another module
    """
    log.debug('starting the ``image_fwhm_bins_csv_exports`` function')

    filterSets = {
        "sofi": ["J", "H", "Ks"],
        "efosc": ["B639", "V641", "R642", "i705"]
    }

    for inst, v in filterSets.items():
        for ffilter in v:

            sqlQuery = """
                select PSF_FWHM from %(inst)s_imaging where esoPhaseIII = 1 and filter = "%(ffilter)s" and currentFilename not like "%%weight%%" order by PSF_FWHM
            """ % locals()
            rows = dms.execute_mysql_read_query(
                sqlQuery=sqlQuery,
                dbConn=dbConn,
                log=log
            )

            print(sqlQuery)

            resolution = 0.2
            minValue = int(
                rows[0]["PSF_FWHM"] / resolution) * resolution + resolution / 2
            maxValue = int(
                rows[len(rows) - 1]["PSF_FWHM"] / resolution) * resolution + resolution / 2

            binnedDictionary = {}
            x = np.arange(minValue - 0.2, maxValue + 0.2, resolution)
            for i in x:
                thisKey = "a%(i)s" % locals()
                binnedDictionary[thisKey] = {
                    "bin": i,
                    "count": 0
                }

            for row in rows:
                bin = int(row["PSF_FWHM"] / resolution) * \
                    resolution + resolution / 2
                thisKey = "a%(bin)s" % locals()
                if thisKey not in list(binnedDictionary.keys()):
                    binnedDictionary[thisKey] = {
                        "bin": bin,
                        "count": 0
                    }
                binnedDictionary[thisKey]["count"] += 1

            with open(csvExportDirectory + "/%(inst)s_imaging_fwhm_binned_%(ffilter)s_band.csv" % locals(), 'wb') as csvFile:
                csvFileWriter = csv.writer(
                    csvFile, dialect='excel', delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                csvFileWriter.writerow(["bin", "count"])
                import collections
                obinnedDictionary = collections.OrderedDict(
                    sorted(binnedDictionary.items()))
                for k, v in obinnedDictionary.items():
                    bin = v["bin"]
                    count = v["count"]
                    csvFileWriter.writerow(["""%(bin).1f""" % locals(), count])

            csvFile.close()

    log.debug('completed the ``image_fwhm_bins_csv_exports`` function')
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
