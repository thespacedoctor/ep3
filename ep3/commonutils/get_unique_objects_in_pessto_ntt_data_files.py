#!/usr/bin/env python
# encoding: utf-8
"""
*Get the unique objects names from the PESSTO NTT data*

:Author:
    David Young

.. todo::
    
    @review: when complete pull all general functions and classes into dryxPython

# xdocopt-usage-tempx
"""
import sys
import os
from docopt import docopt
from fundamentals.mysql import readquery
from fundamentals import tools
# from ..__init__ import *

def main(arguments=None):
    """
    *The main function used when ``get_unique_objects_in_pessto_ntt_data_files.py`` is run as a single script from the cl, or when installed as a cl command*
    """
    su = tools(
        arguments=arguments,
        docString=__doc__,
        logLevel="DEBUG"
    )
    arguments, settings, log, dbConn = su.setup()

    # unpack remaining cl arguments using `exec` to setup the variable names
    # automatically
    for arg, val in list(arguments.items()):
        if arg[0] == "-":
            varname = arg.replace("-", "") + "Flag"
        else:
            varname = arg.replace("<", "").replace(">", "")
        if isinstance(val, ("".__class__, u"".__class__)):
            exec(varname + " = '%s'" % (val,))
        else:
            exec(varname + " = %s" % (val,))
        if arg == "--dbConn":
            dbConn = val
        log.debug('%s = %s' % (varname, val,))

    ## START LOGGING ##

    startTime = dcu.get_now_sql_datetime()
    log.info(
        '--- STARTING TO RUN THE get_unique_objects_in_pessto_ntt_data_files.py AT %s' %
        (startTime,))

    # call the worker function
    # x-if-settings-or-database-credientials
    get_unique_objects_in_pessto_ntt_data_files(
        log=log,
        dbConn=dbConn,
    )

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info('-- FINISHED ATTEMPT TO RUN THE get_unique_objects_in_pessto_ntt_data_files.py AT %s (RUNTIME: %s) --' %
             (endTime, runningTime, ))

    return

###################################################################
# CLASSES                                                         #
###################################################################
# xt-class-module-worker-tmpx
# xt-class-tmpx

###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# LAST MODIFIED : July 24, 2014
# CREATED : July 24, 2014
# AUTHOR : DRYX
def get_unique_objects_in_pessto_ntt_data_files(
        dbConn,
        log,
        instrument,
        setup,
        dataRel=False):
    """
    *get unqiue objects in pessto data files*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    

    **Return**

    - None
    

    .. todo::

        - @review: when complete, clean get_unique_objects_in_pessto_ntt_data_files function
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract function to another module
    """
    log.debug(
        'completed the ````get_unique_objects_in_pessto_ntt_data_files`` function')

    instrument = instrument.lower()
    setup = setup.lower()
    noWeights = """ and currentFilename not like "%%weight%%" """
    orderBy =  """order by object"""

    if dataRel:
        dataRel = """and DATA_REL = "ssdr%(dataRel)s" """ % locals()
    else:
        dataRel = ""

    if instrument == "all":
        if setup != "all":
            sqlQuery = """
                select distinct transientBucketId, object from (
                    select distinct transientBucketId, object from efosc_%(setup)s where esoPhaseIII = 1 %(dataRel)s %(noWeights)s
                    union all
                    select distinct transientBucketId, object from sofi_%(setup)s where esoPhaseIII = 1 %(dataRel)s and currentFilename not like "%%weight%%" 
                ) as alias %(orderBy)s 
            """ % locals()
        else:
            sqlQuery = """
                select distinct transientBucketId, object from (
                    select distinct transientBucketId, object from efosc_imaging where esoPhaseIII = 1 %(dataRel)s %(noWeights)s
                    union all
                    select distinct transientBucketId, object from efosc_spectra where esoPhaseIII = 1 %(dataRel)s %(noWeights)s
                    union all
                    select distinct transientBucketId, object from sofi_imaging where esoPhaseIII = 1 %(dataRel)s %(noWeights)s
                    union all
                    select distinct transientBucketId, object from sofi_spectra where esoPhaseIII = 1 %(dataRel)s %(noWeights)s
                )  as alias %(orderBy)s 
            """ % locals()
    else:
        if setup != "all":
            sqlQuery = """
                select distinct transientBucketId, object from %(instrument)s_%(setup)s where esoPhaseIII = 1 %(dataRel)s %(noWeights)s %(orderBy)s
            """ % locals()
        else:
            sqlQuery = """
                select distinct transientBucketId, object from (
                        select transientBucketId, distinct object from %(instrument)s_imaging where esoPhaseIII = 1 %(dataRel)s %(noWeights)s
                        union all
                        select transientBucketId, distinct object from %(instrument)s_spectra where esoPhaseIII = 1 %(dataRel)s %(noWeights)s
                ) as alias  %(orderBy)s
            """ % locals()

    rows = readquery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn
    )

    objectList = []
    objectList[:] = [[row["object"], row["transientBucketId"]] for row in rows]

    log.debug(
        'completed the ``get_unique_objects_in_pessto_ntt_data_files`` function')
    return objectList

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
