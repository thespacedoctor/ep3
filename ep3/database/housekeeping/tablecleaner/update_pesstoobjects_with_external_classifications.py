#!/usr/local/bin/python
# encoding: utf-8
"""
*Reads new ``transientBucket`` events for any non-pessto classifications, propagates these classifications into the marshall and sets the correct marshall workflow flags to the relevant objects.*

:Author:
    David Young

:Date Created:
    December 12, 2013

Usage:
    pm_update_pesstoobjects_with_external_classifications -s <pathToSettingsFile>

Options:
    -h, --help          show this help message
    -s, --settingsFile  path to the settings file
"""
################# GLOBAL IMPORTS ####################
import sys
import os
from docopt import docopt
from dryxPython import logs as dl
from dryxPython import commonutils as dcu


def main(arguments=None):
    """
    *The main function used when ``update_pesstoobjects_with_external_classifications.py`` is run as a single script from the cl, or when installed as a cl command*
    """
    ########## IMPORTS ##########
    ## STANDARD LIB ##
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##

    # setup the command-line util settings
    from fundamentals import tools
    su = tools(
        arguments=arguments,
        docString=__doc__,
        logLevel="DEBUG"
    )
    arguments, settings, log, dbConn = su.setup()

    # unpack remaining cl arguments using `exec` to setup the variable names
    # automatically
    for arg, val in arguments.iteritems():
        if arg[0] == "-":
            varname = arg.replace("-", "") + "Flag"
        else:
            varname = arg.replace("<", "").replace(">", "")
        if isinstance(val, str) or isinstance(val, unicode):
            exec(varname + " = '%s'" % (val,))
        else:
            exec(varname + " = %s" % (val,))
        if arg == "--dbConn":
            dbConn = val
        log.debug('%s = %s' % (varname, val,))

    ## START LOGGING ##
    startTime = dcu.get_now_sql_datetime()
    log.info(
        '--- STARTING TO RUN THE update_pesstoobjects_with_external_classifications.py AT %s' %
        (startTime,))

    # call the worker function
    # x-if-settings-or-database-credientials
    update_pesstoobjects_with_external_classifications(
        dbConn,
        log
    )

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info(
        '-- FINISHED ATTEMPT TO RUN THE update_pesstoobjects_with_external_classifications.py AT %s (RUNTIME: %s) --' %
        (endTime, runningTime, ))

    return

###################################################################
# CLASSES                                                         #
###################################################################

###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# LAST MODIFIED : December 12, 2013
# CREATED : December 12, 2013
# AUTHOR : DRYX


def update_pesstoobjects_with_external_classifications(
        dbConn,
        log):
    """
    *update pesstoobjects with external classifications*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger

    **Return:**
        - None

    .. todo::

        - @review: when complete, clean update_pesstoobjects_with_external_classifications function
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract function to another module
    """
    ################ > IMPORTS ################
    ## STANDARD LIB ##
    import time
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##
    import dryxPython.mysql as dms
    from pessto_marshall_engine.database.tools import set_pessto_object_classification_flag

    log.debug(
        'completed the ````update_pesstoobjects_with_external_classifications`` function')
    # TEST THE ARGUMENTS

    ## VARIABLES ##

    ################ > VARIABLE SETTINGS ######
    ################ >ACTION(S) ################
    #### FIND OBJECTS IN TRANSIENT BUCKET THAT NOW HAVE A CLASSIFICATION EXTERNAL TO PESSTO ####
    #---------------------------------------------------------------------------------------------------------------------------------#
    sqlQuery = """SELECT DISTINCT transientBucketId, survey
                                FROM transientBucket WHERE
                                (
                                    spectralType is not NULL
                                    AND transientBucketId NOT IN
                                    (
                                        SELECT transientBucketId
                                        FROM pesstoObjects
                                        WHERE classifiedFlag = 1
                                    )
                                )
                        """

    try:
        log.debug(
            "attempting to query for objects in ``transientBucket`` table that now have an external classification")
        rows = dms.execute_mysql_read_query(sqlQuery, dbConn, log)
    except Exception, e:
        log.error(
            "could not query for objects in transientBucket table that now have an external classification - failed with this error %s: " %
            (str(e),))
        return -1

    for row in rows:
        tbId = row['transientBucketId']
        survey = row['survey']
        # print str(tbId)+" "+str(survey)+" \n"

        # CHANGE CLASSIFICATION FLAG, MWFFLAG AND AWFFLAG
        from pessto_marshall_engine.database.tools import set_pessto_object_classification_flag
        set_pessto_object_classification_flag.set_pessto_object_classification_flag(
            log=log,
            dbConn=dbConn,
            transientBucketId=tbId,
            classificationSurvey=survey
        )

    log.debug(
        'completed the ``update_pesstoobjects_with_external_classifications`` function')
    return None

# use the tab-trigger below for new function
# x-def-with-logger
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
