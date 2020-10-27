#!/usr/local/bin/python
# encoding: utf-8
"""
*Export the current classification from the PESSTO Marshall*

:Author:
    David Young

Usage:
    pm_export_pessto_classifications -s <pathToSettingsFile>
    pm_export_pessto_classifications --host=<host> --user=<user> --passwd=<passwd> --dbName=<dbName> --exportFile=<pathToExportFile>

Options:
    -h, --help                         show this help message
    -v, --version                      show version
    -s, --settingsFile                 path to the settings file
    --host=<host>                      database host
    --user=<user>                      database user
    --passwd=<passwd>                  database password
    --dbName=<dbName>                  database name
    --exportFile=<pathToExportFile>    path of the file to export the results to
"""
from builtins import str
import sys
import os
from docopt import docopt

def main(arguments=None):
    """
    *The main function used when ``pessto_classifications.py`` is run as a single script from the cl, or when installed as a cl command*
    """
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

    # SETUP A DATABASE CONNECTION BASED ON WHAT ARGUMENTS HAVE BEEN PASSED
    if 'settings' in locals() and "database settings" in settings:
        host = settings["database settings"]["host"]
        user = settings["database settings"]["user"]
        passwd = settings["database settings"]["password"]
        dbName = settings["database settings"]["db"]
    elif 'host' in locals() and 'dbName' in locals():
        # SETUP DB CONNECTION
        dbConn = True
        host = arguments["--host"]
        user = arguments["--user"]
        passwd = arguments["--passwd"]
        dbName = arguments["--dbName"]
        log.debug('here: %s' % (dbConn,))
        import pymysql as ms
        dbConn = ms.connect(
            host=host,
            user=user,
            passwd=passwd,
            db=dbName,
        )
        dbConn.autocommit(True)
        log.debug('dbConn: %s' % (dbConn,))

    log.debug('locals(): %s' % (locals(),))
    log.debug('dbConn: %s' % (dbConn,))

    ## START LOGGING ##
    startTime = dcu.get_now_sql_datetime()
    log.info(
        '--- STARTING TO RUN THE pessto_classifications.py AT %s' %
        (startTime,))

    if "settings" in locals() and "exports" in settings:
        exportFile = settings["exports"][
            "path to pessto classifications csv export"]

    # call the worker function
    pessto_classifications(
        log=log,
        dbConn=dbConn,
        exportFile=exportFile
    )

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info(
        '-- FINISHED ATTEMPT TO RUN THE pessto_classifications.py AT %s (RUNTIME: %s) --' %
        (endTime, runningTime, ))

    return

###################################################################
# CLASSES                                                         #
###################################################################

###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# LAST MODIFIED : October 28, 2013
# CREATED : October 28, 2013
# AUTHOR : DRYX
# copy usage method(s) into function below and select the following snippet from the command palette:
# x-setup-worker-function-parameters-from-usage-method

def pessto_classifications(
    log,
    dbConn,
    exportFile,
):
    """
    *pessto_classifications*

    **Key Arguments**

    - ``log`` -- the logger
    - ``dbConn`` -- the database connection
    - ``exportFile`` -- path of text file to export the results to
    

    **Return**

    - None
    

    .. todo::

        @review: when complete, clean worker function and add comments
        @review: when complete add logging
    """
    ## STANDARD LIB ##
    import datetime as d
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##
    from fundamentals.mysql import readquery

    fob = open(exportFile, 'w')

    # GRAB THE CALSSIFICATION DATA
    sqlQuery = """SELECT t.transientBucketId,
                          t.name,
                          t.spectralType,
                          t.dateCreated,
                          t.survey,
                          t.observationMJD
                  FROM transientBucket t, pesstoObjects p
                  WHERE p.classifiedFlag = 1 AND
                        t.transientBucketId = p.transientBucketId AND
                        t.spectralType is not NULL AND
                        survey = 'pessto'
                  ORDER BY t.transientBucketId,
                            t.observationMJD DESC"""
    try:
        log.debug(
            "attempting to get the classification data for the PESSTO Objects")
        rows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn
        )
    except Exception as e:
        log.error(
            "could not get the classification data for the PESSTO Objects - failed with this error: %s " %
            (str(e),))
        raise EnvironmentError(
            "could not get the classification data for the PESSTO Objects - failed with this error: %s " % (str(e),))

    # WRITE THE CSV FILE HEADER
    now = d.datetime.now()
    lastName = ""
    fob.write("Generated " + now.strftime("%H:%M, %A %d %B %Y") + "\n")
    fob.write("transientBucketId|objectId|ra|dec|spectralType|observationMJD\n")
    fob.write(
        "---------------------------------------------------------------\n")

    for row in rows:
        # GRAB THE COORDINATES
        sqlQuery = """SELECT DISTINCT raDeg,
                                    decDeg
                      FROM transientBucket
                      WHERE transientBucketId = %s AND
                            raDeg is not NULL AND
                            decDeg is not NULL""" % (row["transientBucketId"],)
        try:
            log.debug(
                "attempting to grab the coordinates of transientBucketId %s" %
                (row["transientBucketId"],))
            rows = readquery(
                log=log,
                sqlQuery=sqlQuery,
                dbConn=dbConn
            )
            # COLLECT AND RETURN THE RESULTS - A LIST OF DICTIONARIES
            raDeg, decDeg = rows[0]["raDeg"], rows[0]["decDeg"]
        except Exception as e:
            log.error(
                "could not grab the coordinates of transientBucketId %s - failed with this error %s: " %
                (row["transientBucketId"], str(e),))
            return -1

        if(lastName != str(row["name"]) or lastClassification != str(row["spectralType"])):
            try:
                log.debug(
                    "attempting to write the classification data to file %s" % (fob,))
                fob.write(str(row["transientBucketId"]) + "|" + str(row["name"]) + "|" + str(raDeg) + "|" + str(
                    decDeg) + "|" + str(row["spectralType"]) + "|" + str(row["observationMJD"]) + "\n")
            except Exception as e:
                log.error(
                    "could not write the classification data to file %s  - failed with this error %s: " % (str(e),))
                return -1

            lastName = str(row["name"])
            lastClassification = str(row["spectralType"])

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
