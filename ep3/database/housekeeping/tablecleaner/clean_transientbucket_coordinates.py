#!/usr/local/bin/python
# encoding: utf-8
"""
*Fill empty RA, DEC and htmID columns in transientbucket*

:Author:
    David Young

Usage:
    clean_transientbucket_coordinates -s <pathToSettingsFile>
    clean_transientbucket_coordinates --host=<host> --user=<user> --passwd=<passwd> --dbName=<dbName>

Options:
    -h, --help          show this help message
    -s, --settingsFile  path to the settings file
    --host=<host>       database host
    --user=<user>       database user
    --passwd=<passwd>   database password
    --dbName=<dbName>   database name
"""
import sys
import os
from docopt import docopt

def main(arguments=None):
    """
    *The main function used when ``clean_transientbucket_coordinates.py`` is run as a single script from the cl, or when installed as a cl command*
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
    dbConn = False
    if 'settings' in locals() and "database settings" in settings:
        host = settings["database settings"]["host"]
        user = settings["database settings"]["user"]
        passwd = settings["database settings"]["password"]
        dbName = settings["database settings"]["db"]
        dbConn = True
    elif "host" in locals() and "dbName" in locals():
        # SETUP DB CONNECTION
        dbConn = True
        host = arguments["--host"]
        user = arguments["--user"]
        passwd = arguments["--passwd"]
        dbName = arguments["--dbName"]
    if dbConn:
        import pymysql as ms
        dbConn = ms.connect(
            host=host,
            user=user,
            passwd=passwd,
            db=dbName,
        )
        dbConn.autocommit(True)
        log.debug('dbConn: %s' % (dbConn,))

    ## START LOGGING ##
    startTime = dcu.get_now_sql_datetime()
    log.info(
        '--- STARTING TO RUN THE clean_transientbucket_coordinates.py AT %s' %
        (startTime,))

    # call the worker function
    clean_transientbucket_coordinates(
        log=log,
        dbConn=dbConn
    )

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info(
        '-- FINISHED ATTEMPT TO RUN THE clean_transientbucket_coordinates.py AT %s (RUNTIME: %s) --' %
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

def clean_transientbucket_coordinates(
    log,
    dbConn,
):
    """
    *clean_transientbucket_coordinates*

    **Key Arguments**

    - ``log`` -- the logger
    - ``dbConn`` -- the database connection
    

    **Return**

    - None
    

    .. todo::

        @review: when complete, clean worker function and add comments
        @review: when complete add logging
    """
    ## STANDARD LIB ##
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##
    from fundamentals.mysql import readquery, writequery

    # select all the rows in the transient bucket where the RA and DEC are NOT
    # set -- also return the RA, DEC and htm data for the master row for each
    # transient
    sqlQuery = """
        select v.raDeg, v.decDeg, v.transientBucketId, v.htm20ID, v.htm16ID, v.cx, v.cy, v.cz, t.raDeg, t.decDeg, t.transientBucketId from transientBucket t, view_transientBucketMaster v where replacedByRowId = 0 and t.raDeg is Null and t.transientBucketId = v.transientBucketId;
    """

    rows = readquery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn
    )

    # update the transientbucket row by substituting in the
    for row in rows:
        # sqlQuery = """update transientBucket set raDeg = %s, decDeg = %s, htm20ID = %s, htm16ID = %s, cx = %s, cy = %s, cz = %s where transientBucketId = %s and raDeg is NULL""" % (
        #     row["raDeg"],
        #     row["decDeg"],
        #     row["htm20ID"],
        #     row["htm16ID"],
        #     row["cx"],
        #     row["cy"],
        #     row["cz"],
        #     row["transientBucketId"]
        # )
        sqlQuery = """update transientBucket set raDeg = %s, decDeg = %s,  htm16ID = %s where transientBucketId = %s and raDeg is NULL""" % (
            row["raDeg"],
            row["decDeg"],
            row["htm16ID"],
            row["transientBucketId"]
        )

        writequery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
        )

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
