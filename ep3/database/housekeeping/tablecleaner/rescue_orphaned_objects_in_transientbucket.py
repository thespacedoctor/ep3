#!/usr/local/bin/python
# encoding: utf-8
"""
*Clean by any objects in the transientBucket which do not have a masterIdFlag set for some reason.*

:Author:
    David Young

:Date Created:
    November 14, 2013

Usage:
    pm_rescue_orphaned_objects_in_transientbucket -s <pathToSettingsFile>
    
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
    *The main function used when ``rescue_orphaned_objects_in_transientbucket.py`` is run as a single script from the cl, or when installed as a cl command*
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
    for arg, val in arguments.items():
        if arg[0] == "-":
            varname = arg.replace("-", "") + "Flag"
        else:
            varname = arg.replace("<", "").replace(">", "")
        if isinstance(val, ("".__class__, u"".__class__)) :
            exec(varname + " = '%s'" % (val,))
        else:
            exec(varname + " = %s" % (val,))
        if arg == "--dbConn":
            dbConn = val
        log.debug('%s = %s' % (varname, val,))

    ## START LOGGING ##
    startTime = dcu.get_now_sql_datetime()
    log.info(
        '--- STARTING TO RUN THE rescue_orphaned_objects_in_transientbucket.py AT %s' %
        (startTime,))

    # call the worker function
    # x-if-settings-or-database-credentials
    rescue_orphaned_objects_in_transientbucket(
        log,
        dbConn
    )

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info(
        '-- FINISHED ATTEMPT TO RUN THE rescue_orphaned_objects_in_transientbucket.py AT %s (RUNTIME: %s) --' %
        (endTime, runningTime, ))

    return

###################################################################
# CLASSES                                                         #
###################################################################

###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# LAST MODIFIED : January 9, 2014
# CREATED : November 14, 2013
# AUTHOR : DRYX


def rescue_orphaned_objects_in_transientbucket(
        log,
        dbConn
):
    """
    *rescue_orphaned_objects_in_transientbucket*

    **Key Arguments:**
        # copy usage method(s) here and select the following snippet from the command palette:
        # x-setup-docstring-keys-from-selected-usage-options

    **Return:**
        - None

    .. todo::

        @review: when complete, clean worker function and add comments
        @review: when complete add logging
    """
    ################ > IMPORTS ################
    ## STANDARD LIB ##
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##
    log.info('running rescue_orphaned_objects_in_transientbucket' % locals())

    import dryxPython.mysql as dms
    import pessto_marshall_engine.conesearchers.conesearch_transientbucket as cs

    sqlQuery = """
        delete from transientBucket where transientBucketId = 0;
        update transientBucket set masterIDFlag = 0 where transientBucketId = -1;
        update transientBucket set transientBucketId = primaryKeyId where transientBucketId = -1;
    """ % locals()
    dms.execute_mysql_write_query(
        sqlQuery=sqlQuery,
        dbConn=dbConn,
        log=log
    )

    tables = ["tmp", "tmp2"]
    for table in tables:
        tmpTableExists = dms.does_mysql_table_exist(
            dbConn=dbConn,
            log=log,
            dbTableName=table
        )

        if tmpTableExists is True:
            sqlQuery = """
                drop tables %(table)s;
            """ % locals()
            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=dbConn,
                log=log
            )

    sqlQuery = """
        create table tmp as select distinct transientBucketId from transientBucket where masterIdFlag = 0;
    """
    dms.execute_mysql_write_query(
        sqlQuery=sqlQuery,
        dbConn=dbConn,
        log=log
    )

    sqlQuery = """
        create table tmp2 as select distinct transientBucketId from transientBucket where masterIdFlag = 1;
    """
    dms.execute_mysql_write_query(
        sqlQuery=sqlQuery,
        dbConn=dbConn,
        log=log
    )

    sqlQuery = """
        select transientBucketId from tmp where transientBucketId not in (select transientBucketId from tmp2);
    """
    rows = dms.execute_mysql_read_query(
        sqlQuery=sqlQuery,
        dbConn=dbConn,
        log=log
    )

    for row in rows:
        log.debug('row["transientBucketId"]: %s' % (row["transientBucketId"],))
        sqlQuery = """
            select raDeg, decDeg, primaryKeyId from transientBucket where replacedByRowId = 0 and transientBucketId = %s order by primaryKeyId limit 1
        """ % (row["transientBucketId"],)
        subrows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            log=log
        )

        for subrow in subrows:
            log.debug('\tsubrow: %s' %
                      (subrow,))

        transientBucketIdList, raList, decList, objectNameList = cs.search(
            dbConn=dbConn,
            log=log,
            raDeg=subrow["raDeg"],
            decDeg=subrow["decDeg"],
            radiusArcSec=15,
            nearest=True)

        # print transientBucketIdList, raList, decList, objectNameList
        # if transientBucketIdList == None:
        #     continue

        if transientBucketIdList != None and (isinstance(transientBucketIdList, long) or len(transientBucketIdList) == 1):
            log.info(
                'match for orphaned transient again transientBucketId = %s' % (transientBucketIdList[0],))
            newId = transientBucketIdList[0]
            oldId = row["transientBucketId"]
            sqlQuery = """
                update transientBucket set transientBucketId = %(newId)s where  transientBucketId = %(oldId)s;
                update pesstoObjectsComments set pesstoObjectsId = %(newId)s   where pesstoObjectsId = %(oldId)s;
                update transients_history_logs set transientBucketId = %(newId)s where transientBucketId = %(oldId)s;
                # delete from marshall_sources_discoveries where marshallID = %(oldId)s;
                # delete from marshall_sources_photometry where marshallID = %(oldId)s;
                delete from pesstoObjects where transientBucketID = %(oldId)s;
                delete from transientBucketSummaries where transientBucketID = %(oldId)s;
            """ % locals()
            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=dbConn,
                log=log
            )

        else:
            sqlQuery = """
                select min(primaryKeyId) from transientBucket where replacedByRowId = 0 and transientBucketId = %s and name not like "atel%%"
            """ % (row["transientBucketId"],)
            primaryKey = dms.execute_mysql_read_query(
                sqlQuery=sqlQuery,
                dbConn=dbConn,
                log=log
            )

            if primaryKey[0]['min(primaryKeyId)']:

                log.info('added in the masterID for transientBucketId %s' %
                         (row["transientBucketId"],))

                sqlQuery = """
                    update transientBucket set masterIdFlag = 1 where primaryKeyId = %s and replacedByRowId =0
                """ % (primaryKey[0]['min(primaryKeyId)'],)
                dms.execute_mysql_write_query(
                    sqlQuery=sqlQuery,
                    dbConn=dbConn,
                    log=log
                )

    sqlQuery = """
        drop tables tmp;
    """
    dms.execute_mysql_write_query(
        sqlQuery=sqlQuery,
        dbConn=dbConn,
        log=log
    )

    sqlQuery = """
        drop tables tmp2;
    """
    dms.execute_mysql_write_query(
        sqlQuery=sqlQuery,
        dbConn=dbConn,
        log=log
    )

    sqlQuery = """
        select distinct transientBucketId from transientBucket;
    """ % locals()
    rows = dms.execute_mysql_read_query(
        sqlQuery=sqlQuery,
        dbConn=dbConn,
        log=log
    )

    theseTransients = ""
    for row in rows:
        tid = row["transientBucketId"]
        theseTransients = """%(theseTransients)s%(tid)s, """ % locals()
    theseTransients = theseTransients[:-2]

    sqlQuery = """
        select distinct transientBucketId from pesstoObjects where transientBucketId not in (%(theseTransients)s);
    """ % locals()
    rows = dms.execute_mysql_read_query(
        sqlQuery=sqlQuery,
        dbConn=dbConn,
        log=log
    )

    for row in rows:
        transientBucketId = row["transientBucketId"]
        sqlQuery = """
            delete from pesstoObjects where transientBucketId = %(transientBucketId)s;
        """ % locals()
        dms.execute_mysql_write_query(
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            log=log
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
