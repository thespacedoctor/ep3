#!/usr/local/bin/python
# encoding: utf-8
"""
*Check the database for object names that have changed and update the filenames of the NTT data files in the archive accordingly*

:Author:
    David Young

:Date Created:
    October 29, 2013

Usage:
    pm_update_ntt_data_filenames -s <pathToSettingsFile> -p <pathToArchiveRoot>
    pm_update_ntt_data_filenames --host=<host> --user=<user> --passwd=<passwd> --dbName=<dbName> -p <pathToArchiveRoot>

Options:
    -h, --help                show this help message
    -s, --settingsFile        path to the settings file
    -p, --pathToArchiveRoot   path to the root of the nested folder archive for ntt data
    --host=<host>             database host
    --user=<user>             database user
    --passwd=<passwd>         database password
    --dbName=<dbName>         database name
"""
################# GLOBAL IMPORTS ####################
import sys
import os
from docopt import docopt
from dryxPython import logs as dl
from dryxPython import commonutils as dcu


def main(arguments=None):
    """
    *The main function used when ``update_ntt_data_filenames.py`` is run as a single script from the cl, or when installed as a cl command*
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
        '--- STARTING TO RUN THE update_ntt_data_filenames.py AT %s' %
        (startTime,))

    # call the worker function
    # x-if-settings-or-database-credentials
    update_ntt_data_filenames(
        pathToArchiveRoot=pathToArchiveRoot,
        dbConn=dbConn,
        log=log
    )

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info(
        '-- FINISHED ATTEMPT TO RUN THE update_ntt_data_filenames.py AT %s (RUNTIME: %s) --' %
        (endTime, runningTime, ))

    return

###################################################################
# CLASSES                                                         #
###################################################################

###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# LAST MODIFIED : September 4, 2013
# CREATED : September 4, 2013
# AUTHOR : DRYX


def update_ntt_data_filenames(
        pathToArchiveRoot,
        dbConn,
        log):
    """
    *Query the database to determine if the object name in the database has been updated and update the updatedFilename column accordingly.*

    **Key Arguments:**
        - ``pathToArchiveRoot`` -- path to the root of the local file archive
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger

    **Return:**
        - None
    """
    ################ > IMPORTS ################
    ## STANDARD LIB ##
    import re
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##
    from dryxPython import mysql as dms

    log.debug('starting the ``update_ntt_data_filenames`` function')
    # TEST THE ARGUMENTS

    ## VARIABLES ##
    reEfoscSpectra = re.compile(
        r'^(t?.*?)(_\d{8}_Gr\d{2}_[a-zA-Z0-9]{4,6}_slit\d\.\d_(\d{5})_\d{1,2}([\w._]{1,8})?\.fits$)')
    reEfoscImage = re.compile(
        r'^(.*?)(_\d{8}_\w\d{3}_(\d{5})_\d{1,2}(_\w{1,3})?\.fits$)')
    reSofiSpectra = re.compile(
        r'^(.*?)(_\d{8}_G(B|R)_merge_(\d{5})_\d{1,2}(_\w{1,3})\.fits$)')
    reSofiImage = re.compile(
        r'^(.*?)(_\d{8}_\w{1,2}_merge_(\d{5})_\d{1,2}\.fits$)')
    reWeight = re.compile(
        r'^(.*)(_(\d{8})_\w{1,2}_merge_(\d{5})_\d{1,2}\.weight\.fits$)')
    reAcq = re.compile(
        r'^acq_(.*?)(_\d{8}_[\w\d]{4,5}_\d{5}_\d{1,2}(_fr)?\.fits)$')

    esSqlQuery = """SELECT primaryId, object, transientBucketId, updatedFilename, currentFilename, archivePath from efosc_spectra WHERE filetype_key_reduction_stage = 3 and filetype_key_calibration = 13  and lock_row = 0 and transientBucketId = 3785621;"""
    eiSqlQuery = """SELECT primaryId, object, transientBucketId, updatedFilename, currentFilename, archivePath  from efosc_imaging WHERE (filetype_key_reduction_stage = 3 and filetype_key_calibration = 13) or (filetype_key_calibration = 8 and filetype_key_reduction_stage = 2 and esoPhaseIII = 1) and lock_row = 0;"""
    ssSqlQuery = """SELECT primaryId, object, transientBucketId, updatedFilename, currentFilename, archivePath  from sofi_spectra WHERE filetype_key_reduction_stage = 3 and filetype_key_calibration = 13 and lock_row = 0;"""
    siSqlQuery = """SELECT primaryId, object, transientBucketId, updatedFilename, currentFilename, archivePath  from sofi_imaging WHERE filetype_key_reduction_stage = 3 and filetype_key_calibration = 13 and lock_row = 0;"""
    wSqlQuery = """SELECT primaryId, object, transientBucketId, updatedFilename, currentFilename, archivePath  from sofi_imaging WHERE filetype_key_reduction_stage = 3 and filetype_key_calibration = 11 and lock_row = 0;"""
    aSqlQuery = """SELECT primaryId, object, transientBucketId, updatedFilename, currentFilename, archivePath  from efosc_imaging WHERE filetype_key_reduction_stage = 3 and filetype_key_calibration = 10 and ((currentFilename like "acq\_%%" or updatedFilename like "acq\_%%") or (currentFilename like "OTHER\_%%" or updatedFilename like "OTHER\_%%") or esoPhaseIII = 1) and lock_row = 0;"""

    tables = ["efosc_spectra", "efosc_imaging", "sofi_spectra",
              "sofi_imaging", "sofi_imaging", "efosc_imaging"]
    regexs = [reEfoscSpectra, reEfoscImage,
              reSofiSpectra, reSofiImage, reWeight, reAcq]
    # sqlQueries = [esSqlQuery, eiSqlQuery,
    #               ssSqlQuery, siSqlQuery, wSqlQuery, aSqlQuery, ]
    sqlQueries = [esSqlQuery, ]
    prepends = ["", "", "", "", "", "acq_"]
    findRelace = [{"_e.fits": ".fits", "_2df.fits": "_si.fits"}, {"": "", }, {
        "_2df.fits": "_si.fits", }, {"": "", }, {"": "", }, {"OTHER": "acq_xxxx", }]

    log.debug('warnings are working')

    # UPDATE updatedFilename
    for table, regex, sqlQuery, prepend, fr in zip(tables, regexs, sqlQueries, prepends, findRelace):
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            log=log
        )
        for row in rows:
            log.debug('row: %s' % (row,))
            if row["updatedFilename"]:
                log.debug('found updatedFilename: %s' %
                          (row["updatedFilename"],))
                matchObject = regex.search(row["updatedFilename"])
            else:
                log.debug('found currentFilename: %s' %
                          (row["currentFilename"],))
                matchObject = regex.search(row["currentFilename"])

            if matchObject and row["object"][0:20] != matchObject.group(1):
                log.debug('changing name')

                updatedFilename = prepend + \
                    row["object"][0:20] + matchObject.group(2)
                updatedFilename = updatedFilename.replace(" ", "_")
                updatedFilepath = pathToArchiveRoot + \
                    "/" + row["archivePath"] + updatedFilename

                sqlQuery = """UPDATE %s SET updatedFilename = '%s', updatedFilepath = '%s' where primaryId = %s  and lock_row = 0""" % (
                    table, updatedFilename, updatedFilepath, row["primaryId"])
                dms.execute_mysql_write_query(
                    sqlQuery,
                    dbConn,
                    log
                )
                log.info('updated object name in filename %s to %s' %
                         (row["updatedFilename"], matchObject.group(1)))
            else:
                log.debug('NOT changing name')
    # UPDATE updatedFilename
    for table, regex, sqlQuery, prepend, fr in zip(tables, regexs, sqlQueries, prepends, findRelace):
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            log=log
        )
        for row in rows:
            log.debug('row: %s' % (row,))
            if row["updatedFilename"]:
                log.debug('found updatedFilename: %s' %
                          (row["updatedFilename"],))
                matchObject = row["updatedFilename"]
            else:
                log.debug('found currentFilename: %s' %
                          (row["currentFilename"],))
                matchObject = row["currentFilename"]

            if matchObject:
                for k, v in fr.iteritems():
                    log.debug('k, v: %s, %s' % (k, v,))
                    if k in matchObject:
                        updatedFilename = matchObject.replace(k, v)
                        updatedFilepath = pathToArchiveRoot + \
                            "/" + row["archivePath"] + updatedFilename
                        # print """updatedFilename %s""" % (updatedFilename,)

                        sqlQuery = """UPDATE %s SET updatedFilename = '%s', updatedFilepath = '%s' where primaryId = %s  and lock_row = 0""" % (
                            table, updatedFilename, updatedFilepath, row["primaryId"])
                        dms.execute_mysql_write_query(
                            sqlQuery,
                            dbConn,
                            log
                        )

    log.debug('completed the ``update_ntt_data_filenames`` function')
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
