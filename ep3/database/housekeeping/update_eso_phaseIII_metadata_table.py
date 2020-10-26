#!/usr/bin/env python
# encoding: utf-8
"""
*Update the eso phase III metadata table in the pessto marshall*

:Author:
    David Young

:Date Created:
    March 28, 2014

Usage:
    pm_update_eso_phaseIII_metadata_table -s <pathToSettingsFile>

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
import dryxPython.mysql as dms


def main(arguments=None):
    """
    *The main function used when ``update_eso_phaseIII_metadata_table.py`` is run as a single script from the cl, or when installed as a cl command*
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
        '--- STARTING TO RUN THE update_eso_phaseIII_metadata_table.py AT %s' %
        (startTime,))

    # call the worker function

    update_eso_phaseIII_metadata_table(
        log=log,
        dbConn=dbConn,
    )

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info(
        '-- FINISHED ATTEMPT TO RUN THE update_eso_phaseIII_metadata_table.py AT %s (RUNTIME: %s) --' %
        (endTime, runningTime, ))

    return

###################################################################
# CLASSES                                                         #
###################################################################

###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################


# use the tab-trigger below for new function
# LAST MODIFIED : March 31, 2014
# CREATED : March 31, 2014
# AUTHOR : DRYX
# copy usage method(s) into function below and select the following snippet from the command palette:
# x-setup-worker-function-parameters-from-usage-method
def update_eso_phaseIII_metadata_table(
        dbConn,
        log):
    """
    *update eso phaseIII metadata table*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        # copy usage method(s) here and select the following snippet from the command palette:
        # x-setup-docstring-keys-from-selected-usage-options

    **Return:**
        - None

    .. todo::

        - @review: when complete, clean update_eso_phaseIII_metadata_table function
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract function to another module
    """
    ################ > IMPORTS ################
    ## STANDARD LIB ##
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##

    log.debug('starting the ``update_eso_phaseIII_metadata_table`` function')

    # first update the individual filetype tables
    _update_filetype_metadata_tables(
        log=log,
        dbConn=dbConn,
    )

    # t = tablename, f = filename match, c = column name
    queryParas = [
        {"t": "efosc_spectra", "f": """ like "%%_sb.fits" """,
            "c": "efosc_1d_binary_spectra"},
        {"t": "efosc_spectra", "f": """ like "%%_si.fits" """,
            "c": "efosc_2d_spectral_images"},
        {"t": "efosc_imaging", "f": """ not like "acq_%%" """,
            "c": "efosc_science_images"},
        {"t": "efosc_imaging", "f": """ like "acq_%%" """,
            "c": "efosc_acq_images"},
        {"t": "sofi_spectra", "f": """ like "%%_sb.fits" """,
            "c": "sofi_1d_binary_spectra"},
        {"t": "sofi_spectra", "f": """ like "%%_si.fits" """,
            "c": "sofi_2d_spectral_images"},
        {"t": "sofi_imaging", "f": """ like "%%weight%%" """,
            "c": "sofi_image_weights"},
        {"t": "sofi_imaging", "f": """ like "%%.fits" and currentFilename not like "%%weight%%" """,
            "c": "sofi_science_images"},
    ]

    releaseVersions = ["SSDR1", "SSDR2", "SSDR3"]
    for rv in releaseVersions:

        for paras in queryParas:
            t = paras["t"]
            f = paras["f"]
            c = paras["c"]

            sqlQuery = """
                select sum(fileSize) as sum from %(t)s where currentFilename %(f)s and data_rel = "%(rv)s" and esoPhaseIII = 1 and data_rel is not null;
            """ % locals()
            rows = dms.execute_mysql_read_query(
                sqlQuery=sqlQuery,
                dbConn=dbConn,
                log=log
            )
            totalFilesize = rows[0]["sum"]

            sqlQuery = """
                select count(*) as sum from %(t)s where currentFilename %(f)s and data_rel = "%(rv)s" and esoPhaseIII = 1 and data_rel is not null;
            """ % locals()
            rows = dms.execute_mysql_read_query(
                sqlQuery=sqlQuery,
                dbConn=dbConn,
                log=log
            )
            thisCount = rows[0]["sum"]

            if totalFilesize == None:
                totalFilesize = 0

            rv = rv.lower()
            sqlQuery = """
                update stats_%(rv)s_overview set dataVolumeBytes = %(totalFilesize)s, numberOfFiles = %(thisCount)s where fileType = "%(c)s"
            """ % locals()
            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=dbConn,
                log=log
            )

            print sqlQuery

    log.debug('completed the ``update_eso_phaseIII_metadata_table`` function')
    return None

# use the tab-trigger below for new function
# x-def-with-logger

###################################################################
# PRIVATE (HELPER) FUNCTIONS                                      #
###################################################################
# LAST MODIFIED : March 28, 2014
# CREATED : March 28, 2014
# AUTHOR : DRYX


def _update_filetype_metadata_tables(
    log,
    dbConn,
):
    """
    *update_filetype_metadata_tables*

    **Key Arguments:**
        - ``log`` -- the logger
        - ``dbConn`` -- the database connection

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

    tables = ["efosc_imaging", "efosc_spectra", "sofi_imaging", "sofi_spectra"]

    for table in tables:
        sqlQuery = """
            select primaryId, currentFilepath from %(table)s where data_rel is not null;
        """ % locals()
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            log=log
        )

        for row in rows:
            primaryId = row["primaryId"]
            thisFilePath = row["currentFilepath"]
            exists = os.path.exists(thisFilePath)
            if exists:
                fileSize = os.path.getsize(thisFilePath)
            else:
                log.warning(
                    "The file %(thisFilePath)s does not exist - please work out where this file has gone and restore it to this location" % locals())
                continue
            # fileSize = 10
            # print "%(fileSize)s bytes" % locals()
            sqlQuery = """
                update %(table)s set filesize = %(fileSize)s where primaryId = %(primaryId)s 
            """ % locals()
            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=dbConn,
                log=log
            )

    return None
############################################
# CODE TO BE DEPECIATED                    #
############################################

if __name__ == '__main__':
    main()

###################################################################
# TEMPLATE FUNCTIONS                                              #
###################################################################
