#!/usr/local/bin/python
# encoding: utf-8
"""
*Scan the database from new 1d spectra files and create the counterpart rows for the binary tables*

:Author:
    David Young

:Date Created:
    October 29, 2013

Usage:
    pm_create_spectra_binary_table_extension_rows_in_database -s <pathToSettingsFile>
    pm_create_spectra_binary_table_extension_rows_in_database --host=<host> --user=<user> --passwd=<passwd> --dbName=<dbName>

Options:
    -h, --help          show this help message
    -s, --settingsFile  path to the settings file
    --host=<host>       database host
    --user=<user>       database user
    --passwd=<passwd>   database password
    --dbName=<dbName>   database name
"""
################# GLOBAL IMPORTS ####################
import sys
import os
from docopt import docopt
from dryxPython import logs as dl
from dryxPython import commonutils as dcu


def main(arguments=None):
    """
    *The main function used when ``create_spectra_binary_table_extension_rows_in_database.py`` is run as a single script from the cl, or when installed as a cl command*
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
        '--- STARTING TO RUN THE create_spectra_binary_table_extension_rows_in_database.py AT %s' %
        (startTime,))

    # call the worker function

    create_spectra_binary_table_extension_rows_in_database(
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
        '-- FINISHED ATTEMPT TO RUN THE create_spectra_binary_table_extension_rows_in_database.py AT %s (RUNTIME: %s) --' %
        (endTime, runningTime, ))

    return

###################################################################
# CLASSES                                                         #
###################################################################

###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# LAST MODIFIED : September 8, 2013
# CREATED : September 8, 2013
# AUTHOR : DRYX


def create_spectra_binary_table_extension_rows_in_database(
        dbConn,
        log):
    """
    *create binary table spectrum rows in database by duplicating the 1D spectrum rows*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger

    **Return:**
        - None

    .. todo::

    """
    ################ > IMPORTS ################
    ## STANDARD LIB ##
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##
    import dryxPython.mysql as dms

    log.debug(
        'completed the ````create_binary_table_spectrum_rows_in_database`` function')

    # select rows of binary table files that don't have a corresponding row in
    # the binary extension table in the database
    sqlQuery = """
        SELECT filename, primaryId from efosc_spectra where esoPhaseIII = 1 and filename like "%%\_sb.fits" and CONCAT(filename, primaryId) not in (SELECT * from (SELECT CONCAT(filename, efosc_spectra_id) from efosc_spectra_binary_table_extension where filename like "%%\_sb.fits") as alias)
    """
    rows = dms.execute_mysql_read_query(
        sqlQuery,
        dbConn,
        log
    )

    for row in rows:
        # add an empty row for each missing binary table file
        sqlQuery = """
            INSERT INTO efosc_spectra_binary_table_extension(filename, efosc_spectra_id) VALUES("%s",%s)
        """ % (row["filename"], row["primaryId"])
        dms.execute_mysql_write_query(
            sqlQuery,
            dbConn,
            log
        )

    # repeat for sofi
    sqlQuery = """
        SELECT filename, primaryId from sofi_spectra where esoPhaseIII = 1 and filename like "%%\_sb.fits" and CONCAT(filename, primaryId) not in (SELECT * from (SELECT CONCAT(filename, sofi_spectra_id) from sofi_spectra_binary_table_extension where filename like "%%\_sb.fits") as alias)
    """
    rows = dms.execute_mysql_read_query(
        sqlQuery,
        dbConn,
        log
    )

    for row in rows:

        sqlQuery = """
            INSERT INTO sofi_spectra_binary_table_extension(filename, sofi_spectra_id) VALUES("%s",%s)
        """ % (row["filename"], row["primaryId"])
        dms.execute_mysql_write_query(
            sqlQuery,
            dbConn,
            log
        )

    log.debug(
        'completed the ``create_binary_table_spectrum_rows_in_database`` function')
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
