#!/usr/local/bin/python
# encoding: utf-8
"""
*Open the spectra fits files and calculate the nelem values for the files*

:Author:
    David Young

Usage:
    pm_add_nelem_keyword_value_to_database -s <pathToSettingsFile>

Options:
    -h, --help          show this help message
    -s, --settingsFile  path to the settings file
"""
from __future__ import print_function
import sys
import os
from docopt import docopt

def main(arguments=None):
    """
    *The main function used when ``add_nelem_keyword_value_to_database.py`` is run as a single script from the cl, or when installed as a cl command*
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

    ## START LOGGING ##
    startTime = dcu.get_now_sql_datetime()
    log.info(
        '--- STARTING TO RUN THE add_nelem_keyword_value_to_database.py AT %s' %
        (startTime,))

    # call the worker function
    # x-if-settings-or-database-credientials
    add_nelem_keyword_value_to_database(
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
        '-- FINISHED ATTEMPT TO RUN THE add_nelem_keyword_value_to_database.py AT %s (RUNTIME: %s) --' %
        (endTime, runningTime, ))

    return

###################################################################
# CLASSES                                                         #
###################################################################

###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# LAST MODIFIED : ggd
# CREATED : ggd
# AUTHOR : DRYX
# copy usage method(s) into function below and select the following snippet from the command palette:
# x-setup-worker-function-parameters-from-usage-method

def add_nelem_keyword_value_to_database(
        log,
        dbConn,
):
    """
    *add_nelem_keyword_value_to_database*

    **Key Arguments**

    # copy usage method(s) here and select the following snippet from the command palette:
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
    import pyfits as pf
    ## LOCAL APPLICATION ##
    from fundamentals.mysql import readquery, writequery

    tables = ["sofi_spectra", "efosc_spectra"]
    for table in tables:

        # add nelem to 1D spectra rows
        sqlQuery = """
            select currentFilename, currentFilepath, primaryId from %s where nelem is null and esoPhaseIII = 1 and currentFilename not like "%%sb%%" and currentFilename not like "%%si%%" and updatedFilename is null  and lock_row = 0;
        """ % (table,)
        rows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn
        )

        for row in rows:
            log.debug('row["currentFilepath"]: %s' % (row["currentFilepath"],))
            oneDHduList = pf.open(row["currentFilepath"])
            oneDFitsData = oneDHduList[0].data
            oneDHduList.close()
            nelem = len(oneDFitsData[1, 0])
            log.debug('nelem: %s' % (nelem,))
            primaryId = row["primaryId"]

            sqlQuery = """
                update %(table)s set nelem = %(nelem)s where primaryId = %(primaryId)s  and lock_row = 0
            """ % locals()
            writequery(
                log=log,
                sqlQuery=sqlQuery,
                dbConn=dbConn,
            )

        # add nelem to associated binary table spectra rows
        sqlQuery = """
            select binary_table_associated_spectrum_id, primaryId from %(table)s where nelem is null and esoPhaseIII = 1 and currentFilename like "%%sb%%"  and updatedFilename is null  and lock_row = 0;
        """ % locals()
        rows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn
        )
        for row in rows:
            primaryId = row["primaryId"]
            binary_table_associated_spectrum_id = row[
                "binary_table_associated_spectrum_id"]
            sqlQuery = """
                select nelem from %(table)s where primaryId = %(binary_table_associated_spectrum_id)s
            """ % locals()
            oneRow = readquery(
                log=log,
                sqlQuery=sqlQuery,
                dbConn=dbConn
            )
            for thisRow in oneRow:
                nelem = thisRow["nelem"]
                if not nelem:
                    log.warning(
                        "binary_table_associated_spectrum_id file with primaryId %(primaryId)s still has no `nelem` keyword set" % locals())
                    print(sqlQuery)
                    continue
                sqlQuery = """
                    update %(table)s set nelem = %(nelem)s where primaryId = %(primaryId)s  and lock_row = 0
                """ % locals()
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
