#!/usr/local/bin/python
# encoding: utf-8
"""
*Fix issues with the CRDER values introduced by the pipeline by extracting the correct values from the ASTROMET keyword value*

:Author:
    David Young

:Date Created:
    November 29, 2013

Usage:
    pm_fix_crder_astrometry_values -s <pathToSettingsFile>

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
    *The main function used when ``fix_crder_astrometry_values.py`` is run as a single script from the cl, or when installed as a cl command*
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
        '--- STARTING TO RUN THE fix_crder_astrometry_values.py AT %s' %
        (startTime,))

    # call the worker function
    fix_crder_astrometry_values(
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
        '-- FINISHED ATTEMPT TO RUN THE fix_crder_astrometry_values.py AT %s (RUNTIME: %s) --' %
        (endTime, runningTime, ))

    return

###################################################################
# CLASSES                                                         #
###################################################################

###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# LAST MODIFIED : November 29, 2013
# CREATED : November 29, 2013
# AUTHOR : DRYX


def fix_crder_astrometry_values(
    log,
    dbConn,
):
    """
    *fix_crder_astrometry_values*

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
    import re
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##
    import dryxPython.mysql as dms

    tables = ["efosc_imaging", "sofi_imaging"]

    for table in tables:

        sqlQuery = """
            select primaryId, ASTROMET from %s where ASTROMET is not null and astromet_rmsx is null;
        """ % (table,)
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            log=log
        )

        for row in rows:
            regex = re.compile(r'\s*')
            splitList = regex.split(row["ASTROMET"], 2)

            rmsx = float(splitList[0])
            rmsy = float(splitList[1])
            sources = int(splitList[2])
            astromet_error = (rmsx ** 2 + rmsy ** 2) ** 0.5
            crder1 = rmsx / 3600.
            crder2 = rmsy / 3600.

            if rmsx > 100:
                rmsx = 999
                crder1 = 9999
            if rmsy > 100:
                rmsy = 999
                crder2 = 9999

            sqlQuery = """
                update %s set astromet_rmsx = %s, astromet_rmsy = %s, astromet_sources = %s, astromet_error_arcsec = %s, crder1 = %s, crder2 = %s where primaryId = %s
            """ % (table, rmsx, rmsy, sources, astromet_error, crder1, crder2, row["primaryId"])
            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=dbConn,
                log=log
            )

            log.debug('splitList: %s' % (splitList,))

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
