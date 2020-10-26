#!/usr/local/bin/python
# encoding: utf-8
"""
*Correct EFOSC Spectra EFFRON when set to null*

:Author:
    David Young

:Date Created:
    August 30, 2016

Usage:
    correct_efosc_effron -s <pathToSettingsFile>

Options:
    -h, --help          show this help message
    -s, --settingsFile  path to the settings file
"""
################# GLOBAL IMPORTS ####################
import sys
import os
import re
import math
from docopt import docopt
from dryxPython import logs as dl
from dryxPython import commonutils as dcu


def main(arguments=None):
    """
    *The main function used when ``correct_efosc_effron.py`` is run as a single script from the cl, or when installed as a cl command*
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
        '--- STARTING TO RUN THE correct_efosc_effron.py AT %s' %
        (startTime,))

    # call the worker function
    # x-if-settings-or-database-credientials
    correct_efosc_effron(
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
        '-- FINISHED ATTEMPT TO RUN THE correct_efosc_effron.py AT %s (RUNTIME: %s) --' %
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


def correct_efosc_effron(
        log,
        dbConn,
):
    """
    *correct_efosc_effron*

    **Key Arguments:**
        # copy usage method(s) here and select the following snippet from the command palette:
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
    import pyfits as pf
    ## LOCAL APPLICATION ##
    import dryxPython.mysql as dms

    # add nelem to 1D spectra rows
    sqlQuery = """
        select effron, primaryId, bi_flag, ff_flag, eso_det_out1_ron from efosc_spectra where effron is null and esoPhaseIII = 1 and lock_row = 0;
    """
    rows = dms.execute_mysql_read_query(
        sqlQuery=sqlQuery,
        dbConn=dbConn,
        log=log
    )

    rebias = re.compile(r'.*?((bias|masterb).*)')
    reflat = re.compile(r'.*?((nflat|masterf).*)')

    for row in rows:
        nflat = None
        nbias = None
        bias = rebias.sub("\g<1>", row["bi_flag"])
        flat = reflat.sub("\g<1>", row["ff_flag"])
        sqlQuery = """
            select currentFilename, ncombine from efosc_imaging where currentFilename like "%(bias)s%%" limit 1;
        """ % locals()
        bb = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            log=log
        )
        for b in bb:
            nbias = b["ncombine"]

        sqlQuery = """
            select currentFilename, ncombine from efosc_spectra where currentFilename like "%(flat)s%%" limit 1;
        """ % locals()
        ff = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            log=log
        )
        for f in ff:
            nflat = f["ncombine"]

        if nbias and nflat:
            ron = row["eso_det_out1_ron"]
            effron = ron * math.sqrt(1. + 1. / nflat + 1. / nbias)
            primaryId = row["primaryId"]

            sqlQuery = u"""
                update efosc_spectra set effron = %(effron)s where primaryId = %(primaryId)s  
            """ % locals()
            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=dbConn,
                log=log,
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
