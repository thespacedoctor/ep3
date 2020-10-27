#!/usr/bin/env python
# encoding: utf-8
"""
*Export data marked as phase III for the marshall archive*

:Author:
    David Young

Usage:
    pm_export_phase_III_data -s <pathToSettingsFile> -n <numberOfRelease> -e <pathToExportFolder> [-d <spectra_imaging_or_all>] [-i <efosc_sofi_or_all>]

Options:
    -h, --help                show this help message
    -s, --settingsFile        path to the settings file
    -n, --ssdr                number of the release
    -e, --pathToExportFolder  path to export the data to
    -d, --dataType            spectra | imaging | all
    -i, --instrument          efosc | sofi | all
"""
from __future__ import print_function
import sys
import os
from docopt import docopt
from fundamentals.mysql import readquery
import shutil

def main(arguments=None):
    """
    *The main function used when ``export_phase_III_data.py`` is run as a single script from the cl, or when installed as a cl command*
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
        '--- STARTING TO RUN THE export_phase_III_data.py AT %s' %
        (startTime,))

    if "spectra_imaging_or_all" not in locals() or spectra_imaging_or_all is None:
        dataType = "all"
    else:
        dataType = spectra_imaging_or_all
    if "efosc_sofi_or_all" not in locals() or efosc_sofi_or_all is None:
        instrument = "all"
    else:
        instrument = efosc_sofi_or_all

    # call the worker function
    # x-if-settings-or-database-credientials
    export_phase_III_data(
        log=log,
        dbConn=dbConn,
        ssdr=numberOfRelease,
        exportFolder=pathToExportFolder,
        dataType=dataType,
        instrument=instrument
    )

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info(
        '-- FINISHED ATTEMPT TO RUN THE export_phase_III_data.py AT %s (RUNTIME: %s) --' %
        (endTime, runningTime, ))

    return

###################################################################
# CLASSES                                                         #
###################################################################

###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# LAST MODIFIED : March 27, 2014
# CREATED : March 27, 2014
# AUTHOR : DRYX
# copy usage method(s) into function below and select the following snippet from the command palette:
# x-setup-worker-function-parameters-from-usage-method

def export_phase_III_data(
    log,
    dbConn,
    ssdr,
    exportFolder,
    dataType,
    instrument,
):
    """
    *export_phase_III_data*

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

    # Recursively create missing directories
    if not os.path.exists(exportFolder):
        os.makedirs(exportFolder)

    if dataType == "all" or dataType == "imaging":
        thisExportFolder = "%(exportFolder)s/imaging" % locals()
        try:
            shutil.rmtree(thisExportFolder)
        except:
            pass
        if not os.path.exists(thisExportFolder):
            os.makedirs(thisExportFolder)
        if instrument == "efosc" or instrument == "all":
            thisExportFolder = "%(exportFolder)s/imaging/efosc" % locals()
            if not os.path.exists(thisExportFolder):
                os.makedirs(thisExportFolder)
            sqlQuery = """
                select currentFilename, currentFilepath from efosc_imaging where esoPhaseIII = 1 and data_rel = "ssdr%(ssdr)s"
            """ % locals()
            rows = readquery(
                log=log,
                sqlQuery=sqlQuery,
                dbConn=dbConn
            )
            for row in rows:
                print(row["currentFilepath"])
                scr = row["currentFilepath"]
                name = row["currentFilename"]
                des = "%(thisExportFolder)s/%(name)s" % locals()
                shutil.copyfile(scr, des)

        if instrument == "sofi" or instrument == "all":
            thisExportFolder = "%(exportFolder)s/imaging/sofi" % locals()
            try:
                shutil.rmtree(thisExportFolder)
            except:
                pass
            if not os.path.exists(thisExportFolder):
                os.makedirs(thisExportFolder)
            sqlQuery = """
                select currentFilename, currentFilepath from sofi_imaging where esoPhaseIII = 1 and data_rel = "ssdr%(ssdr)s"
            """ % locals()
            rows = readquery(
                log=log,
                sqlQuery=sqlQuery,
                dbConn=dbConn
            )
            for row in rows:
                print(row["currentFilepath"])
                scr = row["currentFilepath"]
                name = row["currentFilename"]
                des = "%(thisExportFolder)s/%(name)s" % locals()
                shutil.copyfile(scr, des)

    if dataType == "all" or dataType == "spectra":
        thisExportFolder = "%(exportFolder)s/spectra" % locals()
        try:
            shutil.rmtree(thisExportFolder)
        except:
            pass
        if not os.path.exists(thisExportFolder):
            os.makedirs(thisExportFolder)
        if instrument == "efosc" or instrument == "all":
            thisExportFolder = "%(exportFolder)s/spectra/efosc" % locals()
            if not os.path.exists(thisExportFolder):
                os.makedirs(thisExportFolder)
            sqlQuery = """
                select currentFilename, currentFilepath from efosc_spectra where esoPhaseIII = 1 and data_rel = "ssdr%(ssdr)s" and (currentFilename like "%%_sb.fits" or currentFilename like "%%_si.fits")
            """ % locals()
            rows = readquery(
                log=log,
                sqlQuery=sqlQuery,
                dbConn=dbConn
            )

            for row in rows:
                print(row["currentFilepath"])
                scr = row["currentFilepath"]
                name = row["currentFilename"]
                des = "%(thisExportFolder)s/%(name)s" % locals()
                shutil.copyfile(scr, des)

        if instrument == "sofi" or instrument == "all":
            thisExportFolder = "%(exportFolder)s/spectra/sofi" % locals()
            try:
                shutil.rmtree(thisExportFolder)
            except:
                pass
            if not os.path.exists(thisExportFolder):
                os.makedirs(thisExportFolder)
            sqlQuery = """
                select currentFilename, currentFilepath from sofi_spectra where esoPhaseIII = 1 and data_rel = "ssdr%(ssdr)s" and (currentFilename like "%%_sb.fits" or currentFilename like "%%_si.fits")
            """ % locals()
            rows = readquery(
                log=log,
                sqlQuery=sqlQuery,
                dbConn=dbConn
            )
            for row in rows:
                print(row["currentFilepath"])
                scr = row["currentFilepath"]
                name = row["currentFilename"]
                des = "%(thisExportFolder)s/%(name)s" % locals()
                shutil.copyfile(scr, des)

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
