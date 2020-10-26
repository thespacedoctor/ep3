#!/usr/local/bin/python
# encoding: utf-8
"""
*Export the workflow tables from the PESSTO Marshall as CSV files (human readable and machine readable)*

:Author:
    David Young

:Date Created:
    November 18, 2013

Usage:
    pm_export_csv_workflow_tables -s <pathToSettingsFile>

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
    *The main function used when ``export_csv_workflow_tables.py`` is run as a single script from the cl, or when installed as a cl command*
    """
    ########## IMPORTS ##########
    ## STANDARD LIB ##
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##
    import dryxPython.mysql as dms

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
        '--- STARTING TO RUN THE export_csv_workflow_tables.py AT %s' %
        (startTime,))

    # call the worker function
    # x-if-settings-or-database-credentials

    sqlQuery = """
        select distinct marshallWorkflowLocation from pesstoObjects;
    """
    rows = dms.execute_mysql_read_query(
        sqlQuery=sqlQuery,
        dbConn=dbConn,
        log=log
    )

    for row in rows:
        if row["marshallWorkflowLocation"] != "archive":
            export_csv_workflow_tables(
                log=log,
                dbConn=dbConn,
                marshallWorkflowLocation=row["marshallWorkflowLocation"],
                csvExportDirectoryPath=settings["exports"][
                    "path to csv export directory"],
                asciiExportDirectoryPath=settings["exports"][
                    "path to ascii export directory"]
            )

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info(
        '-- FINISHED ATTEMPT TO RUN THE export_csv_workflow_tables.py AT %s (RUNTIME: %s) --' %
        (endTime, runningTime, ))

    return

###################################################################
# CLASSES                                                         #
###################################################################

###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# LAST MODIFIED : November 18, 2013
# CREATED : November 18, 2013
# AUTHOR : DRYX
# copy usage method(s) into function below and select the following snippet from the command palette:
# x-setup-worker-function-parameters-from-usage-method


def export_csv_workflow_tables(
        log,
        dbConn,
        marshallWorkflowLocation,
        csvExportDirectoryPath,
        asciiExportDirectoryPath
):
    """
    *export_csv_workflow_tables*

    **Key Arguments:**
        # copy usage method(s) here and select the following snippet from the command palette:
        - ``log`` -- the logger
        - ``dbConn`` -- the database connection
        - ``marshallWorkflowLocation`` -- where in marshall workflow you would like to select the data from
        - ``csvExportDirectoryPath`` -- path to the directory to dump the csv files to.
        - ``asciiExportDirectoryPath`` -- path to the directory to dump the ascii files to.

    **Return:**
        - None

    .. todo::

        @review: when complete, clean worker function and add comments
        @review: when complete add logging
        @flagged: update this function to remove the view_object_contextual_data view
    """
    ################ > IMPORTS ################
    ## STANDARD LIB ##
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##
    import dryxPython.mysql as dms
    import csv

    sqlQuery = """
        select * from view_object_contextual_data where marshallWorkflowLocation = "%s"
    """ % (marshallWorkflowLocation,)
    rows = dms.execute_mysql_read_query(
        sqlQuery=sqlQuery,
        dbConn=dbConn,
        log=log
    )

    pathToCsvFile = csvExportDirectoryPath + "/pessto_marshall_" + \
        marshallWorkflowLocation.lower().replace(" ", "_") + "_objects.csv"

    startColumnList = [
        "masterName",
        "transientBucketId",
        "classifiedFlag",
        "marshallWorkflowLocation",
        "alertWorkflowLocation",
        "publicStatus",
        "dateAdded",
        "dateLastModified",
        "finderChartLocalUrl",
        "sdss_coverage",
        "qubClassification",
        "raDeg",
        "decDeg",
        "survey",
        "earliestDetection",
        "earliestDetectionMJD",
        "earliestDetectionMagnitude",
        "earliestDetectionMagnitudeError",
        "filter",
        "transientTypePrediction",
        "surveyObjectUrl",
        "referenceImageUrl",
        "targetImageUrl",
        "subtractedImageUrl",
        "tripletImageUrl",
        "finderImageUrl",
        "lastNonDetectionDate",
        "lastNonDetectionMJD",
        "spectralType",
        "transientRedshift"
    ]

    # Add new columns to the sorted column list
    columnList = startColumnList
    for k, v in rows[0].iteritems():
        if k not in columnList:
            columnList.append(k)

    # output the csv files
    with open(pathToCsvFile, 'wb') as csvFile:
        csvFileWriter = csv.writer(
            csvFile, dialect='excel', delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvFileWriter.writerow(columnList)

        for row in rows:
            rowList = []
            for k in columnList:
                rowList.append(row[k])
            csvFileWriter.writerow(rowList)

    csvFile.close()

    pathToWriteFile = asciiExportDirectoryPath + "/pessto_marshall_" + \
        marshallWorkflowLocation.lower().replace(" ", "_") + "_objects.dat"
    try:
        log.debug("attempting to open the file %s" % (pathToWriteFile,))
        writeFile = open(pathToWriteFile, 'w')
    except IOError, e:
        message = 'could not open the file %s' % (pathToWriteFile,)
        log.critical(message)
        raise IOError(message)

    headerRow = "| "
    for item in columnList:
        span = 35
        if "url" in item.lower():
            span = 110
        headerRow += " " + str(item).ljust(span, ' ') + "|"
    divider = "|" + (len(headerRow) - 2) * "-" + "|\n"
    writeFile.write(divider)
    writeFile.write(headerRow + "\n")
    writeFile.write(divider)

    for row in rows:
        thisRow = "| "
        for k in columnList:
            span = 35
            if "url" in k.lower():
                span = 110
            thisRow += " " + str(row[k]).ljust(span, ' ') + "|"
        writeFile.write(thisRow + "\n")
    writeFile.write(divider)

    writeFile.close()

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
