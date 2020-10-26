#!/usr/bin/env python
# encoding: utf-8
"""
*EXport raw images reqired to make reduced image*

:Author:
    David Young

:Date Created:
    May 13, 2014

Usage:
    pm_export_raw_images_used_to_create_final_image -f <filename> -e <exportDirectory> -s <pathToSettingsFile>

Options:
    -h, --help             show this help message
    -s, --settingsFile     path to the settings file
    -f, --fileName         filename of the final image
    -e, --exportDirectory  path to the directory to export the raw files to
"""
from __future__ import print_function
################# GLOBAL IMPORTS ####################
import sys
import os
import shutil
from docopt import docopt
from dryxPython import logs as dl
from dryxPython import commonutils as dcu
from fundamentals import tools
import dryxPython.mysql as dms
import dryxPython.csvtools as dct


def main(arguments=None):
    """
    *The main function used when ``export_raw_images_used_to_create_final_image.py`` is run as a single script from the cl, or when installed as a cl command*
    """
    su = tools(
        arguments=arguments,
        docString=__doc__,
        logLevel="WARNING"
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
        '--- STARTING TO RUN THE export_raw_images_used_to_create_final_image.py AT %s' %
        (startTime,))

    # call the worker function
    export_raw_images_used_to_create_final_image(
        log=log,
        dbConn=dbConn,
        pathToExportFolder=exportDirectory,
        fileName=filename
    )

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info(
        '-- FINISHED ATTEMPT TO RUN THE export_raw_images_used_to_create_final_image.py AT %s (RUNTIME: %s) --' %
        (endTime, runningTime, ))

    return


###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# LAST MODIFIED : May 13, 2014
# CREATED : May 13, 2014
# AUTHOR : DRYX
def export_raw_images_used_to_create_final_image(
        log,
        dbConn,
        pathToExportFolder,
        fileName):
    """
    *export raw images used to create final image*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``pathToExportFolder`` -- export folder
        - ``fileName`` -- name of reduced file

    **Return:**
        - None

    .. todo::

    """
    log.debug(
        'completed the ````export_raw_images_used_to_create_final_image`` function')

    print("## %(fileName)s" % locals())

    # WORKOUT WHICH TABLE THE FILE IS FOUND IN
    found = 0
    databaseTablenames = [
        "sofi_imaging", "sofi_spectra", "efosc_imaging", "efosc_spectra"]
    for table in databaseTablenames:
        sqlQuery = """
            select prov1, ESO_INS_FILT1_NAME from %(table)s where currentFilename = "%(fileName)s";
        """ % locals()
        match = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            log=log
        )
        if len(match):
            databaseTable = table
            log.info(
                'file found in the `%(databaseTable)s` table' % locals())
            found = 1
            break
    if found == 0:
        log.error('file `%(fileName)s` not found in database' % locals())
        return
    prov1 = match[0]["prov1"]
    ffilter = match[0]["ESO_INS_FILT1_NAME"]

    # SETUP THE EXPORT FOLDER
    if not pathToExportFolder:
        thisFolder = fileName.replace(".fits", "")
    else:
        thisFolder = pathToExportFolder
    dcu.dryx_mkdir(
        log,
        directoryPath=thisFolder
    )

    imagesToExport = []

    # export raw images in prov keywords
    export_raw_science_images(
        dbConn=dbConn,
        log=log,
        fileName=fileName,
        databaseTable=databaseTable,
        imagesToExport=imagesToExport
    )
    log.debug("""imagesToExport: `%(imagesToExport)s`""" % locals())

    export_flat_field_frames(
        dbConn=dbConn,
        log=log,
        fileName=fileName,
        databaseTable=databaseTable,
        prov1=prov1,
        ffilter=ffilter,
        imagesToExport=imagesToExport
    )
    log.debug("""imagesToExport: `%(imagesToExport)s`""" % locals())

    export_bias_frames(
        dbConn=dbConn,
        log=log,
        fileName=fileName,
        databaseTable=databaseTable,
        prov1=prov1,
        imagesToExport=imagesToExport
    )
    log.debug("""imagesToExport: `%(imagesToExport)s`""" % locals())

    if "spectra" in databaseTable:
        export_arc_frame(
            dbConn=dbConn,
            log=log,
            fileName=fileName,
            databaseTable=databaseTable,
            imagesToExport=imagesToExport
        )
        log.debug("""imagesToExport: `%(imagesToExport)s`""" % locals())

        export_spec_standards(
            dbConn=dbConn,
            log=log,
            fileName=fileName,
            databaseTable=databaseTable,
            imagesToExport=imagesToExport
        )
        log.debug("""imagesToExport: `%(imagesToExport)s`""" % locals())

    for image in imagesToExport:
        basename = os.path.basename(image)
        try:
            log.debug("attempting to copy file here")
            shutil.copyfile(image,
                            thisFolder + "/%(basename)s" % locals())
        except Exception as e:
            log.error("could not copy file here - failed with this error: %s " %
                      (str(e),))

    log.debug(
        'completed the ``export_raw_images_used_to_create_final_image`` function')
    return None

# use the tab-trigger below for new function
# LAST MODIFIED : November 10, 2014
# CREATED : November 10, 2014
# AUTHOR : DRYX
# copy usage method(s) into function below and select the following snippet from the command palette:
# x-setup-worker-function-parameters-from-usage-method


def export_raw_science_images(
        dbConn,
        log,
        fileName,
        databaseTable,
        imagesToExport):
    """
    *export raw images*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger

    **Return:**
        - None

    .. todo::

        - @review: when complete, clean export_raw_science_images function
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract function to another module
    """
    log.debug('starting the ``export_raw_science_images`` function')

    # FIND ALL THE PROV KEYWORDS
    sqlQuery = u"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%(databaseTable)s' AND column_name LIKE 'prov%%';
    """ % locals()
    rows = dms.execute_mysql_read_query(
        sqlQuery=sqlQuery,
        dbConn=dbConn,
        log=log
    )
    provKeywords = ""
    for row in rows:
        prov = row["COLUMN_NAME"]
        provKeywords = "%(provKeywords)s%(prov)s, " % locals()
    provKeywords = provKeywords[:-2]

    # FIND ALL THE PROV KEYWORD VALUE FOR FILENAME
    sqlQuery = u"""
        SELECT %(provKeywords)s FROM %(databaseTable)s where currentFilename = '%(fileName)s';
    """ % locals()
    rows = dms.execute_mysql_read_query(
        sqlQuery=sqlQuery,
        dbConn=dbConn,
        log=log
    )
    scienceFrames = ""
    for row in rows:
        for k, v in row.items():
            if v:
                scienceFrames = """ %(scienceFrames)s"%(v)s", """ % locals()
    scienceFrames = scienceFrames[:-2]

    # FIND ALL OF THE RAW SCIENCE FRAMES FOR THIS OBJECT
    sqlQuery = """
        select currentFilepath, currentFilename,  eso_tpl_nexp, ra, decl, origfile, object, eso_tpl_expno from %(databaseTable)s where currentFilename in (%(scienceFrames)s) order by origfile;
    """ % locals()
    rows = dms.execute_mysql_read_query(
        sqlQuery=sqlQuery,
        dbConn=dbConn,
        log=log
    )

    for row in rows:
        imagesToExport.append(row["currentFilepath"])

    sqlQuery = sqlQuery.replace("currentFilepath, ", "")
    csvOutput = dct.sqlquery_to_csv_file.sqlquery_to_csv_file(
        dbConn=dbConn,
        log=log,
        sqlQuery=sqlQuery,
        csvType="human",  # human or machine
        csvTitle="Raw Frames used in creating %(fileName)s" % locals(),
        csvFilename="filename",
        returnFormat="plainText"  # plainText | webpageDownload | webpageView
    )
    print("""\n```
    %(csvOutput)s
```""" % locals())

    log.debug('completed the ``export_raw_science_images`` function')
    return None

# LAST MODIFIED : November 10, 2014
# CREATED : November 10, 2014
# AUTHOR : DRYX


def export_bias_frames(
        dbConn,
        log,
        fileName,
        databaseTable,
        prov1,
        imagesToExport):
    """
    *export bias frames*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``fileName`` -- fileName
        - ``databaseTable`` -- databaseTable
        - ``prov1`` -- prov1
        - ``imagesToExport`` -- imagesToExport

    **Return:**
        - None

    .. todo::

    """
    log.debug('starting the ``export_bias_frames`` function')

    if "sofi" in databaseTable:
        log.debug("No bias frames required for SOFI")
        return
    snippet = prov1[6:25]
    filterKeyword = "ESO_INS_FILT1_NAME"
    from datetime import datetime
    from datetime import timedelta
    thisDate = datetime.strptime(snippet, '%Y-%m-%dT%H:%M:%S')
    from time import strftime

    # bias frames found in imaging tables
    databaseTable = databaseTable.replace("spectra", "imaging")

    newRows = []
    if "efosc" in databaseTable:
        negative = 1
        count = 1
        next = snippet
        print(snippet)
        while not len(newRows):
            if next == snippet:
                next = thisDate - timedelta(days=0.5)
                next = next.strftime('%Y-%m-%d')
            elif negative:
                next = thisDate - timedelta(days=count) - timedelta(days=0.5)
                next = next.strftime('%Y-%m-%d')
                negative = 0
            else:
                next = thisDate + timedelta(days=count) - timedelta(days=0.5)
                next = next.strftime('%Y-%m-%d')
                negative = 1
                count += 1
            sqlQuery = """
                select currentFilepath, currentFilename, ORIGFILE, ESO_TPL_PRESEQ, %(filterKeyword)s , ra, decl, currentFilename, object from %(databaseTable)s  where (currentFilename like "%%%(next)s%%") and (OBJECT = "BIAS") order by origfile;
            """ % locals()
            log.debug("""sqlQuery: `%(sqlQuery)s`""" % locals())
            newRows = dms.execute_mysql_read_query(
                sqlQuery=sqlQuery,
                dbConn=dbConn,
                log=log
            )
            for row in newRows:
                imagesToExport.append(row["currentFilepath"])

        sqlQuery = sqlQuery.replace("currentFilepath, ", "")
        csvOutput = dct.sqlquery_to_csv_file.sqlquery_to_csv_file(
            dbConn=dbConn,
            log=log,
            sqlQuery=sqlQuery,
            csvType="human",  # human or machine
            csvTitle="Bias Frames used in creating %(fileName)s" % locals(),
            csvFilename="filename",
            # plainText | webpageDownload | webpageView
            returnFormat="plainText"
        )
        print("""\n```
        %(csvOutput)s
    ```""" % locals())

    log.debug('completed the ``export_bias_frames`` function')
    return None


# LAST MODIFIED : November 10, 2014
# CREATED : November 10, 2014
# AUTHOR : DRYX
def export_flat_field_frames(
        dbConn,
        log,
        fileName,
        databaseTable,
        prov1,
        ffilter,
        imagesToExport):
    """
    *export flat field frames*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``fileName`` -- fileName
        - ``databaseTable`` -- databaseTable
        - ``prov1`` -- prov1
        - ``ffilter`` -- ffilter
        - ``imagesToExport`` -- imagesToExport

    **Return:**
        - None

    .. todo::

    """
    log.debug('starting the ``export_flat_field_frames`` function')

    if "sofi" in databaseTable:
        snippet = prov1[5:24]
        filterKeyword = "ESO_INS_FILT1_ID"
    else:
        snippet = prov1[6:25]
        filterKeyword = "ESO_INS_FILT1_NAME"
    from datetime import datetime
    from datetime import timedelta
    thisDate = datetime.strptime(snippet, '%Y-%m-%dT%H:%M:%S')
    from time import strftime

    rows = []
    negative = 1
    count = 1
    next = snippet
    print(snippet)
    while not len(rows):
        if next == snippet:
            next = thisDate - timedelta(days=0.5)
            next = next.strftime('%Y-%m-%d')
        elif negative:
            next = thisDate - timedelta(days=count) - timedelta(days=0.5)
            next = next.strftime('%Y-%m-%d')
            negative = 0
        else:
            next = thisDate + timedelta(days=count) - timedelta(days=0.5)
            next = next.strftime('%Y-%m-%d')
            negative = 1
            count += 1
        sqlQuery = """
            select currentFilepath, currentFilename, ORIGFILE, ESO_TPL_PRESEQ, %(filterKeyword)s , ra, decl, currentFilename, object from %(databaseTable)s  where (currentFilename like "%%%(next)s%%") and (ESO_TPL_PRESEQ like "%%flat%%" or OBJECT = "FLAT") and %(filterKeyword)s = "%(ffilter)s" order by origfile;
        """ % locals()
        log.debug("""sqlQuery: `%(sqlQuery)s`""" % locals())
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            log=log
        )
        for row in rows:
            imagesToExport.append(row["currentFilepath"])

    sqlQuery = sqlQuery.replace("currentFilepath, ", "")
    csvOutput = dct.sqlquery_to_csv_file.sqlquery_to_csv_file(
        dbConn=dbConn,
        log=log,
        sqlQuery=sqlQuery,
        csvType="human",  # human or machine
        csvTitle="Flat Frames used in creating %(fileName)s" % locals(),
        csvFilename="filename",
        returnFormat="plainText"  # plainText | webpageDownload | webpageView
    )
    print("""\n```
    %(csvOutput)s
```""" % locals())

    log.debug('completed the ``export_flat_field_frames`` function')
    return None

# LAST MODIFIED : November 10, 2014
# CREATED : November 10, 2014
# AUTHOR : DRYX


def export_arc_frame(
    dbConn,
    log,
    fileName,
    databaseTable,
    imagesToExport
):
    """
    *export arc frame*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``fileName`` -- fileName
        - ``databaseTable`` -- databaseTable
        - ``imagesToExport`` -- imagesToExport

    **Return:**
        - None

    .. todo::

    """
    log.debug('starting the ``export_arc_frame`` function')

    sqlQuery = u"""
        select arc from %(databaseTable)s where currentFilename = "%(fileName)s" 
    """ % locals()
    rows = dms.execute_mysql_read_query(
        sqlQuery=sqlQuery,
        dbConn=dbConn,
        log=log
    )
    arc = rows[0]["arc"]

    log.debug("""arc: `%(arc)s`""" % locals())

    export_raw_science_images(
        dbConn=dbConn,
        log=log,
        fileName=arc,
        databaseTable=databaseTable,
        imagesToExport=imagesToExport
    )

    log.debug('completed the ``export_arc_frame`` function')
    return None

# use the tab-trigger below for new function
# LAST MODIFIED : November 10, 2014
# CREATED : November 10, 2014
# AUTHOR : DRYX
# copy usage method(s) into function below and select the following snippet from the command palette:
# x-setup-worker-function-parameters-from-usage-method


def export_spec_standards(
        dbConn,
        log,
        fileName,
        databaseTable,
        imagesToExport):
    """
    *export standards*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        # copy usage method(s) here and select the following snippet from the command palette:
        # x-setup-docstring-keys-from-selected-usage-options

    **Return:**
        - None

    .. todo::

        - @review: when complete, clean export_spec_standards function
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract function to another module
    """
    log.debug('starting the ``export_spec_standards`` function')

    if "sofi" in databaseTable:
        sqlQuery = u"""
            select SENSFUN, SENSPHOT from %(databaseTable)s where currentFilename = "%(fileName)s" 
        """ % locals()
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            log=log
        )
        SENSFUN = rows[0]["SENSFUN"]
        SENSPHOT = rows[0]["SENSPHOT"]
    else:
        sqlQuery = u"""
            select SENSFUN from %(databaseTable)s where currentFilename = "%(fileName)s" 
        """ % locals()
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            log=log
        )
        SENSFUN = rows[0]["SENSFUN"]
        SENSPHOT = None

    if SENSFUN:
        export_raw_science_images(
            dbConn=dbConn,
            log=log,
            fileName=SENSFUN,
            databaseTable=databaseTable,
            imagesToExport=imagesToExport
        )

    if SENSPHOT:
        export_raw_science_images(
            dbConn=dbConn,
            log=log,
            fileName=SENSPHOT,
            databaseTable=databaseTable,
            imagesToExport=imagesToExport
        )

    log.debug('completed the ``export_spec_standards`` function')
    return None

# use the tab-trigger below for new function
# xt-def-with-logger


if __name__ == '__main__':
    main()
