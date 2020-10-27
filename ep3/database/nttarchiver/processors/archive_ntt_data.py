#!/usr/local/bin/python
# encoding: utf-8
"""
*Archive the FITS files located in the NTT dropboax that have just been ingested into the PESSTO Marshall database*

:Author:
    David Young

Usage:
    pm_archive_ntt_data -s <pathToSettingsFile>
    pm_archive_ntt_data --host=<host> --user=<user> --passwd=<passwd> --dbName=<dbName> -p <pathToArchiveRoot> -d <pathToDropboxFolder>

Options:
    -h, --help                  show this help message
    -s, --settingsFile          path to the settings file
    -p, --pathToArchiveRoot     path to the root of the nested folder archive
    -d, --pathToDropboxRoot     path to the marshall dropbox folder
    --host=<host>               database host
    --user=<user>               database user
    --passwd=<passwd>           database password
    --dbName=<dbName>           database name
"""
from builtins import str
import sys
import os
from docopt import docopt

def main(arguments=None):
    """
    *The main function used when ``archive_ntt_data.py`` is run as a single script from the cl, or when installed as a cl command*
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
        '--- STARTING TO RUN THE archive_ntt_data.py AT %s' %
        (startTime,))

    # call the worker function
    if "settings" in locals() and settings:
        archive_ntt_data(
            pathToDropboxRoot=settings["nttarchiver"][
                "path to dropbox folder"],
            pathToArchiveRoot=settings["nttarchiver"][
                "path to archive root folder"],
            dbConn=dbConn,
            log=log
        )
    elif "host" in locals() and host:
        archive_ntt_data(
            pathToArchiveRoot=pathToArchiveRoot,
            pathToDropbox=pathToDropboxFolder,
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
        '-- FINISHED ATTEMPT TO RUN THE archive_ntt_data.py AT %s (RUNTIME: %s) --' %
        (endTime, runningTime, ))

    return

###################################################################
# CLASSES                                                         #
###################################################################

###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# LAST MODIFIED : October 1, 2013
# CREATED : October 1, 2013
# AUTHOR : DRYX

def archive_ntt_data(
        pathToArchiveRoot,
        pathToDropboxRoot,
        dbConn,
        log):
    """
    *move files from the marshall dropbox to the local archive*

    **Key Arguments**

    - ``pathToArchiveRoot`` -- path to the root of the nested folder archive
    - ``pathToDropboxRoot`` -- path to the marshall dropbox folder
    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    

    **Return**

    - None
    

    .. todo::
    """
    ## STANDARD LIB ##
    import os
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##
    from fundamentals.mysql import readquery, writequery

    log.debug('starting the ``archive_ntt_data`` function')

    # Database tables containing the data to archive
    tables = ["efosc_imaging", "efosc_spectra",
              "sofi_imaging", "sofi_spectra", "corrupted_files"]

    for table in tables:
        # select the metadata of the rows/files that need archived
        sqlQuery = """SELECT primaryId, filename, currentFilename, updatedFileName, filePath, currentFilePath, updatedFilePath, archivePath from %s where archivedLocally = 0 and updatedFilePath is NUll and filename not like "%%_sb.fits" """ % (
            table,)
        rows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn
        )
        log.debug('sqlQuery: %s' % (sqlQuery,))

        # Determine the path for the file to be archived to
        for row in rows:
            if not row["updatedFilePath"]:
                if row["archivePath"]:
                    updatedFilePath = pathToArchiveRoot + \
                        "/" + row["archivePath"] + row["filename"]
                    updatedFileName = row["filename"]
                else:
                    log.warning(
                        'archive path not set in database for file %s' % (row["filename"],))
                    continue

                # add the archive path to the databas row
                sqlQuery = """
                    update %s set updatedFileName = "%s", updatedFilePath = "%s" where primaryId = %s
                """ % (table, updatedFileName, updatedFilePath, row["primaryId"])
                writequery(
                    log=log,
                    sqlQuery=sqlQuery,
                    dbConn=dbConn,
                )

        # Select the same file, but now those with an updated filepath in the
        # database
        sqlQuery = """SELECT filename, primaryId, currentFilePath, updatedFilePath from %s where (archivedLocally = 0 and updatedFilePath is not NUll and filename not like "%%_sb.fits") """ % (
            table,)
        rows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn
        )

        for row in rows:
            if "_sb.fits" in row["filename"]:
                continue

            moved = False
            trashed = False
            basename = os.path.basename(row["updatedFilePath"])
            dirOfFile = row["updatedFilePath"].replace(basename, "")
            directoryPath = dirOfFile
            log.debug('dirOfFile: %s' % (dirOfFile,))
            trash = pathToArchiveRoot + "/trash"
            # Recursively create missing directories and trash folder
            if not os.path.exists(dirOfFile):
                os.makedirs(dirOfFile)
            if not os.path.exists(trash):
                os.makedirs(trash)

            # determine where to put the file in question
            fileExists = False
            try:
                with open(row["updatedFilePath"]):
                    fileExists = True
            except IOError:
                pass

            if fileExists:
                log.warning(
                    '%s already exists in the database -- duplicate left in marshall dropbox:' %
                    (row["currentFilePath"],))
                moved = True
                trashed = True
                # try:
                #     os.rename(row["currentFilePath"], trash + "/" + basename)
                # except OSError:
                #     pass

            ioerror = False
            if not trashed:
                try:
                    log.debug("attempting to rename file %s to %s" %
                              (row["currentFilePath"], row["updatedFilePath"]))
                    os.rename(row["currentFilePath"], row["updatedFilePath"])
                    moved = True
                except OSError:
                    ioerror = True

            if ioerror:
                try:
                    if not os.path.exists(trash):
                        os.makedirs(trash)
                    log.debug("attempting to rename file %s to %s" %
                              (row["currentFilePath"], trash + "/" + basename))
                    os.rename(row["currentFilePath"], trash + "/" + basename)
                    moved = True
                except Exception as e:
                    log.error(
                        "could not move %s to the trash - failed with this error: %s " %
                        (row["currentFilePath"], str(e),))

            # update database to reflect any move of file
            if moved:
                sqlQuery = """
                    update %s set archivedLocally = 1, currentFilename = updatedFilename, currentFilepath = updatedFilepath, updatedFilename = null, updatedFilepath = null where primaryId = %s
                """ % (table, row["primaryId"])

                writequery(
                    log=log,
                    sqlQuery=sqlQuery,
                    dbConn=dbConn,
                )
                log.info('file %s has been archived' %
                         (row["currentFilePath"],))

    # CREATE EMPTY BINARY TABLE FILES
    tables = ["efosc_spectra", "sofi_spectra", ]
    for table in tables:
        sqlQuery = """SELECT primaryId, filename, currentFilename, updatedFileName, filePath, currentFilePath, updatedFilePath, archivePath from %s where archivedLocally = 0 and filename like "%%_sb.fits" """ % (
            table,)
        rows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn
        )

        for row in rows:
            currentFilepath = pathToArchiveRoot + \
                "/" + row["archivePath"] + row["filename"]
            currentFilename = row["filename"]

            sqlQuery = """
                    update %s set currentFilename = "%s", currentFilepath = "%s" where primaryId = %s
                """ % (table, currentFilename, currentFilepath, row["primaryId"])

            writequery(
                log=log,
                sqlQuery=sqlQuery,
                dbConn=dbConn,
            )

            pathToWriteFile = currentFilepath
            try:
                log.debug(
                    "attempting to open the file %s" % (pathToWriteFile,))
                writeFile = open(pathToWriteFile, 'w')
            except Exception as e:
                message = 'could not open the file %s' % (pathToWriteFile,)
                log.critical(message)
                raise IOError(message)

            writeFile.close()

            sqlQuery = """
                    update %s set archivedLocally = 1, updatedFilename = null, updatedFilepath = null where primaryId = %s
                """ % (table, row["primaryId"])
            writequery(
                log=log,
                sqlQuery=sqlQuery,
                dbConn=dbConn,
            )

    # a double check that empty binary files have been created
    tables = ["efosc_spectra", "sofi_spectra", ]
    for table in tables:
        sqlQuery = """SELECT primaryId, filename, currentFilename, updatedFileName, filePath, currentFilePath, updatedFilePath, archivePath from %s where archivedLocally = 1 and filename like "%%_sb.fits" """ % (
            table,)
        rows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn
        )

        for row in rows:
            currentFilepath = row["currentFilePath"]

            pathToReadFile = currentFilepath
            fileExists = True
            try:
                log.debug("attempting to open the file %s" % (pathToReadFile,))
                readFile = open(pathToReadFile, 'r')
                readFile.close()
            except IOError as e:
                message = 'could not open the file %s' % (pathToReadFile,)
                fileExists = False

            if not fileExists:
                pathToWriteFile = currentFilepath
                try:
                    log.debug(
                        "attempting to open the file %s" % (pathToWriteFile,))
                    writeFile = open(pathToWriteFile, 'w')
                    writeFile.close()
                except IOError as e:
                    message = 'could not open the file %s' % (pathToWriteFile,)
                    log.critical(message)
                    raise IOError(message)

    log.debug('completed the ``archive_ntt_data`` function')
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
