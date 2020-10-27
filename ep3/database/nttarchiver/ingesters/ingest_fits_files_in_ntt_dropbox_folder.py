#!/usr/local/bin/python
# encoding: utf-8
"""
*Read the FITS header data of the fits files in the designated NTT data dropbox folder and ingest this header data into the PESSTO Marshall database.__init__.py*

:Author:
    David Young

Usage:
    pm_ingest_fits_files_in_ntt_dropbox_folder -s <pathToSettingsFile>
    pm_ingest_fits_files_in_ntt_dropbox_folder --host=<host> --user=<user> --passwd=<passwd> --dbName=<dbName> --pathToDropboxFolder=<pathToDropboxFolder> --pathToArchiveRoot=<pathToArchiveRoot>

Options:
    -h, --help                                    show this help message
    -s, --settingsFile                            path to the settings file
    --host=<host>                                 database host
    --user=<user>                                 database user
    --passwd=<passwd>                             database password
    --dbName=<dbName>                             database name
    --pathToDropboxFolder=<pathToDropboxFolder>   path to folder where NTT data is dumped
    --pathToArchiveRoot=<pathToArchiveRoot>       path to root of the NTT data archive (nested folders)
"""
from __future__ import print_function
from builtins import str
import sys
import os
from docopt import docopt

def main(arguments=None):
    """
    *The main function used when ``ingest_fits_files_in_ntt_dropbox_folder.py`` is run as a single script from the cl, or when installed as a cl command*
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
        '--- STARTING TO RUN THE ingest_fits_files_in_ntt_dropbox_folder.py AT %s' %
        (startTime,))

    # call the worker function
    if "settings" in locals():
        ingest_fits_files_in_ntt_dropbox_folder(
            dbConn=dbConn,
            log=log,
            pathToDropboxRoot=settings["nttarchiver"][
                "path to dropbox folder"],
            pathToArchiveRoot=settings["nttarchiver"][
                "path to archive root folder"],
        )
    else:
        ingest_fits_files_in_ntt_dropbox_folder(
            dbConn=dbConn,
            log=log,
            pathToDropboxRoot=pathToDropboxFolder,
            pathToArchiveRoot=pathToArchiveRoot
        )

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info(
        '-- FINISHED ATTEMPT TO RUN THE ingest_fits_files_in_ntt_dropbox_folder.py AT %s (RUNTIME: %s) --' %
        (endTime, runningTime, ))

    return

###################################################################
# CLASSES                                                         #
###################################################################

###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# LAST MODIFIED : October 28, 2013
# CREATED : October 28, 2013
# AUTHOR : DRYX

def ingest_fits_files_in_ntt_dropbox_folder(
        dbConn,
        log,
        pathToDropboxRoot,
        pathToArchiveRoot):
    """
    *recursive ingest of fits files in directory*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    - ``pathToDropboxRoot`` -- path to the root directory from which to recusively ingest fits files
    - ``pathToArchiveRoot`` -- path to the root of the archive
    

    **Return**

    - None
    

    .. todo::

        @review: when complete, clean ingest_fits_files_in_ntt_dropbox_folder function
        @review: when complete add logging
        @review: when complete, decide whether to abstract function to another module
    """
    ## STANDARD LIB ##
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##

    log.debug(
        'completed the ````ingest_fits_files_in_ntt_dropbox_folder`` function')
    # TEST THE ARGUMENTS
    if not os.path.exists(pathToDropboxRoot):
        message = 'Please make sure "%s" exists' % (pathToDropboxRoot,)
        log.critical(message)
        raise IOError(message)

    ## VARIABLES ##
    directoryContents = dcu.get_recursive_list_of_directory_contents(
        log,
        baseFolderPath=pathToDropboxRoot,
        whatToList='files'  # [ 'files' | 'dirs' | 'all' ]
    )

    for path in directoryContents:

        if path[-5:] == ".fits":

            print(path)

            _ingest_fits_file(
                log=log,
                dbConn=dbConn,
                pathToFitsFile=path,
                pathToArchiveRoot=pathToArchiveRoot,
                pathToDropboxRoot=pathToDropboxRoot
            )

    log.debug(
        'completed the ``ingest_fits_files_in_ntt_dropbox_folder`` function')
    return None

# use the tab-trigger below for new function
# x-def-with-logger

###################################################################
# PRIVATE (HELPER) FUNCTIONS                                      #
###################################################################
# LAST MODIFIED : August 19, 2013
# CREATED : August 19, 2013
# AUTHOR : DRYX

def _ingest_fits_file(
        log,
        dbConn,
        pathToFitsFile,
        pathToArchiveRoot,
        pathToDropboxRoot):
    """
    *Ingest the header of a fits file, alongside it's name and path into the ntt_dropbox table*

    **Key Arguments**

    - ``log`` -- logger
    - ``dbConn`` -- the database connection
    - ``pathToFitsFile`` -- the path to the fits file we wish to ingest
    - ``pathToArchiveRoot`` -- path to the root of the archive
    - ``pathToDropboxRoot`` -- path to the root directory from which to recusively ingest fits files
    

    **Return**

    - ``None``
    

    .. todo::

        @review: when complete, clean _ingest_fits_file function
        @review: when complete add logging
        @review: when complete, decide whether to abstract function to another module
    """
    ## STANDARD LIB ##
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##
    import dryxPython.fitstools as dft
    from fundamentals.mysql import convert_dictionary_to_mysql_table

    log.debug('starting the ``ingest_fits_file`` function')

    # DIRECTORIES TO SKIP
    for dirr in ["trash", "duplicates"]:
        if dirr in pathToFitsFile:
            return

    # CHECK FILE IS NOT ALREADY IN DB
    basename = os.path.basename(pathToFitsFile)
    sqlQuery = """
    SELECT SUM(NUMBER) FROM ((SELECT
        count(*) number
    from
        efosc_imaging
    where
        filename = '%s' and updatedFilepath is null and currentFilepath is not null) UNION ALL (SELECT
        count(*) number
    from
        efosc_spectra
    where
        filename = '%s' and updatedFilepath is null and currentFilepath is not null) UNION ALL (SELECT
        count(*) number
    from
        sofi_imaging
    where
        filename = '%s' and updatedFilepath is null and currentFilepath is not null) UNION ALL (SELECT
        count(*) number
    from
        sofi_spectra
    where
        filename = '%s' and updatedFilepath is null and currentFilepath is not null)) as test""" % (basename, basename, basename, basename,)

    rows = readquery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn
    )

    # MOVE FILE TO DUPLICATES FOLDER IF ALREADY IN THE MARSHALL DATABASE
    if rows[0]["SUM(NUMBER)"] == 1:
        log.warning('file already in db - moving the to marshall dropbox duplicates folder: %s' %
                    (basename,))
        if not os.path.exists(pathToDropboxRoot + "/duplicates"):
            os.makedirs(pathToDropboxRoot + "/duplicates")
        try:
            log.debug("attempting to rename file %s to %s" %
                      (pathToFitsFile, pathToDropboxRoot + "/duplicates/" + basename))
            os.rename(pathToFitsFile, pathToDropboxRoot +
                      "/duplicates/" + basename)
        except Exception as e:
            log.error(
                "could not rename file %s to %s - failed with this error: %s " %
                (pathToFitsFile, pathToDropboxRoot + "/duplicates/" + basename, str(e),))
        return None

    # find the files in the database but not yet archived
    sqlQuery = """
    SELECT SUM(NUMBER) FROM ((SELECT
        count(*) number
    from
        efosc_imaging
    where
        filename = '%s' and (updatedFilepath is not null or currentFilepath is null)) UNION ALL (SELECT
        count(*) number
    from
        efosc_spectra
    where
        filename = '%s' and (updatedFilepath is not null or currentFilepath is null)) UNION ALL (SELECT
        count(*) number
    from
        sofi_imaging
    where
        filename = '%s' and (updatedFilepath is not null or currentFilepath is null)) UNION ALL (SELECT
        count(*) number
    from
        sofi_spectra
    where
        filename = '%s' and (updatedFilepath is not null or currentFilepath is null))) as test""" % (basename, basename, basename, basename,)
    rows = readquery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn
    )
    if rows[0]["SUM(NUMBER)"] == 1:
        log.warning('file already in db but not archived yet: %s' %
                    (basename,))
        return None
    else:
        log.debug('file not in db yet   : %s' % (basename,))

    ## VARIABLES ##
    try:
        with open(pathToFitsFile):
            pass
    except IOError:
        message = 'Please make sure %s exists' % (pathToFitsFile,)
        log.critical(message)
        raise IOError(message)

    # MASK FILES HAVE MOSY HEADER DETAILS IN EXTENSION 1
    headerExtension = 0

    if "mask" in basename[:4]:
        headerExtension = 1

    if "_sb.fits" in basename:
        headerExtension = 1

    # if basename == "mask.fits":
    #     log.debug('ignoring mask.fits file at: %s' % (pathToFitsFile,))
    #     return None

    fitsFileHeaderDictionary = dft.convert_fits_header_to_dictionary(
        log=log,
        pathToFitsFile=pathToFitsFile,
        headerExtension=headerExtension
    )

    #log.debug('fitsFileHeaderDictionary: %s' % (fitsFileHeaderDictionary,))
    fitsFileHeaderDictionary["filename"] = [
        str(os.path.basename(pathToFitsFile)), "filename"]
    fitsFileHeaderDictionary["filePath"] = [
        str(os.path.abspath(pathToFitsFile)), "the path to the file"]
    fitsFileHeaderDictionary["headerExtension"] = [
        headerExtension, "the fits header extension that was ingested"]

    # CONVERT BOOLEAN TO STRING
    dictCopy = fitsFileHeaderDictionary
    for k, v in list(fitsFileHeaderDictionary.items()):
        if isinstance(v[0], bool):
            if v[0]:
                dictCopy[k] = ["T", v[1]]
            else:
                dictCopy[k] = ["F", v[1]]

        if k == "RELEASE":
            dictCopy["RRELEASE"] = [v[0], v[1]]
            del dictCopy[k]

    fitsFileHeaderDictionary = dictCopy

    # SOME EDGE CASE FIXES
    if "________________OG530_/ADA_GUID_DEC".replace("_", " ") in fitsFileHeaderDictionary:
        fitsFileHeaderDictionary["ESO_ADA_GUID_DEC"] = fitsFileHeaderDictionary[
            "________________OG530_/ADA_GUID_DEC".replace("_", " ")]
        del fitsFileHeaderDictionary[
            "________________OG530_/ADA_GUID_DEC".replace("_", " ")]

    # if "QUALITY" not in list(fitsFileHeaderDictionary.keys()):
    #     fitsFileHeaderDictionary["QUALITY"] = [None, None]

    mysqlTableName = _get_fits_file_type(
        log=log,
        fitsFileHeaderDictionary=fitsFileHeaderDictionary
    )

    log.debug('mysqlTableName to ingest into: %s' % (mysqlTableName,))

    try:
        insertCommand, valueTuple = convert_dictionary_to_mysql_table(
            dbConn=dbConn,
            log=log,
            dictionary=fitsFileHeaderDictionary,
            dbTableName=mysqlTableName,
            uniqueKeyList=["filename", ],
            dateModified=False,
            returnInsertOnly=False,
            replace=False
        )
    except:
        log.error(
            "cound not impor the header for fits file %(pathToFitsFile)s" % locals())

    # log.debug('fitsFileHeaderDictionary: %s' % (fitsFileHeaderDictionary,))
    log.debug('mysqlTableName: %s' % (mysqlTableName,))

    log.debug('completed the ``ingest_fits_file`` function')
    return None

# LAST MODIFIED : August 26, 2013
# CREATED : August 26, 2013
# AUTHOR : DRYX

def _get_fits_file_type(
        log,
        fitsFileHeaderDictionary):
    """
    *get fits file type for a given fits file header dictionary*

    **Key Arguments**

    - ``log`` -- logger
    - ``fitsFileHeaderDictionary`` -- the python version of the fits file header
    

    **Return**

    - ``mysqlTableName`` -- the name of the mysql table to ingest the fits file info into
    

    .. todo::

        @review: when complete, clean _get_fits_file_type function
        @review: when complete add logging
        @review: when complete, decide whether to abstract function to another module
    """
    ## STANDARD LIB ##
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##

    log.debug('starting the ``get_fits_file_type`` function')
    # TEST THE ARGUMENTS
    if not isinstance(fitsFileHeaderDictionary, dict):
        message = 'Please make sure "fitsFileHeaderDictionary" is a dict'
        log.critical(message)
        raise TypeError(message)

    ## VARIABLES ##
    instrument = ""
    mode = 0

    if "corrupted" in fitsFileHeaderDictionary:
        mysqlTableName = "corrupted_files"
    elif "sens_merge" in fitsFileHeaderDictionary["filename"][0]:
        mysqlTableName = "corrupted_files"
    elif "_sb.fits" in fitsFileHeaderDictionary["filename"][0] and "merge" in fitsFileHeaderDictionary["filename"][0]:
        mysqlTableName = "sofi_spectra_binary_table_extension"
    elif "_sb.fits" in fitsFileHeaderDictionary["filename"][0]:
        mysqlTableName = "efosc_spectra_binary_table_extension"
    else:

        if "INSTRUME" not in fitsFileHeaderDictionary:
            message = ""
            for k, v in list(fitsFileHeaderDictionary.items()):
                message += "%s: %s\n" % (k, v[0])
            message += '"INSTRUME" keyword missing or blank for file %s[%s]' % (
                fitsFileHeaderDictionary['filePath'][0], fitsFileHeaderDictionary['headerExtension'][0],)
            log.critical(message)
            raise ValueError(message)

        if fitsFileHeaderDictionary["INSTRUME"][0].lower() == "efosc":
            instrument = "efosc"
        elif fitsFileHeaderDictionary["INSTRUME"][0].lower() == "sofi":
            instrument = "sofi"
        else:
            message = '"INSTRUME" keyword missing or blank'
            log.critical(message)
            raise ValueError(message)

        if "ESO TPL NAME" in list(fitsFileHeaderDictionary.keys()):
            if "spectr" in fitsFileHeaderDictionary["ESO TPL NAME"][0].lower():
                mode = "spectra"
            elif "image" in fitsFileHeaderDictionary["ESO TPL NAME"][0].lower():
                mode = "imaging"
        if mode == 0 and "ESO DPR TECH" in list(fitsFileHeaderDictionary.keys()):
            if "spectr" in fitsFileHeaderDictionary["ESO DPR TECH"][0].lower():
                mode = "spectra"
            elif "image" in fitsFileHeaderDictionary["ESO DPR TECH"][0].lower():
                mode = "imaging"
        if mode == 0 and "OBSTECH" in list(fitsFileHeaderDictionary.keys()):
            if "spectr" in fitsFileHeaderDictionary["OBSTECH"][0].lower():
                mode = "spectra"
            elif "image" in fitsFileHeaderDictionary["OBSTECH"][0].lower():
                mode = "imaging"
        elif mode == 0 and "ESO INS GRIS1 NAME" in list(fitsFileHeaderDictionary.keys()):
            if "foc_wedge" in fitsFileHeaderDictionary["ESO INS GRIS1 NAME"][0].lower():
                mode = "imaging"

        if mode == 0:
            message = '"ESO DPR TECH" and "ESO TPL NAME" and "OBSTECH" missing or incorrect for file %s, mode set to "%s"' % (
                fitsFileHeaderDictionary["filePath"], mode)
            log.critical(message)
            raise ValueError(message)

        mysqlTableName = """%s_%s""" % (instrument, mode)

    log.debug('mysqlTableName: %s' % (mysqlTableName,))

    log.debug('completed the ``get_fits_file_type`` function')
    return mysqlTableName

############################################
# CODE TO BE DEPECIATED                    #
############################################

if __name__ == '__main__':
    main()

###################################################################
# TEMPLATE FUNCTIONS                                              #
###################################################################
