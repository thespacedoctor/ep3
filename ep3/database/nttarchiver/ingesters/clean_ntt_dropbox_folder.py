#!/usr/local/bin/python
# encoding: utf-8
"""
*Clean up the NTT data dropbox folder - removing empty directories and non-fits files*

:Author:
    David Young

:Date Created:
    October 28, 2013

Usage:
    pm_clean_ntt_dropbox_folder -s <pathToSettingsFile>
    pm_clean_ntt_dropbox_folder -p <pathToDropboxFolder>

Options:
    -h, --help                 show this help message
    -s, --settingsFile         path to the settings file
    -p, --pathToDropboxFolder  path to the folder where NTT data is dumped, ready for import
"""
################# GLOBAL IMPORTS ####################
import sys
import os
from docopt import docopt
from dryxPython import logs as dl
from dryxPython import commonutils as dcu


def main(arguments=None):
    """
    *The main function used when ``clean_ntt_dropbox_folder.py`` is run as a single script from the cl, or when installed as a cl command*
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

    # x-setup-database-connection-in-main-function

    ## START LOGGING ##
    startTime = dcu.get_now_sql_datetime()
    log.info(
        '--- STARTING TO RUN THE clean_ntt_dropbox_folder.py AT %s' %
        (startTime,))

    # call the worker function
    if "settings" in locals() and "nttarchiver" in settings:
        clean_ntt_dropbox_folder(
            log=log,
            pathToDropboxFolder=settings[
                "nttarchiver"]["path to dropbox folder"]
        )
    elif "pathToDropboxFolder" in locals() and pathToDropboxFolder:
        clean_ntt_dropbox_folder(
            log=log,
            pathToDropboxFolder=pathToDropboxFolder
        )
    else:
        log.error(
            'cound not find setting for path to NTT dropbox (pathToDropboxFolder)')
        raise AttributeError(
            "cound not find setting for path to NTT dropbox (pathToDropboxFolder)")

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info(
        '-- FINISHED ATTEMPT TO RUN THE clean_ntt_dropbox_folder.py AT %s (RUNTIME: %s) --' %
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


def clean_ntt_dropbox_folder(
        log,
        pathToDropboxFolder
):
    """
    *clean dropbox folder -  remove non fits files*

    **Key Arguments:**
        - ``log`` -- logger
        - ``pathToDropboxFolder`` -- path to the folder to clean

    **Return:**
        - None

    .. todo::

    @review: when complete, clean clean_ntt_dropbox_folder function & add logging
    """
    ################ > IMPORTS ################
    ## STANDARD LIB ##
    from subprocess import call
    import time
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##
    import dryxPython.commonutils as dcu

    log.debug(
        'completed the ````clean_ntt_dropbox_folder`` function')
    ## VARIABLES ##

    basePath = pathToDropboxFolder

    directoryContents = dcu.get_recursive_list_of_directory_contents(
        log,
        baseFolderPath=pathToDropboxFolder,
        whatToList='all'
    )

    for d in directoryContents:
        if os.path.isfile(os.path.join(basePath, d)):
            if "fits" not in d and "downloadRequest" not in d and ".log" not in d:
                try:
                    log.debug("attempting to delete file from dropbox")
                    os.remove(os.path.join(basePath, d))
                except Exception as e:
                    log.warning(
                        "could not delete file from dropbox - failed with this error: %s " % (str(e),))
            if "fits.Z" in d:
                compressedFilePath = os.path.join(basePath, d)
                uncompressedFilePath = compressedFilePath.replace(".Z", "")
                try:
                    with open(uncompressedFilePath):
                        pass
                    fileExists = True
                except IOError:
                    fileExists = False

                if not fileExists:
                    try:
                        log.debug("attempting to uncompress .Z file")
                        call("zcat %s > %s" % (
                            compressedFilePath, uncompressedFilePath), shell=True)
                    except Exception as e:
                        log.error(
                            "could not uncompress .Z file - failed with this error: %s " % (str(e),))
                        break

                    try:
                        log.debug("attempting to delete file from dropbox")
                        os.remove(compressedFilePath)
                    except Exception as e:
                        log.warning(
                            "could not delete file from dropbox - failed with this error: %s " % (str(e),))
                else:
                    log.warning('the file %s already exists - deleting compressed file' % (
                        uncompressedFilePath,))
                    try:
                        log.debug("attempting to delete file from dropbox")
                        os.remove(compressedFilePath)
                    except Exception as e:
                        log.warning(
                            "could not delete file from dropbox - failed with this error: %s " % (str(e),))

    dcu.recursively_remove_empty_directories(
        log,
        basePath=pathToDropboxFolder,
    )

    log.debug(
        'completed the ``clean_ntt_dropbox_folder`` function')

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
