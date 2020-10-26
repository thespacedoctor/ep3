#!/usr/local/bin/python
# encoding: utf-8
"""
*Cone-searching for the PESSTO Marshall Transient Bucket*

:Author:
    David Young

:Date Created:
    October 22, 2013

Usage:
    pm_conesearch_transientbucket --host=<host> --user=<user> --passwd=<passwd> --dbName=<dbName> --raDeg=<raDeg> --decDeg=<decDeg> --radiusArcSec=<radius>
    pm_conesearch_transientbucket --settingsFile=<pathToSettingsFile> --raDeg=<raDeg> --decDeg=<decDeg> --radiusArcSec=<radius>

Options:
    -h, --help                show this help message
    --settingsFile=<pathToSettingsFile>        path to the settings file
    --host=<host>             database host
    --user=<user>             database user
    --passwd=<passwd>         database password
    --dbName=<dbName>         database name
    --raDeg=<raDeg>           ra is decimal degrees
    --decDeg=<decDeg>         dec is decimal degrees
    --radiusArcSec=<radius>   conesearch radius in arcsecs
"""
################# GLOBAL IMPORTS ####################
import sys
import os
from docopt import docopt
from dryxPython import astrotools as dat
from dryxPython import logs as dl
from dryxPython import commonutils as dcu


def main(arguments=None):
    ########## IMPORTS ##########
    ## STANDARD LIB ##
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##

    ## ACTIONS BASED ON WHICH ARGUMENTS ARE RECIEVED ##
    if arguments == None:
        arguments = docopt(__doc__)

    # UNPACK SETTINGS
    if "--settingsFile" in arguments and arguments["--settingsFile"]:
        import yaml
        stream = file(arguments["--settingsFile"], 'r')
        settings = yaml.load(stream)
        stream.close()

    # SETUP LOGGER -- DEFAULT TO CONSOLE LOGGER IF NONE PROVIDED IN SETTINGS
    if 'settings' in locals() and "logging settings" in settings:
        log = dl.setup_dryx_logging(
            yaml_file=arguments["--settingsFile"]
        )
    elif "--logger" not in arguments or arguments["--logger"] is None:
        log = dl.console_logger(
            level="DEBUG"
        )
        log.debug('logger setup')

    # SETUP A DATABASE CONNECTION BASED ON WHAT ARGUMENTS HAVE BEEN PASSED
    dbConn = False
    if 'settings' in locals() and "database settings" in settings:
        host = settings["database settings"]["host"]
        user = settings["database settings"]["user"]
        passwd = settings["database settings"]["password"]
        dbName = settings["database settings"]["db"]
        dbConn = True
    elif "--host" and "--dbName" in locals():
        # SETUP DB CONNECTION
        dbConn = True
        host = arguments["host"]
        user = arguments["user"]
        passwd = arguments["passwd"]
        dbName = arguments["dbName"]
    if dbConn:
        import pymysql as ms
        dbConn = ms.connect(
            host=host,
            user=user,
            passwd=passwd,
            db=dbName,
        )
        dbConn.autocommit(True)
        log.debug('dbConn: %s' % (dbConn,))

    # UNPACK REMAINING CL ARGUMENTS
    for arg, val in arguments.iteritems():
        varname = arg.replace("--", "")
        if isinstance(val, str) or isinstance(val, unicode):
            exec(varname + """ = '%s' """ % (val,))
        else:
            exec(varname + """ = %s """ % (val,))
        if arg == "--dbConn":
            dbConn = val
        log.debug('%s = %s' % (varname, val,))

    ## START LOGGING ##
    startTime = dcu.get_now_sql_datetime()
    log.info(
        '--- STARTING TO RUN THE conesearch_transientbucket.py AT %s' %
        (startTime,))

    # BEGINNING DEVELOPMENT CODE
    ## -- TO BE CLEANED OUT INTO FUNCTIONS AND CLASSES ##
    search(
        dbConn=dbConn,
        log=log,
        raDeg=raDeg,
        decDeg=decDeg,
        radiusArcSec=radiusArcSec,
        nearest=False
    )

    if dbConn in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.debug('runningTime: %s' % (runningTime,))
    log.info(
        '-- FINISHED ATTEMPT TO RUN THE conesearch_transientbucket.py AT %s (RUNTIME: %s) --' %
        (endTime, runningTime, ))

    return


def search(
        dbConn,
        log,
        raDeg,
        decDeg,
        radiusArcSec=5.0,
        nearest=False
):
    # LAST MODIFIED : October 16, 2013
    # CREATED : October 16, 2013
    # AUTHOR : DRYX
    """
    Conesearch the transientBucket table in the PESSTO Marshall with the given arguments.

    **Key Arguments:**
        - ``dbConn`` - the database connection
        - ``log`` -- the logger
        - ``raDeg`` -- right ascendtion (degs)
        - ``decDeg`` -- declination (deg)
        - ``radiusArcSec`` -- the radius of the search aperature to use
        - ``nearest`` -- return just the closest matching result (otherwise return all matches)

    **Return:**
        - None

    .. todo::

        @review: when complete, clean search function
        @review: when complete add logging
        @review: when complete, decide whether to abstract function to another module
    """

    ################ > IMPORTS ################
    ## STANDARD LIB ##
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##
    from pessto_marshall_engine.database import crossmatchers

    # CONVERT THE VARIABLES TO FORMAT REQUIRED BY CONESEARCHER
    try:
        raDeg = float(raDeg)
    except:
        raDeg = dat.ra_sexegesimal_to_decimal.ra_sexegesimal_to_decimal(
            ra=raDeg)

    try:
        decDeg = float(decDeg)
    except:
        decDeg = dat.declination_sexegesimal_to_decimal.declination_sexegesimal_to_decimal(
            dec=decDeg)

    radius = float(radiusArcSec)

    transientBucketIdList, raList, decList, objectNameList = crossmatchers.conesearch_marshall_transientBucket_objects(
        dbConn=dbConn,
        log=log,
        ra=raDeg,
        dec=decDeg,
        radiusArcSec=radius,
        nearest=nearest
    )

    if (isinstance(transientBucketIdList, long) or isinstance(transientBucketIdList, int)) and not isinstance(transientBucketIdList, bool):
        transientBucketIdList = [transientBucketIdList]
        raList = [raList]
        decList = [decList]
        objectNameList = [objectNameList]

    if transientBucketIdList == None or len(transientBucketIdList) == 0:
        print """%s | %s | %s | %s""" % ("NOMATCH".ljust(25), "NOMATCH".ljust(20), "NOMATCH".ljust(20), "NOMATCH".ljust(10))
    else:
        for t, r, d, o in zip(transientBucketIdList, raList, decList, objectNameList):
            print """%s | %s | %s | %s""" % (o.ljust(25), str(r).ljust(20), str(d).ljust(20), str(t).ljust(10))

    return transientBucketIdList, raList, decList, objectNameList


###################################################################
# CLASSES                                                         #
###################################################################
###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
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
