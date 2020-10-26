#!/usr/local/bin/python
# encoding: utf-8
"""
*Using the RA and DEC in the FITS header data ingested into the PESSTO Marshall database, crossmatch the file objects against the transientBucket table to assoicate a trnasientBucketID and correct the RA,DEC and object names.*

:Author:
    David Young

:Date Created:
    October 29, 2013

Usage:
    pm_crossmatch_ntt_data_against_transientbucket -s <pathToSettingsFile> [update]
    pm_crossmatch_ntt_data_against_transientbucket --host=<host> --user=<user> --passwd=<passwd> --dbName=<dbName> [update]

Options:
    -h, --help             show this help message
    -s, --settingsFile     path to the settings file
    update                 update FITS header information already in the database (useful when object names change in marshall)
    --host=<host>          database host
    --user=<user>          database user
    --passwd=<passwd>      database password
    --dbName=<dbName>      database name
"""
################# GLOBAL IMPORTS ####################
import sys
import os
from docopt import docopt
from dryxPython import logs as dl
from dryxPython import commonutils as dcu


def main(arguments=None):
    """
    *The main function used when ``crossmatch_ntt_data_against_transientbucket.py`` is run as a single script from the cl, or when installed as a cl command*
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

    ## START LOGGING ##
    startTime = dcu.get_now_sql_datetime()
    log.info(
        '--- STARTING TO RUN THE crossmatch_ntt_data_against_transientbucket.py AT %s' %
        (startTime,))

    if "update" not in locals():
        update = False

    # call the worker function
    # x-if-settings-or-database-credentials
    crossmatch_ntt_data_against_transientbucket(
        dbConn,
        log,
        updateArchivedFile=update
    )

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info(
        '-- FINISHED ATTEMPT TO RUN THE crossmatch_ntt_data_against_transientbucket.py AT %s (RUNTIME: %s) --' %
        (endTime, runningTime, ))

    return

###################################################################
# CLASSES                                                         #
###################################################################

###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# LAST MODIFIED : October 29, 2013
# CREATED : October 29, 2013
# AUTHOR : DRYX


def crossmatch_ntt_data_against_transientbucket(
        dbConn,
        log,
        updateArchivedFile=False
):
    """

    *Crossmatch the NTT Data using the RA and DEC found in the fits headers (telescope pointings) against the transientBucket and associate with the closest match.*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``updateArchivedFile`` -- update the names in the archived files?

    **Return:**
        - None

    .. todo::


    """
    ################ > IMPORTS ################
    ## STANDARD LIB ##
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##
    import dryxPython.fitstools as dft
    from pessto_marshall_engine.database import crossmatchers as pmcm
    from dryxPython import mysql as dms

    log.debug(
        'completed the ````crossmatch_ntt_data_against_transientbucket`` function')
    ## VARIABLES ##

    # before crossmatch clean up the htmid columns
    dms.add_HTMIds_to_mysql_tables.add_HTMIds_to_mysql_tables(
        raColName="raDeg",
        declColName="decDeg",
        tableName="transientBucket",
        dbConn=dbConn,
        log=log,
        primaryIdColumnName="primaryKeyId")

    # first the NON-RAW IMAGING DATA
    tableNames = ["efosc_imaging", "sofi_imaging"]
    skipNames = ["EFOSC.", "SOFI."]
    skipNames = ["xxxxx", "xxxxxx"]
    searchRadius = [100, 900]

    # Check for the update flag from command-line
    if not updateArchivedFile:
        updateArchivedFile = "and (isInTransientBucket is null or isInTransientBucket = 0) "
    else:
        updateArchivedFile = " "

    print "checking the NTT imaging data"

    PESSTOESO154Names = []
    sqlQuery = """
        select distinct name from transientBucket where transientBucketId = 352873;
    """
    rows = dms.execute_mysql_read_query(
        sqlQuery,
        dbConn,
        log
    )
    for r in rows:
        PESSTOESO154Names.append(r["name"])

    SN2013fcNames = []
    sqlQuery = """
        select distinct name from transientBucket where transientBucketId = 45354;
    """
    rows = dms.execute_mysql_read_query(
        sqlQuery,
        dbConn,
        log
    )
    for r in rows:
        SN2013fcNames.append(r["name"])

    for t, s, r in zip(tableNames, skipNames, searchRadius):
        # grab metadata from ntt fits files
        sqlQuery = """
            SELECT primaryId, RA, DECL, object, ESO_OBS_TARG_NAME, transientBucketId, currentFilename FROM %s WHERE RA is not NULL and filename not like '%s%%' and (filetype_key_calibration = 13 or filetype_key_calibration = 10 or filetype_key_calibration = 11) %s  and lock_row = 0
        """ % (t, s, updateArchivedFile)
        rows = dms.execute_mysql_read_query(
            sqlQuery,
            dbConn,
            log
        )

        totalCount = len(rows)
        thisCount = 0

        # match each file again the transientbucket
        overwrite = True
        for row in rows:
            thisCount += 1
            # Cursor up one line and clear line
            if thisCount > 1 and overwrite:
                sys.stdout.write("\x1b[1A\x1b[2K")
            print "%(thisCount)s/%(totalCount)s %(t)s checked" % locals()
            transientBucketId, raDeg, decDeg, objectName = pmcm.conesearch_marshall_transientBucket_objects(
                dbConn,
                log,
                ra=row["RA"],
                dec=row["DECL"],
                radiusArcSec=r
            )

            # update the names that have changed since the last crossmatch or
            # have not been set yet
            if transientBucketId and (row["object"] != objectName or row["transientBucketId"] is None):
                if (transientBucketId == 352873 or transientBucketId == 45354):
                    if row["object"] in PESSTOESO154Names or row["ESO_OBS_TARG_NAME"] in PESSTOESO154Names:
                        sqlQuery = """
                            UPDATE %s SET isInTransientBucket = 1, transientBucketId = 352873, object = "SN2014eg", rewriteFitsHeader = 1
                                WHERE primaryID = %s  and lock_row = 0""" % (t, row["primaryId"])
                    elif row["object"] in SN2013fcNames or row["ESO_OBS_TARG_NAME"] in SN2013fcNames:
                        sqlQuery = """
                            UPDATE %s SET isInTransientBucket = 1, transientBucketId = 45354
                                WHERE primaryID = %s  and lock_row = 0""" % (t, row["primaryId"])
                    else:
                        thisObject = row["object"]
                        thisFile = row["currentFilename"]
                        log.warning(
                            'too close to another pessto object to distinguish them from one another - PESSTOESO154-G10 and SN2013fc. Please update images manually. The object in this image (%(thisFile)s) is %(thisObject)s' % locals())
                        sqlQuery = """
                        UPDATE %s SET isInTransientBucket = 1
                            WHERE primaryID = %s and lock_row = 0""" % (t, row["primaryId"])
                else:
                    print "requesting a name update from %(objectName)s" % locals()
                    sqlQuery = """
                        UPDATE %s SET transientBucketId = %s, isInTransientBucket = 1, rewriteFitsHeader = 1, updateObjectName = 1
                            WHERE primaryID = %s  and lock_row = 0""" % (t, transientBucketId, row["primaryId"])
            # if no match then show this in database
            elif transientBucketId == None:
                sqlQuery = """
                    UPDATE %s SET isInTransientBucket = 0
                        WHERE primaryID = %s  and lock_row = 0""" % (t, row["primaryId"])
            elif transientBucketId:
                sqlQuery = """
                    UPDATE %s SET isInTransientBucket = 1, transientBucketId = %s
                        WHERE primaryID = %s  and lock_row = 0""" % (t, transientBucketId, row["primaryId"])
            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=dbConn,
                log=log
            )

    print "checking the NTT spectral data"

    # repeat for the spectra data (now for ra, dec AND object - ra and dec
    # originally telescope pointing not the object coordinates)
    tableNames = ["efosc_spectra", "sofi_spectra"]
    skipNames = ["EFOSC.", "SOFI."]
    skipNames = ["xxxxx", "xxxxxx"]
    searchRadius = [600, 600]

    for t, s, r in zip(tableNames, skipNames, searchRadius):
        sqlQuery = """
            SELECT primaryId, RA, DECL, object, transientBucketId, ESO_OBS_TARG_NAME, currentFilename FROM %s WHERE RA is not NULL and filename not like '%s%%' and (filetype_key_calibration = 13 or filetype_key_calibration = 10 or filetype_key_calibration = 11) %s  and lock_row = 0
        """ % (t, s, updateArchivedFile)
        rows = dms.execute_mysql_read_query(
            sqlQuery,
            dbConn,
            log
        )

        totalCount = len(rows)
        thisCount = 0

        for row in rows:
            thisCount += 1
            # Cursor up one line and clear line
            if thisCount > 1:
                sys.stdout.write("\x1b[1A\x1b[2K")
            print "%(thisCount)s/%(totalCount)s %(t)s checked" % locals()
            transientBucketId, raDeg, decDeg, objectName = pmcm.conesearch_marshall_transientBucket_objects(
                dbConn,
                log,
                ra=row["RA"],
                dec=row["DECL"],
                radiusArcSec=r
            )

            print row["RA"], row["DECL"], r
            print transientBucketId, raDeg, decDeg, objectName
            print ""

            if transientBucketId and ((row["object"] != objectName) or (row["transientBucketId"] is None) or (str(row["RA"]) != str(raDeg)) or (str(row["DECL"]) != str(decDeg))):
                if (transientBucketId == 352873 or transientBucketId == 45354):
                    if row["object"] in PESSTOESO154Names or row["ESO_OBS_TARG_NAME"] in PESSTOESO154Names:
                        sqlQuery = """
                            UPDATE %s SET isInTransientBucket = 1, transientBucketId = 352873, RA = 41.288625, DECL = -55.7380305556, rewriteFitsHeader = 1, object = "SN2014eg"
                                WHERE primaryID = %s  and lock_row = 0""" % (t, row["primaryId"])
                        print "HERE"
                    elif row["object"] in SN2013fcNames or row["ESO_OBS_TARG_NAME"] in SN2013fcNames:
                        sqlQuery = """
                            UPDATE %s SET isInTransientBucket = 1, transientBucketId = 45354
                                WHERE primaryID = %s  and lock_row = 0""" % (t, row["primaryId"])
                    else:
                        thisObject = row["object"]
                        thisFile = row["currentFilename"]
                        log.warning(
                            'too close to another pessto object to distinguish them from one another - PESSTOESO154-G10 and SN2013fc. Please update images manually. The object in this image (%(thisFile)s) is %(thisObject)s' % locals())
                        sqlQuery = """
                        UPDATE %s SET isInTransientBucket = 1
                            WHERE primaryID = %s  and lock_row = 0""" % (t, row["primaryId"])
                else:
                    sqlQuery = """
                        UPDATE %s SET transientBucketId = %s, isInTransientBucket = 1, RA = %s, DECL = %s, rewriteFitsHeader = 1, updateObjectName = 1
                            WHERE primaryID = %s  and lock_row = 0""" % (t, transientBucketId, raDeg, decDeg, row["primaryId"])
            elif transientBucketId == None:
                sqlQuery = """
                    UPDATE %s SET isInTransientBucket = 0
                        WHERE primaryID = %s  and lock_row = 0""" % (t, row["primaryId"])
            elif transientBucketId:
                sqlQuery = """
                    UPDATE %s SET isInTransientBucket = 1, transientBucketId = %s
                        WHERE primaryID = %s  and lock_row = 0""" % (t, row["primaryId"], transientBucketId)
            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=dbConn,
                log=log
            )

    # UPDATE THE NAMES IN THE OBJECT FIELD OF THE FITS FILES
    print "updating the names in the object field of NTT fits files"
    tableNames = [
        "efosc_imaging", "efosc_spectra", "sofi_imaging", "sofi_spectra"]
    skipNames = ["EFOSC.", "EFOSC.", "SOFI.", "SOFI."]

    for t, s in zip(tableNames, skipNames):
        sqlQuery = """
            SELECT primaryId, OBJECT, transientBucketId FROM %s WHERE updateObjectName = 1 and filename not like '%s%%' and lock_row = 0
        """ % (t, s)

        try:
            rows = dms.execute_mysql_read_query(
                sqlQuery,
                dbConn,
                log
            )
        except Exception as e:
            log.error(
                "mysql could not execute this query\n%s\nhere is the error: %s " %
                (sqlQuery, str(e),))

        total = len(rows)
        count = 1
        for row in rows:
            sqlQuery = """
                SELECT name FROM view_transientBucketMaster WHERE transientBucketId = %s
            """ % (row["transientBucketId"],)
            if count > 1:
                # Cursor up one line and clear line
                sys.stdout.write("\x1b[1A\x1b[2K")
            print """%(count)s/%(total)s %(t)s checked""" % locals()
            count += 1

            try:
                idRows = dms.execute_mysql_read_query(
                    sqlQuery=sqlQuery,
                    dbConn=dbConn,
                    log=log
                )
            except Exception as e:
                log.error(
                    "mysql could not execute this query\n%s\nhere is the error: %s " %
                    (sqlQuery, str(e),))

            if len(idRows) == 0:
                thisId = row["transientBucketId"]
                sqlQuery = u"""
                    update %(t)s set transientBucketId = 0, isInTransientBucket = null, updateObjectName = 0 where transientBucketId = %(thisId)s and lock_row = 0
                """ % locals()
                dms.execute_mysql_write_query(
                    sqlQuery=sqlQuery,
                    dbConn=dbConn,
                    log=log,
                )
                continue

            tbName = idRows[0]["name"].replace(" ", "_")
            if tbName != row["OBJECT"] and "PESSTOESO154" not in tbName:
                sqlQuery = """
                    UPDATE %s SET OBJECT = "%s", rewriteFitsHeader = 1, updateObjectName = 0
                        WHERE primaryID = %s  and lock_row = 0""" % (t, tbName, row["primaryId"])
            else:
                sqlQuery = """
                    UPDATE %s SET updateObjectName = 0
                        WHERE primaryID = %s  and lock_row = 0""" % (t, row["primaryId"])
            try:
                dms.execute_mysql_write_query(
                    sqlQuery=sqlQuery,
                    dbConn=dbConn,
                    log=log
                )
            except Exception as e:
                log.error(
                    "mysql could not execute this query\n%s\nhere is the error: %s " %
                    (sqlQuery, str(e),))

    # JUST ASSOCIATE TRANSIENT BUCKET ID TO RAW IMAGES
    print "associating transientBucketIDs with raw images"
    tableNames = [
        "efosc_imaging", "efosc_spectra", "sofi_imaging", "sofi_spectra"]
    for t in tableNames:

        sqlQuery = """
            SELECT primaryId, RA, DECL FROM %s WHERE RA is not NULL %s
        """ % (t, updateArchivedFile)
        rows = dms.execute_mysql_read_query(
            sqlQuery,
            dbConn,
            log
        )

        totalCount = len(rows)
        thisCount = 0

        for row in rows:
            thisCount += 1
            if thisCount > 1:
                # Cursor up one line and clear line
                sys.stdout.write("\x1b[1A\x1b[2K")
            print "%(thisCount)s/%(totalCount)s %(t)s checked" % locals()
            transientBucketId, raDeg, decDeg, objectName = pmcm.conesearch_marshall_transientBucket_objects(
                dbConn,
                log,
                ra=row["RA"],
                dec=row["DECL"],
                radiusArcSec=150.0
            )
            if transientBucketId:
                sqlQuery = """
                    UPDATE %s SET transientBucketId = %s, isInTransientBucket = 1 WHERE primaryID = %s  and lock_row = 0""" % (t, transientBucketId, row["primaryId"])
            else:
                sqlQuery = """
                    UPDATE %s SET isInTransientBucket = 0
                        WHERE primaryID = %s  and lock_row = 0""" % (t, row["primaryId"])
            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=dbConn,
                log=log
            )

    log.info(
        'finishing the ``crossmatch_ntt_data_against_transientbucket`` function')

    return None

# the tab-trigger below for new function
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
