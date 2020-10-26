#!/usr/local/bin/python
# encoding: utf-8
"""
*MySQL commands to set and update the flags used to generate helpful views & queries on the transientBucket/pesstoObjects tables
    These flags will mainly be used for the sort queries -- keeping the time heavy queries out of the front-end loading of webpages.*

:Author:
    David Young

:Date Created:
    November 13, 2013

Usage:
    pm_update_transientbucketsummaries_flags -s <pathToSettingsFile>
    pm_update_transientbucketsummaries_flags -s <pathToSettingsFile> --updateAll
    pm_update_transientbucketsummaries_flags -s <pathToSettingsFile> --updateLive
    pm_update_transientbucketsummaries_flags - \
        s <pathToSettingsFile> --updateClassified
    pm_update_transientbucketsummaries_flags -s <pathToSettingsFile> -t <transientBucketId>

Options:
    -h, --help               show this help message
    -s, --settingsFile       path to the settings file
    -t, --transientBucketId  the transient's unique database ID
    --updateAll              update ALL transients in the database (can take a few hrs)
    --updateClassified       update only classified objects
    --updateLive             update only live objects
"""
################# GLOBAL IMPORTS ####################
import sys
import os
from datetime import datetime, date, time
from docopt import docopt
from dryxPython import logs as dl
from dryxPython import commonutils as dcu
from astropy import units as u
from astropy.coordinates import SkyCoord
from fundamentals.mysql import readquery, writequery


def main(arguments=None):
    """
    *The main function used when ``update_transientbucketsummaries_flags.py`` is run as a single script from the cl, or when installed as a cl command*
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
        logLevel="DEBUG",
        projectName="pessto_marshall"
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
        '--- STARTING TO RUN THE update_transientbucketsummaries_flags.py AT %s' %
        (startTime,))

    if updateAllFlag is True:
        log.info('updating ALL object flags')
    elif not transientBucketId:
        log.info('updating only flags for objects not in the archive')
    elif transientBucketId:
        log.info(
            'updating flags for transientBucketId: %(transientBucketId)s' % locals())

    if "transientBucketId" not in locals():
        transientBucketId = False

    # call the worker function
    # x-if-settings-or-database-credentials
    update_transientbucketsummaries_flags(
        log=log,
        dbConn=dbConn,
        updateAll=updateAllFlag,
        transientBucketId=transientBucketId,
        updateClassified=updateClassifiedFlag,
        updateLive=updateLiveFlag
    )

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info(
        '-- FINISHED ATTEMPT TO RUN THE update_transientbucketsummaries_flags.py AT %s (RUNTIME: %s) --' %
        (endTime, runningTime, ))

    return

###################################################################
# CLASSES                                                         #
###################################################################

###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# LAST MODIFIED : November 13, 2013
# CREATED : November 13, 2013
# AUTHOR : DRYX


def update_transientbucketsummaries_flags(
        log,
        dbConn,
        updateAll=False,
        transientBucketId=False,
        updateClassified=False,
        updateLive=False
):
    """
    *update_transientbucketsummaries_flags*

    **Key Arguments:**
        - ``log`` -- the logger
        - ``dbConn`` -- the database connection
        - ``updateAll`` -- update every row in the database (i.e. objects also in the archive)
        - ``transientBucketId`` -- used to update just one object

    **Return:**
        - None

    .. todo::

    """
    ################ > IMPORTS ################
    ## STANDARD LIB ##
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##
    import dryxPython.astrotools as dat

    # ADD NEW ROWS TO THE SUMMARY TABLE
    sqlQuery = """
        select distinct transientBucketId from transientBucket where transientBucketId not in (select transientBucketId from transientBucketSummaries) and replacedByRowId =0
    """ % locals()

    rows = readquery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn,
        quiet=False
    )

    newRowsLen = len(rows)
    log.debug(
        '%(newRowsLen)s rows added to the transientBucketSummaries' % locals())

    for row in rows:
        thisTransientBucketId = row["transientBucketId"]
        if thisTransientBucketId > 0:
            sqlQuery = """
                insert into transientBucketSummaries (transientBucketId) values (%(thisTransientBucketId)s)
            """ % locals()
            writequery(
                log=log,
                sqlQuery=sqlQuery,
                dbConn=dbConn,
            )

    if updateClassified:
        sqlQuery = """
        select distinct p.transientBucketId from transientBucketSummaries s, pesstoObjects p where s.transientBucketId=p.transientBucketId and p.classifiedFlag = 1;
        """
        rows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        theseIds = "-99, "
        for row in rows:
            theseIds += str(row["transientBucketId"]) + ","
        theseIds = theseIds[:-2]

    elif updateLive:
        sqlQuery = """
        select distinct p.transientBucketId from transientBucketSummaries s, pesstoObjects p where s.transientBucketId=p.transientBucketId and p.marshallWorkflowLocation != "archive";
        """
        rows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        theseIds = "-99, "
        for row in rows:
            theseIds += str(row["transientBucketId"]) + ","
        theseIds = theseIds[:-2]
    else:
        # GRAB A LIST OF THE transientBucketIds THAT NEED UPDATED
        sqlQuery = """
            select transientBucketId, dateCreated as dateUpdated, lastTBSUpdate from (select t.transientBucketId, t.dateCreated, s.lastTBSUpdate from transientBucket t, transientBucketSummaries s where t.dateCreated > now() - INTERVAL 6 HOUR and s.transientBucketId=t.transientBucketId and t.replacedByRowId=0 order by t.transientBucketId, t.dateCreated desc) as alias group by transientBucketId;
        """
        rows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        theseIds = "-99, "
        for row in rows:
            if not row["lastTBSUpdate"] or (row["dateUpdated"] > row["lastTBSUpdate"]):
                theseIds += str(row["transientBucketId"]) + ","
        theseIds = theseIds[:-2]

    # WORK OUT WHAT NEEDS UPDATING FROM CL ARGS - DEFAULT IS NON-ARCHIVED
    # OBJECTS
    if (updateAll is not False and updateAll is not None) or (transientBucketId is not False and transientBucketId is not None):
        if (updateAll is not False and updateAll is not None):
            log.debug(
                'updating all flags updateAll = %(updateAll)s, transientBucketId = %(transientBucketId)s ' % locals())
        updateAll = ""
    else:
        updateAll = """and t.transientBucketId in (%(theseIds)s) """ % locals()
    if transientBucketId is not False and transientBucketId is not None:
        transientBucketId = "and t.transientBucketId in (%(transientBucketId)s)" % locals(
        )
    else:
        transientBucketId = ""

    # GRAB A LIST OF THE transientBucketIds THAT NEED UPDATED
    sqlQuery = """
        select distinct t.transientBucketId from transientBucketSummaries t, pesstoObjects p where t.transientBucketId=p.transientBucketId %s%s order by transientBucketId desc
    """ % (updateAll, transientBucketId)

    rows = readquery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn,
        quiet=False
    )
    count = len(rows)

    log.debug('sqlQuery: %(sqlQuery)s' % locals())
    log.info(
        '%(count)s: transients to be updated in transientBucketSummaries' % locals())

    index = 0
    for row in rows:
        index += 1

        if index > 1:
            # Cursor up one line and clear line
            sys.stdout.write("\x1b[1A\x1b[2K")

        percent = (float(index) / float(count)) * 100.
        transientBucketId = row['transientBucketId']
        print '%(index)s/%(count)s (%(percent)1.1f%% done): attempting to update transientBucketId: %(transientBucketId)s' % locals()

        # UPDATE MASTER NAME, CURRENT MAG, COORDINATES, DATE ADDED TO MARSHALL,
        sqlQuery = u"""
             select transientBucketId, name, decDeg, raDeg, surveyObjectUrl, sherlockClassification
                        from transientBucket
                        where transientBucketId = %(transientBucketId)s and
                        masterIdFlag = 1  and replacedByRowId =0
                    limit 1
        """ % locals()
        extras = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )

        if not len(extras):
            log.error(
                'cound not find master name for transientBucketId %(transientBucketId)s ' % locals())
            continue

        masterName = extras[0]["name"]
        raDeg = extras[0]["raDeg"]
        surveyObjectUrl = extras[0]["surveyObjectUrl"]
        decDeg = extras[0]["decDeg"]
        sherlockClassification = extras[0]["sherlockClassification"]

        if surveyObjectUrl == None:
            sqlQuery = u"""
                 select surveyObjectUrl
                            from transientBucket
                            where transientBucketId = %(transientBucketId)s and
                            surveyObjectUrl is not null and surveyObjectUrl not like "%%astronomerstelegram%%" and surveyObjectUrl not like "%%roche%%" and replacedByRowId =0
                        limit 1
            """ % locals()
            extras = readquery(
                log=log,
                sqlQuery=sqlQuery,
                dbConn=dbConn,
                quiet=False
            )
            if len(extras):
                surveyObjectUrl = extras[0]["surveyObjectUrl"]

        log.info(
            "UPDATE MASTER NAME, CURRENT MAG, COORDINATES, DATE ADDED TO MARSHALL" % locals())

        # SURVEYURL, LAST NON-DETECTION, EARLIESTDETECTION, MOST RECENT
        sqlQuery = u"""
            select reducer from transientBucket where transientBucketId = %(transientBucketId)s  and replacedByRowId =0 order by dateCreated limit 1;
        """ % locals()
        tmpRows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        objectAddedToMarshallBy = "null"
        for tmpRow in tmpRows:
            objectAddedToMarshallBy = tmpRow["reducer"]
            objectAddedToMarshallBy = """ "%(objectAddedToMarshallBy)s" """ % locals(
            )

        sqlQuery = u"""
            select magnitude from transientBucket where transientBucketId = %(transientBucketId)s and magnitude is not NULL and magnitude < 50 and replacedByRowId =0 order by observationDate desc limit 1
        """ % locals()
        tmpRows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        currentMagnitude = "null"
        for tmpRow in tmpRows:
            currentMagnitude = tmpRow["magnitude"]

        sqlQuery = u"""
            select dateCreated from transientBucket where transientBucketId = %(transientBucketId)s and dateCreated and dateCreated != 0 is not NULL and replacedByRowId =0 order by dateCreated limit 1
        """ % locals()
        tmpRows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        dateCreated = "null"
        for tmpRow in tmpRows:
            dateCreated = tmpRow["dateCreated"]
            dateCreated = """ "%(dateCreated)s" """ % locals(
            )

        sqlQuery = u"""
            SELECT observationDate, magnitude, filter, survey from transientBucket where transientBucketId = %(transientBucketId)s and  survey != "bright sn list" and observationMJD is not null and observationMJD > 40000 and limitingMag != 1 and magnitude is not null and replacedByRowId =0 order by observationMJD limit 1
        """ % locals()
        tmpRows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )

        # if len(tmpRows) == 0:
        #     sqlQuery = u"""
        #         SELECT p.observationDate, p.magnitude, p.filter, s.survey from marshall_sources_photometry p, marshall_sources_discoveries s where s.marshallID = %(transientBucketId)s and  s.marshallID =p.marshallID and p.observationMJD is not null and p.observationMJD > 40000 and p.limitingMag != 1 order by p.observationMJD limit 1
        #     """ % locals()
        #     tmpRows = readquery(
        #         log=log,
        #         sqlQuery=sqlQuery,
        #         dbConn=dbConn,
        #         quiet=False
        #     )

        if len(tmpRows) == 0:
            sqlQuery = u"""
                SELECT observationDate, magnitude, filter, survey from transientBucket where transientBucketId = %(transientBucketId)s and observationMJD is not null and observationMJD > 40000 and limitingMag != 1 and replacedByRowId =0 order by observationMJD limit 1
            """ % locals()
            tmpRows = readquery(
                log=log,
                sqlQuery=sqlQuery,
                dbConn=dbConn,
                quiet=False
            )

        if len(tmpRows) == 0:
            continue

        observationDate = tmpRows[0]["observationDate"]
        earliestDetection = observationDate
        earliestMagnitude = tmpRows[0]["magnitude"]
        earliestMagnitudeSurvey = tmpRows[0]["survey"]
        earliestMagnitudeFilter = tmpRows[0]["filter"]
        earliestDetection = """ "%(earliestDetection)s" """ % locals(
        )
        earliestMagnitudeSurvey = """ "%(earliestMagnitudeSurvey)s" """ % locals(
        )
        if earliestMagnitude == None:
            earliestMagnitude = """ null """
            earliestMagnitudeFilter = """ null """
        else:
            earliestMagnitude = """ "%(earliestMagnitude)s" """ % locals(
            )
            earliestMagnitudeFilter = """ "%(earliestMagnitudeFilter)s" """ % locals(
            )

        sqlQuery = u"""
            select lastNonDetectionDate from transientBucket where transientBucketId = %(transientBucketId)s and lastNonDetectionDate is not NULL and lastNonDetectionDate < "%(observationDate)s" and replacedByRowId =0 order by lastNonDetectionDate desc limit 1
        """ % locals()
        tmpRows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        lastNonDetectionDate = "null"
        for tmpRow in tmpRows:
            lastNonDetectionDate = tmpRow["lastNonDetectionDate"]
            lastNonDetectionDate = """ "%(lastNonDetectionDate)s" """ % locals(
            )

        sqlQuery = u"""
            select separationArcsec from sherlock_classifications where transient_object_id = %(transientBucketId)s
        """ % locals()
        tmpRows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        separationArcsec = "null"
        for tmpRow in tmpRows:
            if tmpRow["separationArcsec"]:
                separationArcsec = tmpRow["separationArcsec"]

        sqlQuery = u"""
            SELECT observationDate from transientBucket where transientBucketId = %(transientBucketId)s and observationMJD is not null and observationMJD > 40000 and limitingMag != 1 and replacedByRowId =0 order by observationMJD desc limit 1
        """ % locals()
        tmpRows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        currentMagnitudeDate = "null"
        for tmpRow in tmpRows:
            currentMagnitudeDate = tmpRow["observationDate"]
            currentMagnitudeDate = """ "%(currentMagnitudeDate)s" """ % locals(
            )

        sqlQuery = u"""
            select primaryKeyId, transientBucketId, magnitude from transientBucket where magnitude is not null and magnitude < 90.0 and transientBucketId = %(transientBucketId)s and limitingMag != 1 and replacedByRowId =0 order by magnitude limit 1
        """ % locals()
        tmpRows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        peakMagnitude = "null"
        for tmpRow in tmpRows:
            peakMagnitude = tmpRow["magnitude"]

        sqlQuery = u"""
            select transientRedshift from transientBucket where transientBucketId = %(transientBucketId)s and transientRedshift is not null and replacedByRowId =0 order by observationMJD desc, dateCreated  limit 1;
        """ % locals()
        tmpRows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        bestZ = "null"
        for tmpRow in tmpRows:
            bestZ = tmpRow["transientRedshift"]

        sqlQuery = u"""
            select transientTypePrediction from transientBucket where transientBucketId = %(transientBucketId)s and transientTypePrediction is not NULL and replacedByRowId =0 order by dateCreated desc limit 1
        """ % locals()
        tmpRows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        transientTypePrediction = "null"
        for tmpRow in tmpRows:
            transientTypePrediction = tmpRow["transientTypePrediction"]
            transientTypePrediction = """ "%(transientTypePrediction)s" """ % locals(
            )

        log.info(
            "SURVEYURL, LAST NON-DETECTION, EARLIESTDETECTION, MOST RECENT" % locals())

        # CLASSIFICATION, PEAK MAGNITUDE, ATEL STATUS, BEST REDSHIFT
        # MEASUREMENT
        sqlQuery = """
        select transientBucketId, reducer, classificationWRTMax, classificationPhase, survey, spectralType, dateCreated, observationDate
                        from transientBucket
                        where transientBucketId = %(transientBucketId)s and
                            spectralType is not NULL
                             and replacedByRowId =0
                            order by dateCreated
                            desc limit 1""" % locals()
        extras = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        if len(extras):
            reducer = extras[0]["reducer"]
            reducer = """ "%(reducer)s" """ % locals(
            )
            classificationWRTMax = extras[0]["classificationWRTMax"]
            if classificationWRTMax:
                classificationWRTMax = """ "%(classificationWRTMax)s" """ % locals(
                )
            else:
                classificationWRTMax = """ null """
            classificationPhase = extras[0]["classificationPhase"]
            if classificationPhase:
                classificationPhase = """ "%(classificationPhase)s" """ % locals(
                )
            else:
                classificationPhase = """ null """
            survey = '"' + extras[0]["survey"] + '"'
            spectralType = '"' + extras[0]["spectralType"] + '"'
            classificationDate = extras[0]["observationDate"]
            if classificationDate:
                classificationDate = """ "%(classificationDate)s" """ % locals(
                )
            else:
                classificationDate = """ null """

            classificationAddedDate = extras[0]["dateCreated"]
            classificationAddedDate = """ "%(classificationAddedDate)s " """ % locals(
            )
        else:
            reducer = """ null """
            classificationWRTMax = """ null """
            classificationPhase = """ null """
            survey = """ null """
            spectralType = """ null """
            classificationDate = """ null """
            classificationAddedDate = """ null """

        log.info(
            'CLASSIFICATION, PEAK MAGNITUDE, ATEL STATUS, BEST REDSHIFT MEASUREMENT' % locals())

        # UPDATE ATEL FLAG
        sqlQuery = u"""
            select count(*) as count from transientBucket where transientBucketId= %(transientBucketId)s and name like "%%atel%%" and replacedByRowId =0
        """ % locals()
        tmpRows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        if tmpRows[0]["count"] > 0:
            has_atel = 1
        else:
            has_atel = 0

        lastTBSUpdate = datetime.now()
        lastTBSUpdate = lastTBSUpdate.strftime("%Y-%m-%d %H:%M:%S")
        lastTBSUpdate = """ "%(lastTBSUpdate)s" """ % locals()

        if sherlockClassification:
            sherlockClassification = '"%(sherlockClassification)s"' % locals()
        else:
            sherlockClassification = "null"

        # DETERMINE GALACTIC COORDINATES
        c = SkyCoord(ra=raDeg * u.degree, dec=decDeg * u.degree, frame='icrs')
        l = c.galactic.l.deg
        b = c.galactic.b.deg

        sqlQuery = """
            update transientBucketSummaries set
                    masterName = "%(masterName)s" ,
                    raDeg = %(raDeg)s ,
                    surveyObjectUrl = "%(surveyObjectUrl)s" ,
                    decDeg = %(decDeg)s,
                    currentMagnitude = %(currentMagnitude)s,
                    currentMagnitudeDate = %(currentMagnitudeDate)s,
                    dateAdded = %(dateCreated)s,
                    lastNonDetectionDate = %(lastNonDetectionDate)s ,
                    earliestDetection = %(earliestDetection)s ,
                    earliestMagnitude = %(earliestMagnitude)s ,
                    earliestMagnitudeFilter = %(earliestMagnitudeFilter)s ,
                    peakMagnitude = %(peakMagnitude)s,
                    best_redshift = %(bestZ)s ,
                    transientTypePrediction = %(transientTypePrediction)s,
                    objectAddedToMarshallBy = %(objectAddedToMarshallBy)s,
                    classificationWRTMax = %(classificationWRTMax)s,
                    classificationPhase = %(classificationPhase)s,
                    classificationSurvey = %(survey)s,
                    recentClassification = %(spectralType)s,
                    classificationDate = %(classificationDate)s,
                    classificationAddedBy = %(reducer)s,
                    has_atel = %(has_atel)s,
                    lastTBSUpdate = %(lastTBSUpdate)s ,
                    classificationAddedDate = %(classificationAddedDate)s,
                    sherlockClassification = %(sherlockClassification)s,
                    gLat = %(b)s,
                    gLon = %(l)s,
                    separationArcsec=%(separationArcsec)s
                where transientBucketId = %(transientBucketId)s;
        """ % locals()
        writequery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
        )

        log.info(
            'UPDATE ATEL FLAG' % locals())

        log.debug('<br><br>sqlQuery: %(sqlQuery)s' % locals())

        # 2nd RUN TO FIND THE BEST REDSHIFT -- TRY FOR HOST REDSHIFT THIS TIME
        # (transientRedshift ATTEMPTED ABOVE)
        sqlQuery = u"""
            select hostRedshift from transientBucket where transientBucketId = %(transientBucketId)s and transientRedshift is null and replacedByRowId =0 and hostRedshift is not null limit 1
        """ % locals()
        tmpRows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        hostRedshift = """ null """
        if len(tmpRows):
            hostRedshift = tmpRows[0]["hostRedshift"]
            if not hostRedshift:
                hostRedshift = """ null """

        sqlQuery = """
            update transientBucketSummaries set best_redshift = %(hostRedshift)s where transientBucketId = %(transientBucketId)s and best_redshift is null;
        """ % locals()
        writequery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
        )
        log.info(
            'THE BEST REDSHIFT')

        # 3nd RUN TO FIND THE BEST REDSHIFT -- TRY FOR HOST REDSHIFT FROM
        # SHERLOCK CLASSIFIER THIS TIME
        sqlQuery = u"""
            SELECT z FROM sherlock_crossmatches where transient_object_Id = %(transientBucketId)s and rank = 1;
        """ % locals()
        tmpRows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        hostRedshift = """ null """
        if len(tmpRows):
            hostRedshift = tmpRows[0]["z"]
            if not hostRedshift:
                hostRedshift = """ null """

        sqlQuery = """
            update transientBucketSummaries set best_redshift = %(hostRedshift)s where transientBucketId = %(transientBucketId)s and best_redshift is null;
        """ % locals()
        writequery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
        )
        log.info(
            'THE BEST REDSHIFT')

        # NOW JUST THE BEST HOST REDSHIFT
        # 1st RUN TO FIND THE BEST REDSHIFT -- TRY FOR HOST REDSHIFT THIS TIME
        # (transientRedshift ATTEMPTED ABOVE)
        sqlQuery = u"""
            select hostRedshift from transientBucket where transientBucketId = %(transientBucketId)s and transientRedshift is null and hostRedshift is not null  and replacedByRowId =0 limit 1
        """ % locals()
        tmpRows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        hostRedshift = """ null """
        if len(tmpRows):
            hostRedshift = tmpRows[0]["hostRedshift"]
            if not hostRedshift:
                hostRedshift = """ null """

        sqlQuery = """
            update transientBucketSummaries set host_redshift = %(hostRedshift)s where transientBucketId = %(transientBucketId)s and host_redshift is null;
        """ % locals()
        writequery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
        )
        log.info(
            'THE BEST REDSHIFT')

        # 2nd RUN TO FIND THE BEST REDSHIFT -- TRY FOR HOST REDSHIFT FROM
        # SHERLOCK CLASSIFIER THIS TIME
        sqlQuery = u"""
            SELECT z FROM sherlock_crossmatches where transient_object_Id = %(transientBucketId)s and rank = 1;
        """ % locals()
        tmpRows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        hostRedshift = """ null """
        if len(tmpRows):
            hostRedshift = tmpRows[0]["z"]
            if not hostRedshift:
                hostRedshift = """ null """

        sqlQuery = """
            update transientBucketSummaries set host_redshift = %(hostRedshift)s where transientBucketId = %(transientBucketId)s and host_redshift is null;
        """ % locals()
        writequery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
        )
        log.info(
            'THE BEST REDSHIFT')

        # NOW ADD DISTANCE MEASUREMENTS AND ASOLUTE MAGS
        redshift = None
        sqlQuery = """
            select best_redshift from transientBucketSummaries where transientBucketId = %(transientBucketId)s and best_redshift is not null
        """ % locals()
        thisResult = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            quiet=False
        )
        if len(thisResult) > 0:
            log.debug('TRANSIENT REDSHIFT')
            redshift = thisResult[0]["best_redshift"]
            log.debug('redshift = %(redshift)s' % locals())

        peakMag = None
        distanceMpc = None
        if redshift is not None and redshift > 0.00000001:

            # FIND THE BRIGHTEST MAGNITUDE
            sqlQuery = """
                select peakMagnitude from transientBucketSummaries where transientBucketId = %(transientBucketId)s
            """ % locals()
            peakMag = readquery(
                log=log,
                sqlQuery=sqlQuery,
                dbConn=dbConn,
                quiet=False
            )
            peakMag = peakMag[0]["peakMagnitude"]

            log.debug('<br><br>sqlQuery: %(sqlQuery)s' % locals())

            if peakMag is not None:
                # Determine distance
                dd = dat.convert_redshift_to_distance(
                    z=float(redshift)
                )
                distanceMpc = dd['dl_mpc']
                dmod = dd['dmod']

                abspeakMag = peakMag - dmod
                log.debug('transientBucketId: %s' %
                          (row['transientBucketId'],))
                log.debug('distanceMpc: %s' % (distanceMpc,))
                log.debug('dmod: %s' % (dmod,))
                log.debug('peakMag: %s' % (peakMag,))
                log.debug('abspeakMag: %s' % (abspeakMag,))

        if peakMag and distanceMpc:
            sqlQuery = """
                update transientBucketSummaries set absolutePeakMagnitude = %s, distanceMpc = %s where transientBucketId = %s
            """ % (abspeakMag, distanceMpc, row['transientBucketId'],)
            writequery(
                log=log,
                sqlQuery=sqlQuery,
                dbConn=dbConn,
            )
            log.info(
                'DISTANCE MEASUREMENTS AND ASOLUTE MAGS')

    log.debug('finished flag updater')

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
