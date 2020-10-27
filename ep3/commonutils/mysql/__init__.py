#!/usr/local/bin/python
# encoding: utf-8
"""
*This module is a collection of MySQL related objects created specifically for the PESSTO Marshall*

:Author:
    David Young

:Notes:
    - If you have any questions requiring this script please email me: davidrobertyoung@gmail.com

.. todo::
    
    @review: when complete, extract all code out of the main function and add cl commands
    @review: make internal function private
    @review: pull all general functions and classes into dryxPythonModules
"""
from __future__ import print_function
from builtins import str
from builtins import object
import sys
import os
from fundamentals.mysql import readquery, writequery

def main():
    """
    *Debugger*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    

    **Return**

    - ``None``
    
    """
    import pesstoMarshallPythonPath as pp
    pp.set_python_path()
    import pmCommonUtils as p

    ################ > SETUP ##################
    # SETUP DB CONNECTION AND A LOGGER
    dbConn, log = p.settings()
    ## START LOGGING ##
    log.info('----- STARTING TO RUN THE commonutils.mysql -----')
    add_object_comment(dbConn, log, 1, "dryx", """<a href="%s">%s</a>""")

    ################ > VARIABLE SETTINGS ######
    ################ >ACTION(S) ###############

    dbConn.commit()
    dbConn.close()
    ## FINISH LOGGING ##
    log.info('----- FINISHED ATTEMPT TO RUN THE commonutils.mysql -----')
    return

##########################################################################
# CLASSES                                                                                   #
##########################################################################
# LAST MODIFIED : October 10, 2012
# CREATED : October 10, 2012
# AUTHOR : DRYX

class get_pessto_objects_with_flagset(object):
    """

    *A MySQL query builder geared towards finding objects at a specific workflow location in the PESSTO Marshall

    Variable Attributes:
            - ``mwfFlag`` -- marshall workflow location (e.g. "inbox", "queued for classification" ...)
            - ``awfFlag`` -- alert workflow location (e.g. "Pending Classification", "alert released" ...)
            - ``cFlag`` -- classified flag, boolean
            - ``magic`` -- definition
            - ``pageMax`` -- maximum number of tickets on the html page
            - ``pageIndex`` -- which ticket in the array to display first
            - ``sortBy`` -- parameter to sort the tickets by (e.g. abs mag)
            - ``sortDir`` -- sort direction, desc or asc
            - ``search`` -- term to search for (e.g. object name)*
    """
    ################ PUBLIC VARIABLE ATTRIBUTES ################
    mwfFlag = None
    awfFlag = None
    cFlag = None
    magic = None
    pageMax = "40"
    pageIndex = "0"
    sortBy = "p.pesstoObjectsId DESC"
    sortDir = "DESC"
    search = None

    pesstoObjectsColumns = ['pesstoObjectsId']
    transientBucketColumns = []
    qWhere = "p.transientBucketId=t.transientBucketId"

    ############### PRIVATE VARIABLE ATTRIBUTES ###############
    ################ INSTANSIATION METHOD ######################
    def __init__(self, dbConn, log):
        self.dbConn = dbConn
        self.log = log

        return

    ############### METHODS ####################################
    # LAST MODIFIED : August 10, 2012
    # CREATED : August 10, 2012
    # AUTHOR : DRYX
    def get_rows(self, dbConn, log):
        """
        *Method called to return the db rows found by executing the mysql query built by this class*

        **Key Arguments**

        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        -
        

        **Return**

        - ``None``
        
        """
        import operator

        ################ > VARIABLE SETTINGS ######

        ################ >ACTION(S) ################
        # AMEND _SELECT_ CLAUSE TO INCLUDE THE LIST OF COLUMNS #
        columnList = []
        for item in self.pesstoObjectsColumns:
            newItem = "p." + item
            columnList.append(newItem)

        for item in self.transientBucketColumns:
            newItem = "t." + item
            columnList.append(newItem)

        # CONVERT LIST TO STRING
        columnListAsString = ', '.join(columnList)
        qSelect = columnListAsString

        # AMEND _WHERE_ CLAUSE TO INCLUDE WORKFLOW LOCATION FLAGS #
        if(self.mwfFlag == None):
            extraWhere = ""
        else:
            extraWhere = """ AND (p.marshallWorkflowLocation=%s)""" % (
                self.mwfFlag,)

        if(self.awfFlag == None):
            extraWhere += ""
        else:
            extraWhere += " AND (p.alertWorkflowLocation=%s)""" % (
                self.awfFlag,)

        if(self.cFlag == None):
            extraWhere += ""
        elif(self.cFlag == 1):
            extraWhere += " AND (p.classifiedFlag=1)"
        else:
            extraWhere += " AND (p.classifiedFlag=0)"

        #### AMEND _WHERE_ CLAUSE AND _ORDER BY_ CLAUSES TO INCLUDE SORT & SEARCH TOKENS ####
        #-----------------------------------------------------------------------------------#
        qFrom = "pesstoObjects p, transientBucket t"

        if(self.search != None):
            # extraWhere += """ AND p.pesstoObjectsId in
            #                                     (
            #                                         SELECT transientBucketId
            #                                         FROM transientBucket
            #                                         WHERE name like "%%%s%%"
            #                                     )""" % (self.search,)
            qFrom = """pesstoObjects p, (SELECT DISTINCT transientBucketId
                            FROM transientBucket
                            WHERE name like "%%%s%%") as t""" % (self.search,)

        idAkas = ['id', 'name', 'object id', 'identity']
        if any(self.sortBy == s for s in idAkas):
            extraWhere += " AND t.name is not NULL"
            self.sortBy = "t.name"

        raAkas = ['ra']
        if any(self.sortBy == s for s in raAkas):
            extraWhere += " AND t.raDeg is not NULL"
            self.sortBy = "t.raDeg"

        decAkas = ['dec']
        if any(self.sortBy == s for s in decAkas):
            extraWhere += " AND t.decDeg is not NULL"
            self.sortBy = "t.decDeg"

        predictionAkas = ['prediction']
        if any(self.sortBy == s for s in predictionAkas):
            extraWhere += " AND t.transientTypePrediction is not NULL"
            self.sortBy = "t.transientTypePrediction"

        transientBucketId = ['transientBucketId']
        if any(self.sortBy == s for s in transientBucketId):
            self.sortBy = "t.transientBucketId DESC"

        classificationAkas = [
            'classification', 'class', 'spectral type', 'type']
        if any(self.sortBy == s for s in classificationAkas):
            extraWhere += " AND t.spectralType is not NULL"
            self.qWhere = "p.transientBucketId=t.transientBucketId"
            self.sortBy = "t.spectralType"

        # MORE INVOLED SORT FUNCTIONS REQUIRE THAT THE qFrom & qWhere VARIABLE
        # IS EDITED ##
        magnitudeAkas = ['magnitude', 'mag', 'apparent magnitude', 'app mag']
        if any(self.sortBy == s for s in magnitudeAkas):
            qFrom = """pesstoObjects p,
                                    (
                                            SELECT * from
                                            (
                                                SELECT *
                                                from transientBucket
                                                WHERE magnitude is not NULL
                                                ORDER BY transientBucketId, observationMjd DESC
                                            ) AS t
                                            GROUP BY t.transientBucketId
                                    ) AS t"""
            self.qWhere = "p.transientBucketId=t.transientBucketId"
            self.sortBy = "magnitude"

        distanceAkas = ['z', 'distance', 'mpc', 'redshift']
        if any(self.sortBy == s for s in distanceAkas):
            qFrom = """pesstoObjects p,
                                    (
                                        SELECT * from transientBucket
                                        WHERE (transientRedshift is not NULL AND
                                                transientRedshift != 0)
                                            OR (hostRedshift is not NULL AND
                                                hostRedshift != 0)
                                        ORDER BY transientBucketId, transientRedshift DESC, observationMjd
                                    ) AS t"""
            self.qWhere = "p.transientBucketId=t.transientBucketId"
            self.sortBy = "GREATEST(COALESCE(t.transientRedshift, 0),COALESCE(t.hostRedshift, 0))"

        mjdAkas = ['mjd', 'obsmjd', 'obs mjd', 'obs date', 'obsdate',
                   'last observed', 'recent obs', 'recent observation']
        if any(self.sortBy == s for s in mjdAkas):
            qFrom = """pesstoObjects p,
                                    (
                                        SELECT * from
                                        (
                                            SELECT * from transientBucket
                                            WHERE observationMJD is not NULL
                                            ORDER BY transientBucketId, observationMJD DESC
                                        ) AS t
                                        GROUP BY t.transientBucketId
                                    ) AS t"""
            self.qWhere = "p.transientBucketId=t.transientBucketId"
            self.sortBy = "observationMJD DESC"

        # SUPER-COMPLICATED ABS MAG QUERY
        absMagAka = [
            'peak abs mag', 'peak absolute magnitude', 'abs mag', 'absolute magnitude']
        if any(self.sortBy == s for s in absMagAka):
            objectDict = {}
            # GRAB ALL transientObjectIds
            try:
                log.debug(
                    "attempting to list all of the transientBucket object IDs")
                rows = list_all_transientBucketIds(dbConn, log)
            except Exception as e:
                log.error(
                    "could not list all of the transientBucket object IDs - failed with this error %s: " % (str(e),))
                return -1

            for row in rows:
                absMagValue = ""
                #### GRAB THE PEAK MAG FOR THE OBJECT ####
                try:
                    log.debug("attempting to grab the peak mag for transient %s" % (
                        row['transientBucketId'],))
                    peakMag = get_object_peak_mag(
                        dbConn, log, row['transientBucketId'])
                except Exception as e:
                    log.error("could not grab the peak mag for transient %s - failed with this error %s: " % (
                        row['transientBucketId'], str(e),))
                    return -1

                #### GRAB THE LATEST CLASSIFICATION DATA FOR OBJECT ####
                try:
                    log.debug("attempting to get the classification data for transient %s" % (
                        row['transientBucketId'],))
                    classificationList = get_object_classification_data(
                        dbConn, log, row['transientBucketId'])
                except Exception as e:
                    log.error("could not get the classification data for transient %s - failed with this error %s: " % (
                        row['transientBucketId'], str(e),))
                    return -1

                # CALCUATE DISTANCE MODULUS USING THE TRANSIENT REDSHIFT
                transientDistanceModulus = False
                if(len(classificationList) > 0 and classificationList[0]['transientRedshift']):
                    try:
                        log.debug(
                            "attempting to calcuate distance using the transient redshift")
                        dd = u.redshiftToDistance(
                            classificationList[0]['transientRedshift'])
                        transientDistanceModulus = dd['dmod']
                    except Exception as e:
                        log.error(
                            "could not calcuate distance using the transient redshift - failed with this error %s: " % (str(e),))
                        return -1

                # GRAB THE DISCOVERY DATA FOR transientBucketId
                try:
                    log.debug("attempting to get the discovery data for the transient %s" % (
                        row['transientBucketId'],))
                    tbData = get_object_transientBucket_discovery_data(
                        dbConn, log, row['transientBucketId'])
                except Exception as e:
                    log.error("could not get the discovery data for the transient %s - failed with this error %s: " % (
                        row['transientBucketId'], str(e),))
                    return -1

                # CALCUATE THE DISTANCE
                dm = False
                if(transientDistanceModulus):
                    dm = transientDistanceModulus
                elif(tbData[0]['hostRedshift']):
                    # IF NO TRANSIENT REDSHIFT THEN DEFAULT TO HOST REDSHIFT
                    try:
                        log.debug(
                            "attempting to convert a redshift to distance")
                        dd = u.redshiftToDistance(tbData[0]['hostRedshift'])
                        dm = dd['dmod']
                    except Exception as e:
                        log.error(
                            "could not convert a redshift to distance - failed with this error %s: " % (str(e),))
                        return -1

                # CALCULATE THE ABS MAG
                if(dm and peakMag):
                    absMagValue = peakMag - dm
                    objectDict[str(row['transientBucketId'])] = absMagValue

            # FORM A PEAK ABS MAG SORTED LIST OF TUPLES FROM objectDict
            sorted_mags = sorted(
                list(objectDict.items()), key=operator.itemgetter(1))

            objectList = ""
            i = 0
            for item in sorted_mags:
                if(i == 0):
                    objectList += str(item[0])
                    i += 1
                else:
                    objectList += "," + str(item[0])

            extraWhere = " AND t.transientBucketId in (" + objectList + ")"
            self.sortBy = " FIELD(t.transientBucketId, " + objectList + ")"

        # BUILD QUERY
        sqlQuery = """SELECT %s
                                        FROM %s
                                        WHERE %s %s
                                        GROUP BY p.transientBucketId
                                        ORDER BY %s
                                        LIMIT %s, %s""" % (qSelect, qFrom, self.qWhere, extraWhere, self.sortBy, self.pageIndex, self.pageMax,)

        try:
            log.debug(
                "attempting to execute the sql query built to generate the marshall workflow table")
            rows = readquery(log=log, sqlQuery=sqlQuery, dbConn=dbConn)
        except Exception as e:
            log.error(
                "could not execute the sql query built to generate the marshall workflow table - failed with this error: %s\n Here's the SQL query:\n %s " %
                (str(e), sqlQuery,))
            return -1

        return rows

##########################################################################
# PUBLIC FUNCTIONS                                                                          #
##########################################################################
# LAST MODIFIED : December 11, 2012
# CREATED : December 11, 2012
# AUTHOR : DRYX
def get_object_peak_mag(dbConn, log, transientBucketId):
    """
    *Get the peak apparent magnitude for a specific object*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    - ``transientBucketId`` -- object ID
    

    **Return**

    - peakMag
    
    """

    ################ > VARIABLE SETTINGS ######

    ################ >ACTION(S) ################
    #### GRAB THE PEAK MAG FOR THE OBJECT ####
    sqlQuery = """SELECT magnitude
                                FROM transientBucket
                                WHERE transientBucketId = %s AND
                                    magnitude is not NULL
                                ORDER BY magnitude LIMIT 1""" % (transientBucketId,)

    try:
        log.debug(
            "attempting to grab the brightest magnitude for transientBucketId %s" %
            (transientBucketId,))
        magList = readquery(log=log, sqlQuery=sqlQuery, dbConn=dbConn)
    except Exception as e:
        log.error(
            "could not grab the brightest magnitude for transientBucketId %s - failed with this error %s: " %
            (transientBucketId, str(e),))
        return -1

    if(len(magList) != 0):
        peakMag = magList[0]['magnitude']
    else:
        peakMag = None

    return peakMag

# LAST MODIFIED : December 11, 2012
# CREATED : December 11, 2012
# AUTHOR : DRYX

def list_all_transientBucketIds(dbConn, log):
    """
    *Get a complete list of *transientBucketIds* for all the PESSTO objects*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    

    **Return**

    - ``rows`` -- rows returned by the query
    
    """

    ################ > VARIABLE SETTINGS ######
    sqlQuery = """SELECT DISTINCT transientBucketId
                            from pesstoObjects"""

    ################ >ACTION(S) ################
    try:
        log.debug("attempting to run a mysql query")
        rows = readquery(log=log, sqlQuery=sqlQuery, dbConn=dbConn)
    except Exception as e:
        log.error("could not run a mysql query - failed with this error %s: " %
                  (str(e),))
        return -1

    return rows

# LAST MODIFIED : December 11, 2012
# CREATED : December 11, 2012
# AUTHOR : DRYX

def get_object_classification_data(dbConn, log, transientBucketId):
    """
    *Get a list of classification data for a specific object*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    - ``transientBucketId`` -- object ID
    

    **Return**

    - ``classificationList`` -- list of classification data for the specific object
    
    """
    ################ > VARIABLE SETTINGS ######
    sqlQuery = """SELECT spectralType, survey, observationDate, transientRedshift
                                FROM transientBucket
                                WHERE (transientBucketId = %s AND
                                    spectralType is not NULL)
                                GROUP BY survey""" % (transientBucketId,)

    ################ >ACTION(S) ################
    try:
        log.debug(
            "attempting to grab the latest transient redshift for transientBucketId %s" %
            (transientBucketId,))
        classificationList = readquery(
            log=log, sqlQuery=sqlQuery, dbConn=dbConn)
    except Exception as e:
        log.error(
            "could not grab the latest transient redshift for transientBucketId %s - failed with this error %s: " %
            (transientBucketId, str(e),))
        return -1

    return classificationList

# LAST MODIFIED : December 11, 2012
# CREATED : December 11, 2012
# AUTHOR : DRYX

def get_object_transientBucket_discovery_data(
        dbConn,
        log,
        transientBucketId):
    """
    *Get a list of the discovery data for a specific object*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    - ``transientBucketId`` -- object ID
    

    **Return**

    - ``discoveryData`` -- a list of dictionaries
    
    """

    ################ > VARIABLE SETTINGS ######
    sqlQuery = """SELECT *
                                FROM transientBucket
                                WHERE transientBucket.masterIDFlag = 1 AND
                                    transientBucket.transientBucketId = %s""" % (transientBucketId,)
    ################ >ACTION(S) ################
    try:
        log.debug(
            "attempting to grab the discovery data for transientBucketId %s" %
            (transientBucketId,))
        log.debug("Here is the query %s" %
                  (sqlQuery,))
        discoveryData = readquery(log=log, sqlQuery=sqlQuery, dbConn=dbConn)
    except Exception as e:
        log.error(
            "could not grab the discovery data for transientBucketId %s - failed with this error %s: " %
            (str(e),))
        return -1

    return discoveryData

# LAST MODIFIED : December 11, 2012
# CREATED : December 11, 2012
# AUTHOR : DRYX
def get_object_last_non_detection(dbConn, log, transientBucketId):
    """
    *Get the latest observation of a specific object*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    - ``transientBucketId`` -- object ID
    

    **Return**

    - ``rows`` -- returned from mySQL query
    
    """

    ################ > VARIABLE SETTINGS ######
    sqlQuery = """SELECT lastNonDetectionDate
                                FROM transientBucket
                                WHERE lastNonDetectionDate is not NULL AND
                                    transientBucket.transientBucketId = %s
                                LIMIT 0,1""" % (transientBucketId,)

    ################ >ACTION(S) ################
    try:
        log.debug("attempting to find the last non-detection for %s" %
                  (transientBucketId,))
        rows = readquery(log=log, sqlQuery=sqlQuery, dbConn=dbConn)
    except Exception as e:
        log.error(
            "could not find the last non-detection for %s - failed with this error %s: " %
            (transientBucketId, str(e),))
        return -1

    return rows

# LAST MODIFIED : December 11, 2012
# CREATED : December 11, 2012
# AUTHOR : DRYX

def get_object_pesstoObjects_discovery_data(dbConn, log, transientBucketId):
    """
    *Get data in the pesstoObjects DB table for a specific object*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    - transientBucketId
    

    **Return**

    - ``rows`` -- returned from mySQL query
    
    """

    ################ > VARIABLE SETTINGS ######
    sqlQuery = """SELECT *
                                FROM pesstoObjects
                                WHERE pesstoObjects.transientBucketId = %s""" % (transientBucketId,)

    ################ >ACTION(S) ################
    try:
        log.debug(
            "attempting to find the discovery data for transientBucketId %s" %
            (transientBucketId,))
        rows = readquery(log=log, sqlQuery=sqlQuery, dbConn=dbConn)
    except Exception as e:
        log.error(
            "could not find the discovery data for transientBucketId %s - failed with this error %s: " %
            (transientBucketId, str(e),))
        return -1

    return rows

# LAST MODIFIED : December 11, 2012
# CREATED : December 11, 2012
# AUTHOR : DRYX

def get_object_akas(
        dbConn,
        log,
        transientBucketId):
    """
    *Get a list of AKAs for a specific object*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    - ``transientBucketId`` -- object Id
    

    **Return**

    - ``rows`` -- returned from mySQL query
    
    """

    ################ > VARIABLE SETTINGS ######
    sqlQuery = """SELECT DISTINCT name, surveyObjectUrl
                                FROM transientBucket
                                WHERE masterIDFlag = 0 AND
                                    transientBucketId = %s AND
                                    name not like "atel_%%" """ % (transientBucketId,)

    ################ >ACTION(S) ################
    try:
        log.debug("attempting to find the akas for transientBucketId %s" %
                  (transientBucketId,))
        rows = readquery(log=log, sqlQuery=sqlQuery, dbConn=dbConn)
    except Exception as e:
        log.error(
            "could not find the akas for transientBucketId %s - failed with this error %s: " %
            (transientBucketId, str(e),))
        return -1

    return rows

# LAST MODIFIED : December 11, 2012
# CREATED : December 11, 2012
# AUTHOR : DRYX
def get_object_mag_history(dbConn, log, transientBucketId):
    """
    *Get a date-order magnitude history for a specific object*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    - ``transientBucketId`` -- object Id
    

    **Return**

    - ``rows`` -- returned from mySQL query
    
    """

    ################ > VARIABLE SETTINGS ######
    sqlQuery = """SELECT magnitude, filter, observationDate, survey
                                FROM transientBucket
                                WHERE (transientBucketId = %s AND magnitude is not NULL)
                                ORDER BY observationDate DESC""" % (transientBucketId,)

    ################ >ACTION(S) ################
    try:
        log.debug(
            "attempting to find date-order magnitude history for transientBucketId %s" %
            (transientBucketId,))
        rows = readquery(log=log, sqlQuery=sqlQuery, dbConn=dbConn)
    except Exception as e:
        log.error(
            "could not find date-order magnitude history for transientBucketId %s - failed with this error %s: " %
            (transientBucketId, str(e),))
        return -1

    return rows

# LAST MODIFIED : December 11, 2012
# CREATED : December 11, 2012
# AUTHOR : DRYX
def get_object_comments(dbConn, log, pesstoObjectsId):
    """
    *Get all comments for a specific object*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    - ``transientBucketId`` -- object Id
    

    **Return**

    - ``rows`` -- returned from mySQL query
    
    """

    ################ > VARIABLE SETTINGS ######
    sqlQuery = """SELECT comment, commentAuthor, dateCreated
                                FROM pesstoObjectsComments
                                WHERE pesstoObjectsId = %s
                                ORDER BY dateCreated DESC""" % (pesstoObjectsId,)

    ################ >ACTION(S) ################
    try:
        log.debug("attempting to find all comments for pesstoObjectsId %s" %
                  (pesstoObjectsId,))
        rows = readquery(log=log, sqlQuery=sqlQuery, dbConn=dbConn)
    except Exception as e:
        log.error(
            "could not find all comments for pesstoObjectsId %s - failed with this error %s: " %
            (pesstoObjectsId, str(e),))
        return -1

    return rows

# LAST MODIFIED : December 8, 2012
# CREATED : December 8, 2012
# AUTHOR : DRYX

def insert_changelog_entry(dbConn, log, transientBucketId, comment):
    """
    *Insert an entry into the pessto marshall changelog db table.*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    - transientBucketId
    - ``comment`` -- comment about what changed
    

    **Return**

    - ``None``
    
    """

    ################ > VARIABLE SETTINGS ######
    sqlQuery = """INSERT INTO pesstoObjectsChangeLog (
                                pesstoObjectsId,
                                whatWasChanged,
                                whenChangeOccured,
                                changeAuthor)
                                VALUES (%s, '%s', NOW(), 'marshall')
                        """ % (transientBucketId, comment,)
    ################ >ACTION(S) ################
    try:
        log.debug(
            "attempting to add an entry into the marshall changelog db table")
        writequery(sqlQuery=sqlQuery, dbConn=self.dbConn, log=self.log)
    except Exception as e:
        log.error(
            "could not add an entry into the marshall changelog db table - failed with this error %s: " %
            (str(e),))
        return -1

    return None

# LAST MODIFIED : December 10, 2012
# CREATED : December 10, 2012
# AUTHOR : DRYX
def change_pesstoObjects_flag(dbConn, log, pesstoObjectsId, flag, value):
    """
    *Convert a flag in the ``pesstoObjects`` db table to shift the object through the various marshall workflows.
    A comment is also added to the marshall changelog table.*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    - ``pesstoObjectsId`` -- ID of the object in the pesstoObjects table
    - ``flag`` -- the flag that needs changed
    - ``value`` -- value the flag is to be changed to
    

    **Return**

    - ``None``
    
    """

    ################ > VARIABLE SETTINGS ######

    ################ >ACTION(S) ################
    currentFlagQuery = """SELECT alertWorkflowLocation
                                                FROM pesstoObjects
                                                WHERE pesstoObjectsId = %s """ % (pesstoObjectsId,)
    try:
        log.debug("attempting to find %s flag for pesstoObjectsId %s" %
                  (flag, pesstoObjectsId,))

        currentFlag = readquery(
            log=log, sqlQuery=currentFlagQuery, dbConn=dbConn)
    except Exception as e:
        log.error(
            "could not find %s flag for pesstoObjectsId %s - failed with this error %s: " %
            (flag, str(e),))
        return -1

    try:
        log.debug("attempting to creating MySQL Queries")
        # CHANGE OF FLAG
        sqlQuery = """UPDATE pesstoObjects
                                SET %s = "%s", snoozed = 0
                                WHERE pesstoObjectsId = %s""" % (flag, value, pesstoObjectsId, )
        #log.info('sqlQuery %s' % (sqlQuery,))
        # CHANGE dateLastModified
        sqlQuery2 = """UPDATE pesstoObjects
                                SET dateLastModified = NOW()
                                WHERE pesstoObjectsId = %s""" % (pesstoObjectsId,)
        #log.info('sqlQuery2 %s' % (sqlQuery2,))
        # UPDATE CHANGELOG TABLE
        sqlQuery3 = """INSERT INTO pesstoObjectsChangeLog
                                        (pesstoObjectsId,whatWasChanged,whenChangeOccured,changeAuthor)
                                        VALUES (%s,"%s changed from %s to %s",NOW(),'web user')""" % (pesstoObjectsId, flag, currentFlag, value,)
        #log.info('sqlQuery3 %s' % (sqlQuery3,))

    except Exception as e:
        log.error(
            "could not creating MySQL Queries - failed with this error %s: " %
            (str(e),))
        return -1

    try:
        log.debug(
            "attempting to update the alert workflow flag for pesstoObjectsId %s" %
            (pesstoObjectsId,))
        writequery(sqlQuery=sqlQuery, dbConn=self.dbConn, log=self.log)
        writequery(sqlQuery=sqlQuery2, dbConn=self.dbConn, log=self.log)
        writequery(sqlQuery=sqlQuery3, dbConn=self.dbConn, log=self.log)
    except Exception as e:
        log.error(
            "could not update the alert workflow flag for pesstoObjectsId %s - failed with this error %s: " %
            (pesstoObjectsId, str(e),))
        return -1

    return None

# LAST MODIFIED : December 11, 2012
# CREATED : December 11, 2012
# AUTHOR : DRYX
def count_objects_with_flagset(
        dbConn,
        log,
        mwfFlag=None,
        awfFlag=None,
        cFlag=None):
    """
    *Get the count of the number of objects in the Marshall with a specific flagset*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    - ``mwfFlag`` -- marshall workflow location
    - ``awfFlag`` -- alert workflow location
    - ``cFlag`` -- classification flag
    

    **Return**

    - ``count`` -- the number of objects with the specified flagset
    
    """
    ################ > VARIABLE SETTINGS ######

    ################ >ACTION(S) ################
    sqlQuery = """select count from meta_workflow_lists_counts where 1=1 """

    # AMEND WHERE CLAUSE TO INCLUDE WORKFLOW LOCATION FLAGS #
    extraWhere = ""
    if(mwfFlag != None):
        extraWhere = """%(extraWhere)s AND listName= %(mwfFlag)s """ % locals()

    if(awfFlag != None):
        extraWhere = """%(extraWhere)s AND listName= %(awfFlag)s """ % locals()

    if(cFlag != None):
        extraWhere = """%(extraWhere)s  AND listName = "classified" """ % locals(
        )

    sqlQuery = """%(sqlQuery)s %(extraWhere)s;""" % locals()

    try:
        log.debug(
            "attempting to find the number of rows in the db with a specified flagset")
        rows = readquery(log=log, sqlQuery=sqlQuery, dbConn=dbConn)
        # print sqlQuery + "<br>"
        count = 0
        for row in rows:
            count += row["count"]
    except Exception as e:
        log.error(
            "could not find the number of rows in the db with a specified flagset - failed with this error %s: " %
            (str(e),))
        log.debug('sqlQuery: %(sqlQuery)s' % locals())
        sys.exit(0)

    return count

# LAST MODIFIED : January 7, 2013
# CREATED : January 7, 2013
# AUTHOR : DRYX

def add_object_comment(dbConn, log, transientBucketId, author, comment):
    """
    *Add a comment to the ``pesstoObjectsComments`` table.*

    **Key Arguments**

    
      - ``dbConn`` -- mysql database connection
      - ``log`` -- logger
      - ``paras`` -- FieldStorage parameters passed via the URL
      - ``transientBucketId`` -- the transientBucketId
      - ``author`` -- the author of the comment
      - ``comment`` -- comment

    **Return**

    
      - None
    """
    ## STANDARD LIB ##
    import sys
    import os
    import cgi as c
    ## LOCAL APPLICATION ##
    import ep3.commonutils.mysql as pm

    ################ > VARIABLE SETTINGS ######

    ################ >ACTION(S) ################
    # ENCODE COMMENT
    try:
        log.debug("attempting to encode the comment")
        comment = c.escape(comment, quote=True)
    except Exception as e:
        log.error("could not encode the comment - failed with this error: %s " %
                  (str(e),))
        return -1

    # INSERT COMMENT
    sqlQuery = """INSERT INTO pesstoObjectsComments (pesstoObjectsId,
                                                    dateCreated,
                                                    dateLastModified,
                                                    commentAuthor,
                                                    comment
                                                  )
                VALUES (%s,NOW(),NOW(),"%s","%s")""" % (transientBucketId, author, comment,)

    try:
        log.debug(
            "attempting to add a comment to the pessto marshall database")
        writequery(sqlQuery=sqlQuery, dbConn=self.dbConn, log=self.log)
    except Exception as e:
        log.error(
            "could not add a comment to the pessto marshall database - failed with this error: %s " %
            (str(e),))
        return -1

    return

# LAST MODIFIED : January 21, 2013
# CREATED : January 21, 2013
# AUTHOR : DRYX
def record_module_running_time(dbConn,
                               log,
                               startTime,
                               endTime,
                               runningTime,
                               moduleName):
    """
    *Record the running times of a python module into the pessto marshall database - can be used later for efficiency checks*

    **Key Arguments**

    
      - ``dbConn`` -- mysql database connection
      - ``log`` -- logger
      - ``moduleName`` -- name of the module that has executed
      - ``startTime`` -- starting datetime
      - ``endTiming`` -- end datetime
      - ``runningTime`` -- total time taken to run

    **Return**

    
      - None
    """
    ## STANDARD LIB ##
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##

    ################ > VARIABLE SETTINGS ######

    ################ >ACTION(S) ################
    sqlQuery = """INSERT INTO logs_executable_timings (module_name,
                                                    start_time,
                                                    end_time,
                                                    running_time)
                        VALUES ("%s", "%s", "%s", "%s")""" % (moduleName, startTime, endTime, runningTime,)

    try:
        log.debug(
            "attempting to add an entry into the execution logs db table")
        writequery(sqlQuery=sqlQuery, dbConn=self.dbConn, log=self.log)
    except Exception as e:
        log.error(
            "could not add an entry into the executions logs db table - failed with this error: %s " %
            (str(e),))
        return -1

    return

# LAST MODIFIED : January 25, 2013
# CREATED : January 25, 2013
# AUTHOR : DRYX

def get_workflow_flags_for_object(dbConn, log, transientBucketId):
    """
    *Get the Marshall Workflow flags (marshall location, alert location, classification) for a given transientBucketId*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    - ``transientBucketId`` -- the objects transientBucketId
    

    **Return**

    - None
    
    """
    ## STANDARD LIB ##
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##

    ################ > VARIABLE SETTINGS ######

    ################ >ACTION(S) ################
    x = 1

    return

# LAST MODIFIED : January 30, 2013
# CREATED : January 30, 2013
# AUTHOR : DRYX

def get_pessto_classified_object_counts(dbConn, log, typeList):
    """
    *Return the number of objects in the Marshall DB that have been classified by PESSTO and are found in the ``typeList``*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    - ``typeList`` -- the types of objects you wish to count.
    

    **Return**

    - None
    
    """
    ## STANDARD LIB ##
    import sys
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##

    ################ > VARIABLE SETTINGS ######
    spectralTypes = """spectralType = 9238238424"""
    for item in typeList:
        if "%" in item:
            spectralTypes += """ or spectralType like "%s" """ % (item, )
        else:
            spectralTypes += """ or spectralType = "%s" """ % (item, )
        log.debug('spectralTypes %s' % (spectralTypes,))

    sqlQuery = """SELECT count(*)
                    FROM (
                        (SELECT DISTINCT transientBucketId
                            FROM transientBucket
                            WHERE spectralType is NOT NULL AND
                                survey like '%%pessto%%' AND
                                (%s)) as s)
                """ % (spectralTypes, )

    ################ >ACTION(S) ################
    try:
        log.debug(
            "attempting to count the number of objects classified by pessto with the types %s" %
            (typeList,))
        rows = readquery(log=log, sqlQuery=sqlQuery, dbConn=dbConn)
    except Exception as e:
        log.error(
            "could not count the number of objects classified by pessto with the types %s - failed with this error: %s " %
            (typeList, str(e),))
        print(str(e))
        sys.exit(0)

    count = rows[0]["count(*)"]

    return count

# LAST MODIFIED : December 11, 2012
# CREATED : December 11, 2012
# AUTHOR : DRYX
# def name(dbConn, log, transientBucketId):
#   """Get ______ for a specific object

#   **Key Arguments:**
#     - ``dbConn`` -- mysql database connection
#     - ``log`` -- logger
#     - ``transientBucketId`` -- object Id

#   **Return:**
#     - ``rows`` -- returned from mySQL query
#   """

#   ################ > VARIABLE SETTINGS ######
#   sqlQuery = """
#                   transientBucketId = %s """ % (transientBucketId,))

#   ################ >ACTION(S) ################
#   try:
#     log.debug("attempting to find ____ for transientBucketId %s" % (transientBucketId,))
#     rows = m.execute_mysql_read_query(sqlQuery,dbConn, log)
#   except Exception as e:
#     log.error("could not find ____ for transientBucketId %s - failed with this error %s: " % (transientBucketId,str(e),))
#     return -1

#   return rows

##########################################################################
# PRIVATE (HELPER) FUNCTIONS                                                                #
##########################################################################
if __name__ == '__main__':
    main()
