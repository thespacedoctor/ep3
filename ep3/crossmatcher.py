#!/usr/bin/env python
# encoding: utf-8
"""
*Crossmatch the FOV of the FITS frames against coordinates in the marshall transient tables*

The code will add a transient ID to each of the frames matched, update the object keyword with the best matching transient name and finally rename the fits files reveal the transient name in the filenames

:Author:
    David Young

:Date Created:
    January 29, 2021
"""
from builtins import object
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from fundamentals.mysql import readquery
import collections
from fundamentals.mysql import insert_list_of_dictionaries_into_database_tables
from HMpTy.mysql import conesearch


class crossmatcher(object):
    """
    *The worker class for the crossmatcher module*

    **Key Arguments:**
        - `log` -- logger
        - `dbConn` -- the connection to the phase III database
        - `settings` -- the settings dictionary

    **Usage:**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_). 

    To initiate a crossmatcher object, use the following:

    ```eval_rst
    .. todo::

        - add usage info
        - create a sublime snippet for usage
        - create cl-util for this class
        - add a tutorial about ``crossmatcher`` to documentation
        - create a blog post about what ``crossmatcher`` does
    ```

    ```python
    usage code 
    ```

    """

    def __init__(
            self,
            log,
            dbConn,
            settings=False,

    ):
        self.log = log
        log.debug("instansiating a new 'crossmatcher' object")
        self.settings = settings
        self.dbConn = dbConn

        return None

    def match(self):
        """
        *match the frames agains the list of transients in the marshall*

        **Return:**
            - ``crossmatcher``

        **Usage:**

        ```eval_rst
        .. todo::

            - add usage info
            - create a sublime snippet for usage
            - create cl-util for this method
            - update the package tutorial if needed
        ```

        ```python
        usage code 
        ```
        """
        self.log.debug('starting the ``match`` method')

        # FIRST ROUND OF MATCHING
        fitsPrimaryIds, fitsRas, fitsDecs, fitsObjects = self.list_frames(
            tableName="efosc_spectra",
            frameTypes=["acquisition_image", "weight", "science"]
        )
        matchIndies, matches, akas = self.crossmatch_transientBucketSummaries(
            fitsRas=fitsRas,
            fitsDecs=fitsDecs
        )

        # FIRST DEAL WITH FRAMES WHERE FRAME COORDINATES AND OBJECT NAME
        # MATCHES CORRDINATES & NAME IN MARSHALL
        solidMatches = []
        counts = collections.Counter(matchIndies.flatten())
        for i, m in zip(matchIndies, matches.list):
            if fitsObjects[i].lower() in akas[m["transientBucketId"]] or fitsObjects[i].lower().replace("at2", "sn2") in akas[m["transientBucketId"]]:
                solidMatches.append({"primaryId": fitsPrimaryIds[
                                    i], "transientBucketId": m["transientBucketId"]})
                m["solidMatch"] = 1
            else:
                m["solidMatch"] = 0
            m["primaryId"] = fitsPrimaryIds[i]
            m["fitsObject"] = fitsObjects[i]
            m["matchCount"] = counts[i]

        # INSERT ALL MATCHES BACK INTO THE DATABASE
        insert_list_of_dictionaries_into_database_tables(
            dbConn=self.dbConn,
            log=self.log,
            dictList=solidMatches,
            dbTableName="efosc_spectra",
            uniqueKeyList=["primaryId"],
            dateModified=True,
            dateCreated=True,
            batchSize=2500,
            replace=True,
            dbSettings=self.settings["database settings"]
        )

        self.log.debug('completed the ``match`` method')
        return crossmatcher

    def list_frames(
            self,
            tableName,
            frameTypes=False,
            locked=False):
        """*list the frames of a given instrument and observation type*

        **Key Arguments:**
            - `tableName` -- the database table to query
            - `frameTypes` -- list of frame types to filter by
            - `locked` -- return alos the locked rows? Default *False*

        **Return:**
            - `fitsPrimaryIds` -- the primaryIDs of the frames
            - `fitsRas` -- the RAs of the frames 
            - `fitsDecs` -- the DECs of the frames 
            - `fitsObjects` -- the object names in the frames 

        **Usage:**

        ```python
        from ep3 import crossmatcher
        matcher = crossmatcher(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        fitsPrimaryIds, fitsRas, fitsDecs, fitsObjects = matcher.list_frames(
            tableName="efosc_spectra",
            frameTypes=["acquisition_image", "weight", "science"]
        )
        ```
        """
        self.log.debug('starting the ``list_frames`` method')

        if not locked:
            locked = " and lock_row = 0 "
        else:
            locked = ""

        keys = []
        if frameTypes and len(frameTypes) > 0:
            frameTypes = ('","').join(frameTypes)
            sqlQuery = f"""
                SELECT `key` FROM pessto_phase_iii.filetype_key_calibration where `value` in ("{frameTypes}");
            """
            rows = readquery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.dbConn
            )
            keys = []
            keys[:] = [str(row["key"]) for row in rows]
            keys = (",").join(keys)
            sqlQuery = f"""
                select primaryId, ra, decl, object from {tableName} where transientBucketId is null or transientBucketId = 0 and filetype_key_calibration in ({keys}) {locked};
            """
        else:
            sqlQuery = f"""
                select primaryId, ra, decl, object from {tableName} where transientBucketId is null or transientBucketId = 0 {locked};
            """
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        if len(rows):
            fitsPrimaryIds, fitsRas, fitsDecs, fitsObjects = zip(
                *[(row["primaryId"], row["ra"], row["decl"], row["object"]) for row in rows])
        else:
            fitsPrimaryIds, fitsRas, fitsDecs, fitsObjects = [], [], [], []

        self.log.debug('completed the ``list_frames`` method')
        return fitsPrimaryIds, fitsRas, fitsDecs, fitsObjects

    def crossmatch_transientBucketSummaries(
            self,
            fitsRas,
            fitsDecs):
        """*crossmatch frame coordinates against the transient coordinates and return the results including AKAs of the matched transient sources*

        **Key Arguments:**
            - ``fitsRas`` -- list of the frame RAs
            - ``fitsDecs`` -- list of the frame DECs

        **Return:**
            - `matchIndies` -- the indexes of the orginal RA and DEC lists 
            - `matches` -- the transient matches as a list of dictionaries (same length as `matchIndies`)
            - `akas` -- dictionary of matched transient AKAs. Keys are transientBucketIds and values are a list of all transient names.

        **Usage:**

        ```python
        from ep3 import crossmatcher
        matcher = crossmatcher(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        fitsPrimaryIds, fitsRas, fitsDecs, fitsObjects = matcher.list_frames(
            tableName="efosc_spectra",
            frameTypes=["acquisition_image", "weight", "science"]
        )
        matchIndies, matches, akas = self.crossmatch_transientBucketSummaries(
            fitsRas=fitsRas,
            fitsDecs=fitsDecs
        )
        ```
        """
        self.log.debug(
            'starting the ``crossmatch_transientBucketSummaries`` method')

        # FIRST CROSSMATCH THE TRANSIENTS WITH A SUITABLE RADIUS
        cs = conesearch(
            log=self.log,
            dbConn=self.dbConn,
            tableName="transientBucketSummaries",
            columns="transientBucketId, masterName",
            ra=fitsRas,
            dec=fitsDecs,
            radiusArcsec=600,
            separations=True,
            distinct=False,
            sqlWhere=False,
            closest=False
        )
        matchIndies, matches = cs.search()

        # COMPILE A LIST OF ALL TRANSIENT IDS
        allTransientIds = []
        allTransientIds[:] = [m["transientBucketId"] for m in matches.list]
        allTransientIds = (",").join([str(t)
                                      for t in list(set(allTransientIds))])
        # GRAB ALL AKAs FOR THE TRANSIENTS
        sqlQuery = f"""
           select transientBucketId, name from marshall_transient_akas where transientBucketId in ({allTransientIds})
        """
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        # CREATE A DICTIONARY OF AKAs. KEYS ARE TRANSIENTBUCKETIDS AND VALUES
        # ARE A LIST OF ALL TRANSIENT NAMES.
        akas = {}
        for r in rows:
            if r["transientBucketId"] in akas:
                akas[r["transientBucketId"]].append(r["name"].lower())
            else:
                akas[r["transientBucketId"]] = [r["name"].lower()]

        self.log.debug(
            'completed the ``crossmatch_transientBucketSummaries`` method')
        return matchIndies, matches, akas

    # use the tab-trigger below for new method
    # xt-class-method
