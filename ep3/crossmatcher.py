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
from operator import itemgetter
from fundamentals.renderer import list_of_dictionaries


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

        - add a tutorial about ``crossmatcher`` to documentation
    ```

    To only run the automatic matching to update frames whose coordinates are close to a transient AND whose object name matches an AKA of that same transient:

    ```python
    from ep3 import crossmatcher
    matcher = crossmatcher(
        log=log,
        dbConn=dbConn,
        settings=settings
    )
    matcher.match(
        transientId=False,
        fitsObject=False
    )
    ```

    Then to force a match of a frame which doesn't pass automatic checking but you have manually checked:

    ```python
    from ep3 import crossmatcher
    matcher = crossmatcher(
        log=log,
        dbConn=dbConn,
        settings=settings
    )
    matcher.match(
        transientId=12812982,
        fitsObject="SNBang"
    )
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

    def match(
            self,
            transientId=False,
            fitsObject=False):
        """
        *match the frames against the list of transients in the marshall*

        **Key Arguments:**
            - `transientId` -- transient ID to match with frames with fitsObject. Default *False* (perform automated matches)
            - `fitsObject` -- fitsObject to match to transientID. Default *False* (perform automated matches)

        **Usage:**

        To only run the automatic matching to update frames whose coordinates are close to a transient AND whose object name matches an AKA of that same transient:

        ```python
        from ep3 import crossmatcher
        matcher = crossmatcher(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        matcher.match(
            transientId=False,
            fitsObject=False
        )
        ```

        Then to force a match of a frame which doesn't pass automatic checking but you have manually checked:

        ```python
        from ep3 import crossmatcher
        matcher = crossmatcher(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        matcher.match(
            transientId=12812982,
            fitsObject="SNBang"
        )
        ```
        """
        self.log.debug('starting the ``match`` method')

        dbTables = ["efosc_spectra", "efosc_imaging",
                    "sofi_imaging", "sofi_spectra"]
        for dbTable in dbTables:

            # FIRST ROUND OF MATCHING
            fitsPrimaryIds, fitsRas, fitsDecs, fitsObjects, fitsReduced = self.list_frames(
                tableName=dbTable,
                frameTypes=["acquisition_image", "weight", "science"]
            )
            if not len(fitsRas):
                print(f"* All frames matched in the `{dbTable}` table, have a pat on the back!\n")
                continue
            matchIndies, matches, akas = self.crossmatch_transientBucketSummaries(
                fitsRas=fitsRas,
                fitsDecs=fitsDecs
            )

            # MAKE LOWERCASE AKAS
            akasLowered = {}
            if len(akas):
                for k, v in akas.items():
                    akasLowered[k] = [one.lower().replace("-", "")
                                      for one in v]

            # FIRST DEAL WITH FRAMES WHERE FRAME COORDINATES AND OBJECT NAME
            # MATCHES CORRDINATES & NAME IN MARSHALL
            solidMatches = []
            solidMatchObjects = []
            ropeyMatchObjects = []
            ropeyMatches = []
            ropeyMatchUniqueIndex = []
            counts = collections.Counter(matchIndies.flatten())
            for i, m in zip(matchIndies, matches.list):
                m["reduced"] = fitsReduced[i]
                m["primaryId"] = fitsPrimaryIds[i]
                m["fitsObject"] = fitsObjects[i]
                m["matchCount"] = counts[i]
                if transientId != False and fitsObject != False and fitsObjects[i].lower().strip().replace("-", "") == fitsObject.lower().strip().replace("-", "") and m["transientBucketId"] == int(transientId):
                    match = True
                elif fitsObjects[i].strip().lower().replace("-", "") in akas[m["transientBucketId"]] or fitsObjects[i].strip().lower().replace("at2", "sn2").replace("-", "") in akasLowered[m["transientBucketId"]]:
                    match = True
                else:
                    match = False
                    ui = str(m["transientBucketId"]) + m["fitsObject"]
                    if ui not in ropeyMatchUniqueIndex:
                        ropeyMatchUniqueIndex.append(ui)
                        m["solidMatch"] = 0
                        m["akas"] = (",").join(akas[m["transientBucketId"]])
                        ropeyMatches.append(m)
                        ropeyMatchObjects.append(m["fitsObject"])
                if match is True:
                    matchDict = {"primaryId": fitsPrimaryIds[
                        i], "transientBucketId": m["transientBucketId"]}
                    # ONLY UPDATE OBJECT/COORDINATES IN REDUCED FRAMES
                    if fitsReduced[i]:
                        matchDict["object"] = akas[m["transientBucketId"]][0]
                        # ONLY UPDATE COORDINATES IN SPECTRAL FRAMES
                        if "spec" in dbTable:
                            matchDict["ra"] = m["raDeg"]
                            matchDict["decl"] = m["decDeg"]
                    m["solidMatch"] = 1
                    solidMatchObjects.append(m["fitsObject"])
                    solidMatches.append(matchDict)

            # FILTER OUT MATCHED ROWS
            unmatched = []
            for m, o in zip(ropeyMatches, ropeyMatchObjects):
                if o not in solidMatchObjects:
                    unmatched.append(m)

            # INSERT ALL MATCHES BACK INTO THE DATABASE
            insert_list_of_dictionaries_into_database_tables(
                dbConn=self.dbConn,
                log=self.log,
                dictList=solidMatches,
                dbTableName=dbTable,
                uniqueKeyList=["primaryId"],
                dateModified=True,
                dateCreated=True,
                batchSize=2500,
                replace=True,
                dbSettings=self.settings["database settings"]
            )

            # SORT REMAINING MATCHES
            if not len(ropeyMatches):
                print(f"* All frames matched in the `{dbTable}` table, have a pat on the back!\n")
                continue
            unmatched = sorted(unmatched, key=itemgetter(
                'fitsObject'))

            dataSet = list_of_dictionaries(
                log=self.log,
                listOfDictionaries=unmatched
            )
            tableData = dataSet.table(filepath=None)
            print(f"Check the matches below for the `{dbTable}` table and comfirm matches with ... ")
            # UPDATE `unit_tests`.`efosc_spectra` SET
            # `filetype_key_calibration` = 5 WHERE (`primaryId` = '622');
            print(tableData)
            print()

        self.log.debug('completed the ``match`` method')
        return None

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
                select primaryId, ra, decl, object,  if(filetype_key_reduction_stage=3,1,0) as reduced from {tableName} where transientBucketId is null or transientBucketId = 0 and filetype_key_calibration in ({keys}) {locked};
            """
        else:
            sqlQuery = f"""
                select primaryId, ra, decl, object,  if(filetype_key_reduction_stage=3,1,0) as reduced from {tableName} where transientBucketId is null or transientBucketId = 0 {locked};
            """
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        print(sqlQuery)

        if len(rows):
            fitsPrimaryIds, fitsRas, fitsDecs, fitsObjects, fitsReduced = zip(
                *[(row["primaryId"], row["ra"], row["decl"], row["object"], row["reduced"]) for row in rows])
        else:
            fitsPrimaryIds, fitsRas, fitsDecs, fitsObjects, fitsReduced = [], [], [], [], []

        self.log.debug('completed the ``list_frames`` method')
        return fitsPrimaryIds, fitsRas, fitsDecs, fitsObjects, fitsReduced

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

        if not len(matchIndies):
            self.log.debug(
                'completed the ``crossmatch_transientBucketSummaries`` method')
            return matchIndies, matches, []

        # COMPILE A LIST OF ALL TRANSIENT IDS

        allTransientIds = []
        allTransientIds[:] = [m["transientBucketId"] for m in matches.list]
        allTransientIds = (",").join([str(t)
                                      for t in list(set(allTransientIds))])
        # GRAB ALL AKAs FOR THE TRANSIENTS
        sqlQuery = f"""
           select transientBucketId, name from marshall_transient_akas where transientBucketId in ({allTransientIds}) order by master desc
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
                akas[r["transientBucketId"]].append(r["name"])
            else:
                akas[r["transientBucketId"]] = [r["name"]]

        self.log.debug(
            'completed the ``crossmatch_transientBucketSummaries`` method')
        return matchIndies, matches, akas

    # use the tab-trigger below for new method
    # xt-class-method
