#!/usr/bin/env python
# encoding: utf-8
"""
*Clean up the database entries, fixing any issues needing resolved for data releases*

:Author:
    David Young

:Date Created:
    February 12, 2021
"""
from builtins import object
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from fundamentals.mysql import writequery, readquery
from fundamentals.mysql import insert_list_of_dictionaries_into_database_tables
# OR YOU CAN REMOVE THE CLASS BELOW AND ADD A WORKER FUNCTION ... SNIPPET TRIGGER BELOW
# xt-worker-def


class clean(object):
    """
    *Clean up the database entries, fixing any issues needing resolved for data releases*

    **Key Arguments:**
        - ``log`` -- logger
        - ``dbConn`` -- the database connection
        - ``settings`` -- the settings dictionary

    **Usage:**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_).

    To initiate a clean object, use the following:

    ```eval_rst
    .. todo::

        - add a tutorial about ``clean`` to documentation
        - create a blog post about what ``clean`` does
    ```

    ```python
    from ep3 import clean
    cleaner = clean(
        log=log,
        dbConn=dbConn,
        settings=settings
    )
    cleaner.clean()
    ```

    """
    # Initialisation
    # 1. @flagged: what are the unique attrributes for each object? Add them
    # to __init__

    def __init__(
            self,
            log,
            dbConn,
            settings=False,

    ):
        self.log = log
        log.debug("instansiating a new 'clean' object")
        self.settings = settings
        self.dbConn = dbConn
        # xt-self-arg-tmpx

        # 2. @flagged: what are the default attrributes each object could have? Add them to variable attribute set here
        # Variable Data Atrributes

        # 3. @flagged: what variable attrributes need overriden in any baseclass(es) used
        # Override Variable Data Atrributes

        # Initial Actions

        return None

    def clean(self):
        """
        *run the database cleaning tasks*

        **Return:**
            - ``clean``

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
        self.log.debug('starting the ``clean`` method')

        procedures = [
            "ep3_clean_transientBucketSummaries()",
            "ep3_basic_keyword_value_corrections()",
            "ep3_force_match_object_to_frame()",
            "ep3_set_file_associations()",
            "ep3_flag_frames_for_release()",
            "ep3_set_data_rel_versions()",
            "ep3_set_zeropoint_in_efosc_images()",
            "ep3_set_maglim_magat_in_images()",
            "ep3_create_spectrum_binary_table_rows()",
            "ep3_set_exportFilenames()"]

        for p in procedures:
            sqlQuery = f"""CALL {p};"""
            print(f"Running the `{p}` procedure")
            writequery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.dbConn
            )

        self.fix_image_astrometry_keywords()
        self.fix_sofi_mjd_keywords()

        self.log.debug('completed the ``clean`` method')
        return clean

    def fix_image_astrometry_keywords(
            self):
        """*fix image astrometry keywords*

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Usage:**

        ```python
        usage code
        ```

        ---

        ```eval_rst
        .. todo::

            - add usage info
            - create a sublime snippet for usage
            - write a command-line tool for this method
            - update package tutorial with command-line tool info if needed
        ```
        """
        self.log.debug('starting the ``fix_image_astrometry_keywords`` method')

        tables = ["efosc_imaging", "sofi_imaging"]

        for table in tables:

            sqlQuery = f"""
                select primaryId, ASTROMET from {table} where ASTROMET is not null and astromet_rmsx is null;
            """
            rows = readquery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.dbConn
            )

            dictList = []

            for row in rows:
                splitList = row["ASTROMET"].split()

                rmsx = float(splitList[0])
                rmsy = float(splitList[1])
                sources = int(splitList[2])
                astromet_error = (rmsx ** 2 + rmsy ** 2) ** 0.5
                crder1 = rmsx / 3600.
                crder2 = rmsy / 3600.

                if rmsx > 100:
                    rmsx = 999
                    crder1 = 9999
                if rmsy > 100:
                    rmsy = 999
                    crder2 = 9999

                thisDict = {
                    "primaryId": row["primaryId"],
                    "astromet_rmsx": rmsx,
                    "astromet_rmsy": rmsy,
                    "astromet_sources": sources,
                    "astromet_error_arcsec": astromet_error,
                    "crder1": crder1,
                    "crder2": crder2
                }
                dictList.append(thisDict)

            # USE dbSettings TO ACTIVATE MULTIPROCESSING - INSERT LIST OF
            # DICTIONARIES INTO DATABASE
            insert_list_of_dictionaries_into_database_tables(
                dbConn=self.dbConn,
                log=self.log,
                dictList=dictList,
                dbTableName=table,
                uniqueKeyList=['primaryId'],
                dateModified=True,
                dateCreated=True,
                batchSize=2500,
                replace=True,
                dbSettings=self.settings["database settings"]
            )

        self.log.debug(
            'completed the ``fix_image_astrometry_keywords`` method')
        return None

    def fix_sofi_mjd_keywords(
            self):
        """*fix sofi mjd keywords*

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Usage:**

        ```python
        usage code
        ```

        ---

        ```eval_rst
        .. todo::

            - add usage info
            - create a sublime snippet for usage
            - write a command-line tool for this method
            - update package tutorial with command-line tool info if needed
        ```
        """
        self.log.debug('starting the ``fix_sofi_mjd_keywords`` method')

        tables = ["sofi_imaging", "sofi_spectra"]
        provLimit = [60, 16]

        for table, lim in zip(tables, provLimit):

            provs = []
            provs[:] = [f"prov{p}" for p in range(1, lim + 1)]
            provString = (',').join(provs)

            sqlQuery = f"""
                select tmid, mjd_obs,ndit, primaryId ,filename, ncombine, {provString} from {table} where filetype_key_reduction_stage  =3 and filetype_key_calibration in (11,13) and lock_row = 0
            """

            rows = readquery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.dbConn
            )
            provFiles = []
            provFiles[:] = [[row[p] for p in provs if row[p]] for row in rows]

            # LIST OF NCOMBINES
            ncomb = [len(n) for n in provFiles]

            # MAKE FLAT LIST OF UNIQUE PROV FILES
            provFilesFlat = list(
                set([sorted(sublist)[-1] for sublist in provFiles]))
            provFilesFlat = ('","').join(provFilesFlat)

            # NOW GRAB THE STATS
            sqlQuery = f"""
                select mjd_obs, exptime, filename from {table}  where filename in ("{provFilesFlat}") order by mjd_obs asc
            """
            provRows = readquery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.dbConn
            )

            # CONVERT TO EASY LOOKUP DICT
            provRows = {p["filename"]: p for p in provRows}

            dictList = []
            for r, p, n in zip(rows, provFiles, ncomb):
                lastFile = sorted(p)[-1]
                mjd_end = provRows[lastFile]["mjd_obs"] + r["ndit"] * \
                    (provRows[lastFile]["exptime"] + 1.8) / (60. * 60. * 24.)
                telapse = (mjd_end - r["mjd_obs"]) * (60. * 60. * 24.)
                tmid = (mjd_end + r["mjd_obs"]) / 2.
                if "spectra" in table:
                    dictList.append({"primaryId": r[
                                    "primaryId"], "ncombine": n, "mjd_end": mjd_end, "telapse": telapse, "tmid": tmid})
                else:
                    dictList.append(
                        {"primaryId": r["primaryId"], "mjd_end": mjd_end, "tmid": tmid})

            # USE dbSettings TO ACTIVATE MULTIPROCESSING - INSERT LIST OF
            # DICTIONARIES INTO DATABASE
            insert_list_of_dictionaries_into_database_tables(
                dbConn=self.dbConn,
                log=self.log,
                dictList=dictList,
                dbTableName=table,
                uniqueKeyList=["primaryId"],
                dateModified=True,
                dateCreated=True,
                batchSize=2500,
                replace=True,
                dbSettings=self.settings["database settings"]
            )

        self.log.debug('completed the ``fix_sofi_mjd_keywords`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method

    # 5. @flagged: what actions of the base class(es) need ammending? ammend them here
    # Override Method Attributes
    # method-override-tmpx
