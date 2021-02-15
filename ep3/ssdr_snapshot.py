#!/usr/bin/env python
# encoding: utf-8
"""
*Tools to help generate a snapshot of a previous data release hosted by ESO SAF*

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
import codecs
import unicodecsv as csv
from fundamentals.mysql import writequery
from fundamentals.mysql import insert_list_of_dictionaries_into_database_tables
import shutil

# OR YOU CAN REMOVE THE CLASS BELOW AND ADD A WORKER FUNCTION ... SNIPPET TRIGGER BELOW
# xt-worker-def


class ssdr_snapshot(object):
    """
    *The worker class for the ssdr_snapshot module*

    **Key Arguments:**
        - ``log`` -- logger
        - ``dbConn`` -- the database connection.
        - ``settings`` -- the settings dictionary

    **Usage:**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_). 

    To initiate a ssdr_snapshot object, use the following:

    ```eval_rst
    .. todo::

        - add usage info
        - create a sublime snippet for usage
        - create cl-util for this class
        - add a tutorial about ``ssdr_snapshot`` to documentation
        - create a blog post about what ``ssdr_snapshot`` does
    ```

    ```python
    usage code 
    ```

    """

    def __init__(
            self,
            log,
            dbConn=False,
            settings=False,

    ):
        self.log = log
        log.debug("instansiating a new 'ssdr_snapshot' object")
        self.settings = settings
        self.dbConn = dbConn

        # xt-self-arg-tmpx

        # 2. @flagged: what are the default attrributes each object could have? Add them to variable attribute set here
        # Variable Data Atrributes

        # 3. @flagged: what variable attrributes need overriden in any baseclass(es) used
        # Override Variable Data Atrributes

        # Initial Actions

        return None

    def add_database_table(self,
                           ssdr_ver,
                           pathToCsvListing):
        """
        *get the ssdr_snapshot object*

        **Key Arguments:**
        - ``ssdr_ver`` -- version of the data release listing downloaded from the eso saf
        - ``pathToCsvListing`` -- path to the CSV file listing downloaded from the eso saf

        To get the CSV listing:

        * Go to the [ESO Data Release listing](https://www.eso.org/rm/publicAccess#/dataReleases?wcmmode=disabled) and navigate to PESSTO.
        * Select to go to the 'Archive Query Form' from the latest release:

            [![](https://live.staticflickr.com/65535/50936216258_f3dd9e22ee_m.png)](https://live.staticflickr.com/65535/50936216258_f3dd9e22ee_o.png)

        * Select 'current product version'

            [![](https://live.staticflickr.com/65535/50936992846_ca1709f689_z.png)](https://live.staticflickr.com/65535/50936992846_ca1709f689_o.png)

        * Ask for all of the files in an CSV output and hit the search button (the CSV file will now download)

        **Return:**
            - ``ssdr_snapshot``

        **Usage:**

        ```eval_rst
        .. todo::

            - create cl-util for this method
            - update the package tutorial if needed
        ```

        ```python
        from ep3 import ssdr_snapshot
        ssdr = ssdr_snapshot(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        ssdr.add_database_table(
            ssdr_ver=3.1,
            pathToCsvListing=pathToInputDir + "/wdb_query_7563_eso.csv")
        ```
        """
        self.log.debug('starting the ``add_database_table`` method')

        with codecs.open(pathToCsvListing, encoding='utf-8', mode='r') as readFile:
            thisData = readFile.read()

        thisData = thisData.replace(
            " (&lambda;/&delta;&lambda;)", "").replace(
            "\ufeff", "").strip().split("\n\n")[0]

        # FIX HEADRERS
        headerline = thisData.split("\n")[0]
        headerline_fixed = headerline.replace(
            " ", "_").replace("-", "_").lower().replace("run/", "").replace(",r,", ",resolution,").replace("dec,", "decDeg,").replace("ra,", "raDeg,")
        thisData = thisData.replace(headerline, headerline_fixed)

        # WRITE FILE TO TMP LOCATION
        pathToCsvListing = '/tmp/ssdr.csv'
        myFile = open(pathToCsvListing, "w")
        myFile.write(thisData)
        myFile.close()

        with open(pathToCsvListing, 'rb') as csvFile:
            csvReader = csv.DictReader(
                csvFile, dialect='excel', delimiter=',', quotechar='"')
            dictList = []
            dictList[:] = [d for d in csvReader if d[
                'product_category'] != "SCIENCE.CATALOG"]

        for d in dictList:
            d["data_rel"] = ssdr_ver

        # DROP AND CREATE FRESH TABLE
        createStatement = """
        DROP TABLE IF EXISTS ssdr_file_listing;
        CREATE TABLE `ssdr_file_listing` (
          `primaryId` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'An internal counter',
          `dateCreated` datetime DEFAULT CURRENT_TIMESTAMP,
          `dateLastModified` datetime DEFAULT CURRENT_TIMESTAMP,
          `updated` tinyint(4) DEFAULT '0',
          `abmaglim` double DEFAULT NULL,
          `arcfile` varchar(100) NOT NULL,
          `collection` varchar(100) DEFAULT NULL,
          `date_obs` datetime DEFAULT NULL,
          `decDeg` double DEFAULT NULL,
          `exptime` double DEFAULT NULL,
          `filter` varchar(100) DEFAULT NULL,
          `instrument` varchar(100) DEFAULT NULL,
          `object` varchar(100) DEFAULT NULL,
          `origfile` varchar(100) DEFAULT NULL,
          `product_category` varchar(100) DEFAULT NULL,
          `product_version` tinyint(1) DEFAULT NULL,
          `program_id` varchar(100) DEFAULT NULL,
          `raDeg` double DEFAULT NULL,
          `referenc` varchar(100) DEFAULT NULL,
          `resolution` double DEFAULT NULL,
          `snr` double DEFAULT NULL,
          `wavelength` varchar(100) DEFAULT NULL,
          `data_rel` double DEFAULT NULL,
          `tablePrimaryId` int(11) DEFAULT NULL,
          PRIMARY KEY (`primaryId`),
          UNIQUE KEY `arcfile` (`arcfile`),
          KEY `idx_date_obs` (`date_obs`),
          KEY `idx_origname` (`origfile`),
          KEY `idx_tablePriamryId` (`tablePrimaryId`)
        ) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4;
        """
        writequery(
            log=self.log,
            sqlQuery=createStatement,
            dbConn=self.dbConn,
        )

        # USE dbSettings TO ACTIVATE MULTIPROCESSING - INSERT LIST OF
        # DICTIONARIES INTO DATABASE
        insert_list_of_dictionaries_into_database_tables(
            dbConn=self.dbConn,
            log=self.log,
            dictList=dictList,
            dbTableName="ssdr_file_listing",
            uniqueKeyList=["arcfile"],
            dateModified=True,
            dateCreated=True,
            batchSize=2500,
            replace=True,
            dbSettings=self.settings["database settings"]
        )

        # TRASH TMP FILE
        os.remove(pathToCsvListing)

        # SYNC WITH EXIST TABLES - MATCH FILENAMES AND DATEOBS TO POPULATE
        # ESOFILENAME COLUMNS
        sqlQuery = f"""call ep3_match_esoFilenames()"""
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        self.log.debug('completed the ``add_database_table`` method')
        return ssdr_snapshot

    def ssdr_file_reset(
            self,
            pathToDownloadDir):
        """*ssdr file reset*

        **Key Arguments:**
            - ``pathToDownloadDir`` -- path the to directory of FITS downloads.

        **Return:**
            - `count` -- the number of files renamed

        **Usage:**

        ```python
        from ep3 import ssdr_snapshot
        ssdr = ssdr_snapshot(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        count = ssdr.ssdr_file_reset(
            pathToDownloadDir="/my/eso_downlaods/")
        print(f"Successfully renamed {count} files.")
        ```

        ---

        ```eval_rst
        .. todo::

            - update package tutorial with command-line tool info if needed
        ```
        """
        self.log.debug('starting the ``ssdr_file_reset`` method')

        # GENERATE A LIST OF FITS FILE PATHS
        fitsPaths = []
        for d in os.listdir(pathToDownloadDir):
            filepath = os.path.join(pathToDownloadDir, d)
            if os.path.isfile(filepath) and os.path.splitext(filepath)[1] == ".fits":
                fitsPaths.append(filepath)

        print(fitsPaths)

        # GENERATE THE IMAGECOLLECTION
        from ccdproc import ImageFileCollection
        keys = ['ORIGFILE']
        collection = ImageFileCollection(
            location=pathToDownloadDir, filenames=fitsPaths, keywords=keys)
        collection.summary.pprint_all()

        # CONVERT IMAGECOLLECTION TO A LIST OF DICTIONARIES
        dictList = []
        dictList[:] = [l for l in collection.summary]

        count = 0
        for d in dictList:
            if d["file"] != d["ORIGFILE"]:
                shutil.move(pathToDownloadDir + "/" +
                            d["file"], pathToDownloadDir + "/" + d["ORIGFILE"])
                count += 1

        self.log.debug('completed the ``ssdr_file_reset`` method')
        return count

    # use the tab-trigger below for new method
    # xt-class-method

    # 5. @flagged: what actions of the base class(es) need ammending? ammend them here
    # Override Method Attributes
    # method-override-tmpx
