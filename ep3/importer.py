#!/usr/bin/env python
# encoding: utf-8
"""
*Import the EFOSC and SOFI Fits Frames into the Database and Nested Folder Archive*

:Author:
    David Young

:Date Created:
    December 14, 2020
"""
from builtins import object
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from astropy.io import fits
from fundamentals.mysql import insert_list_of_dictionaries_into_database_tables
from fundamentals.mysql import writequery
from fundamentals.mysql import readquery


class importer(object):
    """
    *The worker class for the importer module*

    **Key Arguments:**
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary

    **Usage:**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_).

    To initiate a importer object, use the following:

    ```eval_rst
    .. todo::

        - create cl-util for this class
        - add a tutorial about ``importer`` to documentation
    ```

    ```python
    from ep3 import importer
    ingester = importer(
        log=log,
        settings=settings
    )
    ingester.ingest()
    ```

    """

    def __init__(
            self,
            log,
            settings=False
    ):
        self.log = log
        log.debug("instansiating a new 'importer' object")
        self.settings = settings
        # xt-self-arg-tmpx

        # SETUP ALL DATABASE CONNECTIONS
        from fundamentals.mysql import database
        self.dbConn = database(
            log=self.log,
            dbSettings=settings["database settings"]
        ).connect()

        self.tables = ['efosc_imaging', 'sofi_imaging',
                       'efosc_spectra', 'sofi_spectra']

        return None

    def ingest(self):
        """
        *ingest fits headers into the database and files in the phase III archive*

        **Usage:**

        ```eval_rst
        .. todo::

            - create cl-util for this method
        ```

        ```python
        from ep3 import importer
        ingester = importer(
            log=log,
            settings=settings
        )
        ingester.ingest()
        ```
        """
        self.log.debug('starting the ``ingest`` method')

        efoscSpectra, sofiSpectra, efoscImages, sofiImages = self.filter_directory_of_fits_frame(
            batchSize=2000
        )

        for l, t in zip([efoscSpectra, sofiSpectra, efoscImages, sofiImages], ['efosc_spectra', 'sofi_spectra', 'efosc_imaging', 'sofi_imaging']):
            if not l:
                continue
            listOfDictionaries = []
            listOfDictionaries[:] = [self.header_to_dict(h, f)
                                     for h, f in zip(l.headers(), l.files_filtered(include_path=True))]

            # USE dbSettings TO ACTIVATE MULTIPROCESSING - INSERT LIST OF
            # DICTIONARIES INTO DATABASE
            insert_list_of_dictionaries_into_database_tables(
                dbConn=self.dbConn,
                log=self.log,
                dictList=listOfDictionaries,
                dbTableName=t,
                uniqueKeyList=["filename"],
                dateModified=True,
                dateCreated=True,
                batchSize=2500,
                replace=True,
                dbSettings=self.settings["database settings"]
            )

        # BOOKKEEPING QUERIES
        sqlQueries = [
            "call ep3_set_file_type();",
            "call ep3_set_archive_path();"
        ]
        for sqlQuery in sqlQueries:
            writequery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.dbConn
            )

        self.log.debug('completed the ``ingest`` method')
        return None

    def filter_directory_of_fits_frame(
            self,
            batchSize=False):
        """*read in the fits files from a directory and filter them into their types, returning a dictionary of types and lists of fits files matching those types*

        **Key Arguments:**
            - ``batchSize`` -- limit the number of fits files to filter. Default **False**

        **Return:**
            - ` efoscSpectra`, `sofiSpectra`, `efoscImages`, `sofiImages` -- filter imageFileCollections

        **Usage:**

        ```python
        efoscSpectra, sofiSpectra, efoscImages, sofiImages = ingester.filter_directory_of_fits_frame(
            batchSize=50
        )
        ```
        """
        self.log.debug(
            'starting the ``filter_directory_of_fits_frame`` method')

        # GENERATE A LIST OF FITS FILE PATHS
        fitsPaths = []
        pathToDirectory = self.settings["dropbox"]
        for d in os.listdir(pathToDirectory):
            filepath = os.path.join(pathToDirectory, d)
            if os.path.isfile(filepath) and os.path.splitext(filepath)[1] == ".fits":
                fitsPaths.append(filepath)

        # LIMIT TO BATCHSIZE
        if batchSize:
            fitsPaths = fitsPaths[:batchSize]

        # GENERATE THE IMAGECOLLECTION
        from ccdproc import ImageFileCollection
        keys = ['INSTRUME', 'ESO DPR CATG', 'ESO DPR TYPE', 'ESO DPR TECH', 'ESO TPL NAME',
                'OBSTECH', 'ESO INS GRIS1 NAME', 'BITPIX']
        collection = ImageFileCollection(
            location=pathToDirectory, filenames=fitsPaths, keywords=keys)

        # EFOSC SPECTRA
        matches = (collection.summary['INSTRUME'] == "EFOSC") & (
            (collection.summary['ESO DPR TECH'] == 'SPECTRUM') |
            (collection.summary['OBSTECH'] == 'SPECTRUM')) & (collection.summary['BITPIX'] != 8)
        efoscSpectra = collection.summary['file'][matches]
        files = []
        files[:] = [f for f in efoscSpectra]
        if len(files):
            efoscSpectra = ImageFileCollection(
                location=pathToDirectory, filenames=files, keywords=keys)
        else:
            efoscSpectra = None

        # SOFI SPECTRA
        matches = (collection.summary['INSTRUME'] == "SOFI") & (
            (collection.summary['ESO DPR TECH'] == 'SPECTRUM') |
            (collection.summary['OBSTECH'] == 'SPECTRUM')) & (collection.summary['BITPIX'] != 8)
        sofiSpectra = collection.summary['file'][matches]
        files = []
        files[:] = [f for f in sofiSpectra]
        if len(files):
            sofiSpectra = ImageFileCollection(
                location=pathToDirectory, filenames=files, keywords=keys)
        else:
            sofiSpectra = None

        # EFOSC IMAGES
        matches = (collection.summary['INSTRUME'] == "EFOSC") & (
            (collection.summary['ESO DPR TECH'] == 'IMAGE') |
            (collection.summary['OBSTECH'] == 'IMAGE'))
        efoscImages = collection.summary['file'][matches]
        files = []
        files[:] = [f for f in efoscImages]
        if len(files):
            efoscImages = ImageFileCollection(
                location=pathToDirectory, filenames=files, keywords=keys)
        else:
            efoscImages = None

        # SOFI IMAGES
        matches = (collection.summary['INSTRUME'] == "SOFI") & (
            (collection.summary['ESO DPR TECH'] == 'IMAGE') |
            (collection.summary['OBSTECH'] == 'IMAGE'))
        sofiImages = collection.summary['file'][matches]
        files = []
        files[:] = [f for f in sofiImages]
        if len(files):
            sofiImages = ImageFileCollection(
                location=pathToDirectory, filenames=files, keywords=keys)
        else:
            sofiImages = None

        self.log.debug(
            'completed the ``filter_directory_of_fits_frame`` method')
        return efoscSpectra, sofiSpectra, efoscImages, sofiImages

    def header_to_dict(
            self,
            header,
            fitsPath):
        """*convert the header of the fits frame to a dictionary*

        **Key Arguments:**
            - `header` -- the fits header
            - `fitsPath` -- path to the fits frame

        **Return:**
            - ``fitsDict`` -- the fits header as a dictionary

        **Usage:**

        ```python
        fitsDict = importer.header_to_dict(header, fitsPath)
        ```
        """
        self.log.debug('starting the ``header_to_dict`` method')

        fitsDict = {
            'filePath': fitsPath,
            'filename': os.path.basename(fitsPath),
            'headerExtension': 0
        }
        for k, v in header.items():
            if not len(k):
                continue
            if type(v) == bool:
                if v is True:
                    v = "T"
                else:
                    v = "F"
            if k == "RELEASE":
                k = 'RRELEASE'
            if k == "DEC":
                k = "DECL"
            if k == "________________OG530_/ADA_GUID_DEC".replace("_", " "):
                k = "ESO_ADA_GUID_DEC"
            k = k.strip().replace(" ", "_").replace("-", "_")
            fitsDict[k] = v

        self.log.debug('completed the ``header_to_dict`` method')
        return fitsDict

    def select_files_to_archive(
            self,
            tableName):
        """*select files to archive from the requested table*

        **Key Arguments:**
            - `tableName` -- the name of the database table to lift the data from

        **Return:**
            - `primaryIds` -- list of the primaryIds of the files from the database
            - `filePaths` -- list of the current filepaths (same length as `primaryIds`)
            - `archivePaths` -- list of the destinations directories to send files too (same length as `primaryIds`)

        **Usage:**

        ```python
        from ep3 import importer
        ingester = importer(
            log=log,
            settings=settings
        )
        primaryIds, filePaths, archivePaths = ingester.select_files_to_archive('efosc_spectra')
        ```
        """
        self.log.debug('starting the ``select_files_to_archive`` method')

        sqlQuery = f"""
            select primaryId, filePath, archivePath from {tableName} where filePath is not null; 
        """
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        primaryIds = []
        primaryIds[:] = [r["primaryId"] for r in rows]
        filePaths = []
        filePaths[:] = [r["filePath"] for r in rows]
        archivePaths = []
        archivePaths[:] = [r["archivePath"] for r in rows]

        self.log.debug('completed the ``select_files_to_archive`` method')
        return primaryIds, filePaths, archivePaths

    # use the tab-trigger below for new method
    # xt-class-method
