#!/usr/bin/env python
# encoding: utf-8
"""
*Populate and export the PESSTO_TRAN_CAT*

:Author:
    David Young

:Date Created:
    February 18, 2021
"""
from builtins import object
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from astropy.io import fits
from datetime import datetime, date, time
import numpy as np
import collections
from os.path import expanduser
from fundamentals.mysql import readquery


class transient_catalogue(object):
    """
    *Tools to create the ESO data release transient catalogue `PESSTO_TRAN_CAT.fits`*

    **Key Arguments:**
        - ``log`` -- logger
        - ``dbConn`` -- database connection
        - ``outputDirectory`` -- directory path for the output fits file
        - ``settings`` -- the settings dictionary

    **Usage:**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_).

    To initiate a transient_catalogue object, use the following:

    ```eval_rst
    .. todo::

        - create cl-util for this class
        - add a tutorial about ``transient_catalogue`` to documentation
        - create a blog post about what ``transient_catalogue`` does
    ```

    ```python
    from ep3 import transient_catalogue
    cat = transient_catalogue(
        log=log,
        dbConn=dbConn,
        outputDirectory="/path/to/output/",
        settings=settings
    )
    fitsPath = cat.create()
    ```

    """

    def __init__(
            self,
            log,
            dbConn,
            outputDirectory,
            settings=False,

    ):
        self.log = log
        log.debug("instansiating a new 'transient_catalogue' object")
        self.settings = settings
        self.dbConn = dbConn

        home = expanduser("~")
        outputDirectory = outputDirectory.replace("~", home)

        self.pathToOutput = outputDirectory + "/PESSTO_TRAN_CAT.fits"

        return None

    def create(self):
        """
        *create the transient catalogue binary FITS table*

        **Return:**
            - ``transient_catalogue``

        **Usage:**

        ```python
        from ep3 import transient_catalogue
        cat = transient_catalogue(
            log=log,
            dbConn=dbConn,
            outputDirectory="/path/to/output/",
            settings=settings
        )
        fitsPath = cat.create()
        ```
        """
        self.log.debug('starting the ``create`` method')

        primHdu = self.generate_primary_header()
        binTableHdu = self.generate_data_table_unit()

        # BUILD AND FIX HDU LIST
        hduList = fits.HDUList([primHdu, binTableHdu])
        hduList.verify("fix")

        # WRITE TO FILE
        hduList.writeto(self.pathToOutput, checksum=True, overwrite=True)

        self.log.debug('completed the ``create`` method')
        return self.pathToOutput

    def generate_data_table_unit(
            self):
        """*generate data table unit*

        **Return:**
            - ``binTableHdu`` -- the table HDU (pre-header population)
        """
        self.log.debug('starting the ``generate_data_table_unit`` method')

        # GET THE COLUMN FORMATS FROM DATABASE
        from fundamentals.mysql import readquery
        sqlQuery = f"""
            select * from pessto_tran_cat_column_desc
        """
        coldesc = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        # GET THE DATA
        sqlQuery = f"""
            select * from PESSTO_TRAN_CAT
        """
        data = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        # LET'S GENERATE THE COLUMNS
        allColumns = []
        for col in coldesc:
            # CONVERT NULL CHARACTER TO INT IF POSSIBLE
            try:
                col["TNULL"] = int(col["TNULL"])
            except:
                pass
            # DETERMINE NULL VALUE
            if col["TNULL"]:
                null = col["TNULL"]
            elif "A" in col["TFORM"]:
                null = '\0'
            elif "E" in col["TFORM"] or "D" in col["TFORM"]:
                null = "NaN"

            else:
                null = col["TNULL"]

            if not col["TUNIT"]:
                col["TUNIT"] = " "

            # GENERATE THE ARRAY WITH THE CORRECT NULLS
            array = []
            array[:] = [a[col["TTYPE"]] if a[col["TTYPE"]] else null
                        for a in data]
            thisColumn = fits.Column(
                name=col["TTYPE"], format=col["TFORM"], array=array, null=col["TNULL"], unit=col["TUNIT"])
            allColumns.append(thisColumn)
        binTableHdu = fits.BinTableHDU.from_columns(allColumns)
        binTableHdu.header.insert(key="XTENSION", card=(
            'EXTNAME', 'PHASE3CATALOG', 'fits extension name'), after=True)
        for col in coldesc:
            after = f'TFORM{col["columnNumber"]}'
            for k in ["TPRIC", "TINDX", "TUCD", "TCOMM"]:
                if col[k]:
                    if col[k] == "T":
                        col[k] = True
                    key = f'{k}{col["columnNumber"]}'
                    binTableHdu.header.insert(key=after, card=(key,
                                                               col[k]), after=True)

        self.log.debug('completed the ``generate_data_table_unit`` method')
        return binTableHdu

    def generate_primary_header(
            self):
        """*generate the primary header for the table*
        """
        self.log.debug('starting the ``_generate_primary_header`` method')

        # GET ALL PROGIDS
        sqlQuery = f"""
            select distinct PROG_ID from view_ssdr_efosc_spectra_binary_tables where PROG_ID is not null order by PROG_ID asc;
        """
        progIds = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )
        progIds[:] = [p["PROG_ID"] for p in progIds]

        # GET ALL PROGIDS
        sqlQuery = f"""
            select distinct PROCSOFT from view_ssdr_efosc_spectra_binary_tables where PROCSOFT is not null order by PROCSOFT desc;
        """
        procsoft = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )
        procsoft[:] = [p["PROCSOFT"] for p in procsoft]
        procsoft = procsoft[0]

        # GET ALL MJDs
        sqlQuery = f"""
            select distinct TRANSIENT_CLASSIFICATION_MJD from PESSTO_TRAN_CAT where TRANSIENT_CLASSIFICATION_MJD is not null order by TRANSIENT_CLASSIFICATION_MJD desc;
        """
        mjds = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )
        mjds[:] = [p["TRANSIENT_CLASSIFICATION_MJD"] for p in mjds]
        mjdMax = mjds[0]
        mjdMin = mjds[-1]

        now = datetime.now()
        now = now.strftime("%Y-%m-%dT%H:%M:%S")

        primHdu = fits.PrimaryHDU()
        primHdu.header["ORIGIN"] = ("ESO", "European Southern Observatory")
        primHdu.header["DATE"] = (now, "Date the file was written")
        primHdu.header["TELESCOP"] = ("ESO-NTT", "ESO Telescope designation")
        primHdu.header["INSTRUME"] = ("EFOSC", "Instrument name")
        primHdu.header["EQUINOX"] = 2000
        primHdu.header["MJD-OBS"] = (mjdMin, "Start of observations (days)")
        primHdu.header["MJD-END"] = (mjdMax, "End of observations (days)")
        primHdu.header["RADECSYS"] = ("FK5", "Coordinate reference frame")
        primHdu.header["PROG_ID"] = ("MULTI", "ESO programme identification")
        for i, progId in enumerate(progIds):
            ii = i + 1
            primHdu.header[f"PROGID{ii}"] = (progId, "ESO programme identification")
        primHdu.header["OBSTECH"] = ("SPECTRUM", "Technique of observation")
        primHdu.header["PROCSOFT"] = (
            procsoft, "Data reduction software/system with version no.")
        primHdu.header["PRODCATG"] = (
            "SCIENCE.CATALOG", "Data product category")
        primHdu.header["REFERENC"] = (
            "2015A&A...579A..40S", "Bibcode reference")

        self.log.debug('completed the ``_generate_primary_header`` method')
        return primHdu

    # use the tab-trigger below for new method
    # xt-class-method
