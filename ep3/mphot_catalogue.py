#!/usr/bin/env python
# encoding: utf-8
"""
*Generate the MPHOT photometry catalogue in FITS binary format*

:Author:
    David Young

:Date Created:
    July  9, 2021
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


class mphot_catalogue(object):
    """
    *Tools to create the ESO data release photometry catalogue `PESSTO_MPHOT.fits`*

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
        - add a tutorial about ``mphot_catalogue`` to documentation
        - create a blog post about what ``mphot_catalogue`` does
    ```

    ```python
    from ep3 import mphot_catalogue
    cat = mphot_catalogue(
        log=log,
        dbConn=dbConn,
        outputDirectory="/path/to/output/",
        settings=settings
    )
    fitsPath = cat.create()
    ```

    """
    # Initialisation
    # 1. @flagged: what are the unique attrributes for each object? Add them
    # to __init__

    def __init__(
            self,
            log,
            dbConn,
            outputDirectory,
            settings=False
    ):
        self.log = log
        log.debug("instansiating a new 'transient_catalogue' object")
        self.settings = settings
        self.dbConn = dbConn

        home = expanduser("~")
        outputDirectory = outputDirectory.replace("~", home)

        self.pathToOutput = outputDirectory + "/PESSTO_MPHOT.fits"

        return None

    def create(self):
        """
        *create the photometry catalogue binary FITS table*

        **Return:**
            - ``mphot_catalogue``

        **Usage:**

        ```python
        from ep3 import mphot_catalogue
        cat = mphot_catalogue(
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

        # GET ALL MJDs
        sqlQuery = f"""
            select min(mjd) as minmjd, max(mjd) as maxmjd from pessto_mphot_cat;
        """
        mjds = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )
        mjdMax = float(mjds[0]["maxmjd"])
        mjdMin = float(mjds[0]["minmjd"])

        now = datetime.now()
        now = now.strftime("%Y-%m-%dT%H:%M:%S")

        primHdu = fits.PrimaryHDU()
        primHdu.header["ORIGIN"] = ("ESO", "European Southern Observatory")
        primHdu.header["DATE"] = (now, "Date the file was written")
        primHdu.header["TELESCOP"] = ("ESO-NTT", "ESO Telescope designation")
        primHdu.header["INSTRUME"] = ("MULTI", "Instrument name")
        primHdu.header["INSTR1"] = ("EFOSC", "Instrument name")
        primHdu.header["INSTR2"] = ("SOFI", "Instrument name")
        primHdu.header["NOESODAT"] = (True, "True if non-ESO data used")
        primHdu.header["EQUINOX"] = 2000
        primHdu.header["MJD-OBS"] = (mjdMin, "Start of observations (days)")
        primHdu.header["MJD-END"] = (mjdMax, "End of observations (days)")
        primHdu.header["RADESYS"] = ("FK5", "Coordinate reference frame")
        primHdu.header["PROG_ID"] = ("MULTI", "ESO programme identification")
        for i, progId in enumerate(progIds):
            ii = i + 1
            primHdu.header[f"PROGID{ii}"] = (progId, "ESO programme identification")
        primHdu.header["OBSTECH"] = ("IMAGE", "Technique of observation")
        primHdu.header["PRODCATG"] = (
            "SCIENCE.CATALOG", "Data product category")
        primHdu.header["PROCSOFT"] = (
            'MULTI', 'Data reduction software/system with version no.')
        primHdu.header["REFERENC"] = (
            "2015A&A...579A..40S", "Bibcode reference")

        self.log.debug('completed the ``_generate_primary_header`` method')
        return primHdu

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
            select * from pessto_mphot_cat_column_desc
        """
        coldesc = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        # GET THE DATA
        sqlQuery = f"""
            select * from PESSTO_MPHOT_CAT order by PHOT_ID
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

            col["TTYPE"] = col["TTYPE"].strip()

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
            print(after)
            for k in ["TPRIC", "TINDX", "TUCD", "TCOMM", "TXLNK", "TXRGF", "TXCTY"]:
                if col[k]:
                    if col[k] == "T":
                        col[k] = True
                    key = f'{k}{col["columnNumber"]}'
                    binTableHdu.header.insert(key=after, card=(key,
                                                               col[k]), after=True)

        self.log.debug('completed the ``generate_data_table_unit`` method')
        return binTableHdu
