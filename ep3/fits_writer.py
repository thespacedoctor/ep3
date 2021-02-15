#!/usr/bin/env python
# encoding: utf-8
"""
*refresh fits headers of all reduced science frames and write binary fits tables*

:Author:
    David Young

:Date Created:
    February  5, 2021
"""
from builtins import object
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from fundamentals.mysql import readquery
import datetime
from decimal import Decimal
from fundamentals import fmultiprocess
from astrocalc.times import conversions
from astrocalc.coords import unit_conversion
from astropy.io import fits
import numpy as np
from .clean import clean


class fits_writer(object):
    """
    *refresh fits headers of all reduced science frames and write binary fits tables*

    **Key Arguments:**
        - ``log`` -- logger
        - ``dbConn`` -- database connection
        - ``settings`` -- the settings dictionary

    **Usage:**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_).

    To initiate a fits_writer object, use the following:

    ```eval_rst
    .. todo::

        - create cl-util for this class
        - add a tutorial about ``fits_writer`` to documentation
    ```

    ```python
    from ep3 import fits_writer
    writer = fits_writer(
        log=log,
        dbConn=dbConn,
        settings=settings
    )
    writer.write()
    ```
    """

    def __init__(
            self,
            log,
            dbConn,
            settings=False,

    ):
        self.log = log
        log.debug("instansiating a new 'fits_writer' object")
        self.settings = settings
        self.dbConn = dbConn
        self.keywordDict = {}

        return None

    def write(self):
        """
        *refresh fits headers and write fits binary frames for all reduced science products*

        **Return:**
            - ``newFramePaths`` -- list of paths to the frames that have had their headers refreshed
            - ``newTablePaths`` -- list of paths to the binary tables that have been rewritten

        ```python
        from ep3 import fits_writer
        writer = fits_writer(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        newFramePaths, newTablePaths = writer.write()
        ```
        """
        self.log.debug('starting the ``get`` method')

        # CLEAN UP FIRST
        cleaner = clean(
            log=self.log,
            dbConn=self.dbConn,
            settings=self.settings
        )
        cleaner.clean()

        # FIRST WRITE THE FITS FRAMES
        newFramePaths = []
        frameTypes = ["spec1D", "spec2D", "weight", "image"]
        instrument = ["efosc", "sofi"]
        # frameTypes = ["weight", "image"]
        # instrument = ["sofi"]
        for i in instrument:
            for t in frameTypes:
                # GENERARE THE PRIMARY HEADERS FOR ALL FRAMES NEEDING UPDATED
                hdrList, fitsPaths = self.get_primary_fits_headers(i, t)
                for h, f in zip(hdrList, fitsPaths):
                    # OPEN FITS FILE AS HDULIST - HDU (HEADER DATA UNIT) CONTAINS A HEADER AND A DATA ARRAY (IMAGE) OR
                    # TABLE.
                    with fits.open(f, "update") as hdul:
                        # APPEND THE NEW HEADER AND WRITE THE FRAME TO FILE
                        hdul[0].header = h
                        hdul.flush(output_verify='fix+warn')
                        newFramePaths.append(f)

        # NOW THE BINARY TABLES
        newTablePaths = []
        for instrument in ["efosc", "sofi"]:
            table = instrument + "_spectra"

            # GENERATE THE PRIMARY FITS HEADERS
            hdrList, fitsPaths = self.get_primary_fits_headers(
                instrument, "bintable")
            primaryHeaderDict = {f: h for h, f in zip(hdrList, fitsPaths)}

            # GRAB INFO NEEDED TO BUILD THE BINARY TABLES
            origFitsPaths, tableFitsPaths, fluxScalingFactors, fits_values, mysql_keywords, fits_keywords, fits_comments = self.get_binary_table_info_from_database(
                instrument=instrument)

            binHduList = []
            dataDictList = []

            dataDictList = []
            dataDictList[:] = [{"instrument": instrument, "origPath": origPath, "tablePath": tablePath, "primaryHdr": primaryHeaderDict[
                tablePath], "fluxScalingFactor": fluxScalingFactor, "values": values}
                for origPath, tablePath, fluxScalingFactor, values in
                zip(origFitsPaths, tableFitsPaths, fluxScalingFactors, fits_values)]

            filePaths = fmultiprocess(log=self.log, function=write_binary_table,
                                      inputArray=dataDictList, poolSize=False, timeout=300, mysql_keywords=mysql_keywords,
                                      fits_keywords=fits_keywords,
                                      fits_comments=fits_comments)
            newTablePaths += filePaths

        self.log.debug('completed the ``get`` method')
        return newFramePaths, newTablePaths

    def get_primary_fits_headers(
            self,
            instrument,
            frameType):
        """*generate and return a fits header given the keywords, values and comments*

        **Key Arguments:**
            - ``instrument`` -- efosc or sofi
            - ``frameType`` -- spec1D, spec2D, weight, image or bintable

        **Return:**
            - `hdrList` -- list of primary header objects to be added to the data frames
            - `fitsPaths` -- list of paths to fits files corresponding to the headers

        **Usage:**

        To generate the primary headers for all EFOSC 1D spectra

        ```python
        hdrList = writer.get_primary_fits_headers(
            instrument="efosc", frameType="spec1D")
        ```
        """
        self.log.debug('starting the ``get_primary_fits_header`` method')

        # GET THE FITS HEADER DETAILS
        mysql_keywords, fits_keywords, fits_comments, values = self.get_primary_header_keyword_values_from_database(
            instrument=instrument, frameType=frameType)

        # GENERATE A FRESH HDR OBJECT
        hdu = fits.PrimaryHDU()
        hdr = hdu.header

        # POPULATE THE HEADERS
        hdrList = fmultiprocess(log=self.log, function=_generate_primary_fits_hdr,
                                inputArray=list(values), poolSize=False, timeout=300, hdr=hdr, mysql_keywords=mysql_keywords,
                                fits_keywords=fits_keywords,
                                fits_comments=fits_comments)

        # GENERATE THE LIST OF FILE PATHS
        fitsPaths = []
        fitsPaths[:] = [self.settings["archive-root"] +
                        "/" + f["filepath"] for f in values]

        self.log.debug('completed the ``get_primary_fits_header`` method')
        return hdrList, fitsPaths

    def get_primary_header_keyword_values_from_database(
            self,
            instrument,
            frameType):
        """*return keywords, filenames, archive paths for the requested fits frames*

        **Key Arguments:**
            - ``instrument`` -- efosc or sofi
            - ``frameType`` -- spec1D, spec2D, weight, image or bintable

        **Return:**
            - `mysql_keywords` -- the keywords are stored in the mysql databasse
            - `fits_keywords` -- the keywords as to be written in the FITS headers
            - `fits_comments` -- the FITS header comments for each keyword
            - `values` -- a list of keyword-value dictionaries (using `mysql_keywords` keys)

        **Usage:**

        Here is some code to return the details needed to build the FITS headers for SOFI image-weight frames:

        ```python
        mysql_keywords, fits_keywords, fits_comments, values = writer.get_primary_header_keyword_values_from_database(
            instrument="sofi", frameType="weight"
        )
        ```
        """
        self.log.debug(
            'starting the ``get_primary_header_keyword_values_from_database`` method')

        if instrument == "efosc" and frameType == "weight":
            return [], [], [], []

        if instrument == "efosc" and frameType == "image":
            return [], [], [], []

        # DETERMINE WHAT WE ARE LOOKING AT
        frameTypes = [
            "spec1D",
            "spec2D",
            "weight",
            "image",
            "bintable"
        ]
        tag = [
            "spectrum_1d",
            "spectrum_2d",
            "image_weight",
            "image",
            "spectrum_binary_table"
        ]
        tableSuffixs = [
            "spectra",
            "spectra",
            "imaging",
            "imaging",
            "spectra"
        ]
        viewSuffixs = [
            "spectra_1d",
            "spectra_2d",
            "imaging_weights",
            "imaging",
            "spectra_binary_tables"
        ]

        whereExtras = [
            """PRODCATG = "SCIENCE.SPECTRUM" and binary_table_associated_spectrum_id = 0 and filetype_key_calibration = 13 """,
            """PRODCATG = "ANCILLARY.2DSPECTRUM" and filetype_key_calibration = 13""",
            """filetype_key_calibration = 11""",
            """PRODCATG = "SCIENCE.IMAGE" and filetype_key_calibration = 13""",
            """PRODCATG = "SCIENCE.SPECTRUM" and binary_table_associated_spectrum_id > 0 and filetype_key_calibration = 13 """
        ]

        index = frameTypes.index(frameType)
        suffix = tag[index]
        whereExtra = whereExtras[index]
        tableSuffix = tableSuffixs[index]
        viewSuffix = viewSuffixs[index]
        frameKey = f"{instrument}_{suffix}"

        # DETERMINE KEYWORDS NEEDED - OR LIFT FROM CACHE (self.keywordDict)
        if frameKey not in self.keywordDict:
            sqlQuery = f"""
                SELECT mysql_keyword, fits_keyword, fits_comment from fits_header_keywords where {instrument}_{suffix} = 1;
            """
            rows = readquery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.dbConn
            )
            mysql_keywords, fits_keywords, fits_comments = zip(
                *[(row["mysql_keyword"], row["fits_keyword"], row["fits_comment"]) for row in rows])
            self.keywordDict[
                frameKey] = mysql_keywords, fits_keywords, fits_comments
        else:
            mysql_keywords, fits_keywords, fits_comments = self.keywordDict[
                frameKey]

        # FIRST GET THE PRIMARY IDS
        if "_1d" in viewSuffix:
            viewName = f"view_ssdr_supplimentary_{instrument}_{viewSuffix}"
        else:
            viewName = f"view_ssdr_{instrument}_{viewSuffix}"
        sqlQuery = f"""
            select primaryId from {viewName} where lock_row = 0
        """
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )
        primaryIds = []
        primaryIds[:] = [str(r["primaryId"]) for r in rows]
        primaryIds = (",").join(primaryIds)

        # ASK FOR KEYWORD VALUES
        myKeys = ("`,`").join(mysql_keywords)
        sqlQuery = f"""
            select `{myKeys}`, concat(`archivePath`,"/",filename) as filepath from {instrument}_{tableSuffix} where primaryId in ({primaryIds})
        """
        values = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        self.log.debug(
            'completed the ``get_primary_header_keyword_values_from_database`` method')
        return mysql_keywords, fits_keywords, fits_comments, values

    def get_binary_table_info_from_database(
            self,
            instrument):
        """*return info, including extension header keyword values, from the database for all binary tables that need written*

        **Key Arguments:**
            - ``instrument`` -- which instrument (sofi or efosc)

        **Return:**
            - `origFitsPaths` -- the paths to the original 1D spectral frames
            - `tableFitsPaths` -- the paths to the required binary table files
            - `fluxScalingFactors` -- take scaling factor required for the flux in 1D spectral frames
            - `fits_values` -- the extension header values for the fits frames (same length and order as `origFitsPaths`)
            - `mysql_keywords` -- the keywords are stored in the mysql databasse
            - `fits_keywords` -- the keywords as to be written in the FITS headers
            - `fits_comments` -- the FITS header comments for each keyword

        **Usage:**

        To grab data for all EFOSC FITS binary tables to be written:

        ```python
        origFitsPaths, tableFitsPaths, fluxScalingFactors, fits_values, mysql_keywords, fits_keywords, fits_comments = writer.get_binary_table_info_from_database(instrument="efosc")
        ```
        """
        self.log.debug(
            'starting the ``get_binary_table_info_from_database`` method')

        table = instrument + "_spectra"
        view = f"view_ssdr_{instrument}_spectra_binary_tables"

        # FIRST GET THE PRIMARY IDS
        sqlQuery = f"""
            select primaryId from {view} where lock_row = 0
        """
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )
        primaryIds = []
        primaryIds[:] = [str(r["primaryId"]) for r in rows]
        primaryIds = (",").join(primaryIds)

        # FIRST GRAB THE INFO FOR THE FITS FILES
        sqlQuery = f"""
            select a.primaryId, concat("{self.settings["archive-root"]}/",a.archivePath,"/",a.filename) as orig_filepath,concat("{self.settings["archive-root"]}/",a.archivePath,"/",b.filename) as filepath, if(a.flux_scaling_factor,a.flux_scaling_factor,1) as scaleFactor from {table} a
INNER JOIN {table} b on a.primaryId=b.binary_table_associated_spectrum_id where b.primaryId in ({primaryIds});
        """
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        origFitsPaths, tableFitsPaths, fluxScalingFactors, primaryIds = zip(*[(row["orig_filepath"], row["filepath"],
                                                                               row["scaleFactor"], row["primaryId"]) for row in rows])

        # NOW GRAB DATA NEEDED FOR EXTENSION HEADERS
        if "efosc" in table:
            keywordTable = "efosc_spectrum_binary_table_extension"
            specId = "efosc_spectra_id"
        else:
            keywordTable = "sofi_spectrum_binary_table_extension"
            specId = "sofi_spectra_id"
        sqlQuery = f"""
            SELECT mysql_keyword, fits_keyword, fits_comment from fits_header_keywords where {keywordTable} = 1;
        """
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        mysql_keywords, fits_keywords, fits_comments = zip(
            *[(row["mysql_keyword"], row["fits_keyword"], row["fits_comment"]) for row in rows])

        # ASK FOR KEYWORD VALUES
        myKeys = ("`,e.`").join(mysql_keywords)
        primaryIdString = (",").join([str(p) for p in primaryIds])
        sqlQuery = f"""
            select e.`{myKeys}`, concat(s.archivePath,"/",s.filename) as filepath from {keywordTable} e, {table} s where e.{specId} = s.primaryId and s.binary_table_associated_spectrum_id in ({primaryIdString}) ORDER BY field(s.binary_table_associated_spectrum_id, {primaryIdString});
        """
        fits_values = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.dbConn
        )

        if len(fits_values) != len(primaryIds):
            message = f"Seems there are rows missing from the {keywordTable} table"
            self.log.error(message)
            raise LookupError(message)

        self.log.debug(
            'completed the ``get_binary_table_info_from_database`` method')
        return origFitsPaths, tableFitsPaths, fluxScalingFactors, fits_values, mysql_keywords, fits_keywords, fits_comments

    # use the tab-trigger below for new method
    # xt-class-method


def write_binary_table(
        dataDictList,
        log,
        mysql_keywords,
        fits_keywords,
        fits_comments):
    """*write a binary table file based on data in 1D spec frame*

    **Key Arguments:**

        - `dataDict` -- dictionary of data containing the following keys
            - `instrument` -- efosc or sofi
            - `origPath` -- path to the original 1D spec frame
            - `tablePath` -- path to write the binary table to
            - `primaryHdr` -- the primary header for the table
            - `fluxScalingFactor` -- scaling factor required for the flux in 1D spectral frame
            - `values` -- a list of keyword-value dictionaries (using `mysql_keywords` keys)
        - `log` -- logger
        - `mysql_keywords` -- the keywords are stored in the mysql databasse
        - `fits_keywords` -- the keywords as to be written in the FITS headers
        - `fits_comments` -- the FITS header comments for each keyword


    **Return:**
        - `tablePath` -- path of written binary table

    **Usage:**

    ```python
    tablePath = write_binary_table(
        log=self.log,
        dataDictList=d,
        mysql_keywords=mysql_keywords,
        fits_keywords=fits_keywords,
        fits_comments=fits_comments
    )
    ```
    """
    log.debug('starting the ``write_binary_table`` method')

    instrument = dataDictList["instrument"]
    origPath = dataDictList["origPath"]
    tablePath = dataDictList["tablePath"]
    primaryHdr = dataDictList["primaryHdr"]
    fluxScalingFactor = dataDictList["fluxScalingFactor"]
    values = dataDictList["values"]

    # OPEN THE 1D SPEC FILE AS READ-ONLY
    with fits.open(origPath, "readonly") as hdul:
        # PUSH FLUX, BACKGROUND, ERROR INTO 3 ARRAYS
        fluxData = hdul[0].data[0]
        backgroundData = hdul[0].data[2]
        sigmaData = hdul[0].data[3]

        # READ HEADER AND COUNT NUMBER OF PIXELS
        primHeader = hdul[0].header
        pixelCount = len(fluxData[0])

        # DETERMINE IF SOFI OR EFOSC
        if instrument.lower() == "efosc":
            voUCD = 'em.opt'
        else:
            voUCD = 'em.IR'

        # DETERMINE PIXEL WAVELENTH ARRAY
        if not primHeader['WAVELMIN'] or not primHeader['WAVELMIN']:
            log.error(f"The file {origPath} has no WAVELMIN and/or WAVELMIN set. Could not produce a binary table of the spectrum. Please correct the original 1D frame.")
            return None, None
        minWave = primHeader['WAVELMIN'] * 10.
        maxWave = primHeader['WAVELMAX'] * 10.
        rangeWave = maxWave - minWave
        pixelGaps = len(fluxData[0]) - 1
        deltaPixel = rangeWave / pixelGaps
        fluxError = primHeader['SPEC_ERR']
        wlArray = []
        for item in range(len(fluxData[0])):
            wlArray.append(item * deltaPixel + minWave)

        # BINTABLE COLUMNS : wl in Angstrom, flux in erg cm**(-2) s**(-1)
        # angstrom**(-1)
        wlArray = np.array([wlArray[:]], dtype=object)
        # SCALE THE FLUX ARRAY
        fluxArray = np.array([fluxData[0]], dtype=object) * \
            fluxScalingFactor
        backgroundDataArray = np.array(
            [backgroundData[0]], dtype=object)
        fluxErrArray = np.array([sigmaData[0]], dtype=object)
        wlCol = fits.Column(name='WAVE',
                            format='%sE' % (pixelCount,), unit='angstrom', array=wlArray)
        fluxCol = fits.Column(name='FLUX',
                              format='%sE' % (pixelCount,), unit='erg cm**(-2) s**(-1) angstrom**(-1)', array=fluxArray)
        fluxErrCol = fits.Column(name='ERR',
                                 format='%sE' % (pixelCount,), unit='erg cm**(-2) s**(-1) angstrom**(-1)', array=fluxErrArray)
        backgroundCol = fits.Column(name='SKY_BACKGROUND',
                                    format='%sE' % (pixelCount,), unit='erg cm**(-2) s**(-1) angstrom**(-1)', array=backgroundDataArray)

        # NOW COMBINE THE COLUMNS INTO BINTABLE HDU
        coldefs = fits.ColDefs(
            [wlCol, fluxCol, fluxErrCol, backgroundCol])
        binTableHdu = fits.BinTableHDU.from_columns(coldefs)

        # PREPARE THE FITS KEYWORD VALUES AND COMMENTS AND ADD TO
        # EXTENSION HEADER
        values, fits_comments = _prepare_fits_keywords(log,
                                                       mysql_keywords, fits_keywords, fits_comments, values)
        for m, f, c in zip(mysql_keywords, fits_keywords, fits_comments):
            v = values[m]
            if m in ["SIMPLE", "EXTEND", "NAXIS1", "NELEM"]:
                continue
            binTableHdu.header[f] = (v, c)

        # ADD A FEW MORE DYNAMIC KEYWORD VALUES AND COMMENTS
        for i in range(1, 5):
            binTableHdu.header.comments[
                f"TFORM{i}"] = f'Data format of field {i}'
        binTableHdu.header.comments[
            "NAXIS1"] = 'Length of data axis 1'
        nelem = len(fluxArray)
        binTableHdu.header['NELEM'] = (
            len(fluxArray[0]), 'Length of the data arrays')

        primaryHdu = fits.PrimaryHDU(
            header=primaryHdr, data=None)
        tableToWrite = fits.HDUList(
            [primaryHdu, binTableHdu])
        tableToWrite.writeto(
            tablePath, checksum=True, overwrite=True, output_verify='fix+warn')

    log.debug('completed the ``write_binary_table`` method')
    return tablePath


def _prepare_fits_keywords(
        log,
        mysql_keywords,
        fits_keywords,
        fits_comments,
        values):
    """*prepare the fits keyword values and comments to be written to the FITS header*

    **Key Arguments:**
        - `log` -- logger
        - `mysql_keywords` -- the keywords are stored in the mysql databasse
        - `fits_keywords` -- the keywords as to be written in the FITS headers
        - `fits_comments` -- the FITS header comments for each keyword
        - `values` -- a list of keyword-value dictionaries (using `mysql_keywords` keys)

    **Return:**
        - `cleanValues` -- prepared values
        - `cleanComment` -- prepared comments

    **Usage:**

    ```python
    fits_values, fits_comments = writer._prepare_fits_keywords(log,mysql_keywords, fits_keywords, fits_comments, fits_values)
    ```
    """
    log.debug('starting the ``_prepare_fits_keywords`` method')

    # CONVERTER TO CONVERT MJD TO DATE
    converter = conversions(
        log=log
    )

    # ASTROCALC UNIT CONVERTER OBJECT
    unit_converter = unit_conversion(
        log=log
    )

    cleanValues = {}
    cleanComments = []
    for m, f, c in zip(mysql_keywords, fits_keywords, fits_comments):
        v = values[m]
        # if m in ["SIMPLE", "EXTEND", "NAXIS1", "NELEM"]:
        #     continue

        if "HIERARCH" not in f and isinstance(v, ("".__class__, u"".__class__)) and len(v) > 68:
            v = v[0:68]
        elif v == "T":
            v = True
        elif v == "F":
            v = False
        elif isinstance(v, Decimal):
            v = float(v)

        if isinstance(v, datetime.datetime) or m == "DATE_OBS":
            if m == "DATE_OBS":
                v = converter.mjd_to_ut_datetime(
                    mjd=values["MJD_OBS"],
                    sqlDate=True,
                    datetimeObject=True
                )

            v = str(v).replace(" ", "T")
            v = v.split(".")[0]

        if m == "RA" and v:
            raSex = unit_converter.ra_decimal_to_sexegesimal(
                ra=v,
                delimiter=':'
            )
            c = raSex + " RA (J2000) pointing (deg)"
        elif m == "DECL" and v:
            decSex = unit_converter.dec_decimal_to_sexegesimal(
                dec=v,
                delimiter=':'
            )
            c = decSex + " DEC (J2000) pointing (deg)"
        elif m == "ESO_ADA_GUID_RA" and v:
            raSex = unit_converter.ra_decimal_to_sexegesimal(
                ra=v,
                delimiter=':'
            )
            c = raSex + " Guide Star RA J2000"
        elif m == "ESO_ADA_GUID_DEC" and v:
            decSex = unit_converter.dec_decimal_to_sexegesimal(
                dec=v,
                delimiter=':'
            )
            c = decSex + " Guide Star DEC J2000"
        elif m == "ESO_TEL_MOON_RA" and v:
            raSex = unit_converter.ra_decimal_to_sexegesimal(
                ra=v,
                delimiter=':'
            )
            c = raSex + " RA (J2000) (deg)"
        elif m == "ESO_TEL_MOON_DEC" and v:
            decSex = unit_converter.dec_decimal_to_sexegesimal(
                dec=v,
                delimiter=':'
            )
            c = decSex + " DEC (J2000) (deg)"
        elif m == "MJD_OBS" and v:
            utDate = converter.mjd_to_ut_datetime(
                mjd=v,
                sqlDate=True,
                datetimeObject=False
            )
            c = """MJD start (%(utDate)s)""" % locals()

        cleanValues[m] = v
        cleanComments.append(c)

    log.debug('completed the ``_prepare_fits_keywords`` method')
    return cleanValues, cleanComments


def _generate_primary_fits_hdr(
        fits_values,
        log,
        hdr,
        mysql_keywords,
        fits_keywords,
        fits_comments):
    """*generate the primary header for a single frame*

    **Key Arguments:**
        - ``hdr`` -- empty header
        - `log` -- logger
        - `mysql_keywords` -- the keywords are stored in the mysql databasse
        - `fits_keywords` -- the keywords as to be written in the FITS headers
        - `fits_comments` -- the FITS header comments for each keyword
        - `fits_values` -- a keyword-value dictionary (using `mysql_keywords` keys)

    **Return:**
        - `hdr` -- the FITS header
    """
    log.debug('starting the ``_generate_primary_fits_hdr`` method')

    # CONVERTER TO CONVERT MJD TO DATE
    converter = conversions(
        log=log
    )

    # ASTROCALC UNIT CONVERTER OBJECT
    unit_converter = unit_conversion(
        log=log
    )

    fits_values, fits_comments = _prepare_fits_keywords(log,
                                                        mysql_keywords, fits_keywords, fits_comments, fits_values)

    for m, f, c in zip(mysql_keywords, fits_keywords, fits_comments):
        v = fits_values[m]
        # EASY FIXES
        if "prov" in m[:6].lower() and v is None:
            continue
        # NOW JUST ADD THE NEW CARD
        hdr[f] = (v, c)

    log.debug('completed the ``_generate_primary_fits_hdr`` method')
    return hdr
