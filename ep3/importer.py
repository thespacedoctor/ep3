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

# OR YOU CAN REMOVE THE CLASS BELOW AND ADD A WORKER FUNCTION ... SNIPPET TRIGGER BELOW
# xt-worker-def


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

        - add usage info
        - create a sublime snippet for usage
        - create cl-util for this class
        - add a tutorial about ``importer`` to documentation
        - create a blog post about what ``importer`` does
    ```

    ```python
    usage code
    ```

    """
    # Initialisation
    # 1. @flagged: what are the unique attrributes for each object? Add them
    # to __init__

    def __init__(
            self,
            log,
            settings=False,

    ):
        self.log = log
        log.debug("instansiating a new 'importer' object")
        self.settings = settings
        # xt-self-arg-tmpx

        # 2. @flagged: what are the default attrributes each object could have? Add them to variable attribute set here
        # Variable Data Atrributes

        # 3. @flagged: what variable attrributes need overriden in any baseclass(es) used
        # Override Variable Data Atrributes

        # Initial Actions

        return None

    # 4. @flagged: what actions does each object have to be able to perform? Add them here
    # Method Attributes
    def get(self):
        """
        *get the importer object*

        **Return:**
            - ``importer``

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
        self.log.debug('starting the ``get`` method')

        importer = None

        self.log.debug('completed the ``get`` method')
        return importer

    def import_single_frame(
            self,
            pathToFitsFile):
        """*import a single fits frame into the database and nested folder archive*

        **Key Arguments:**
            - ``pathToFitsFile`` -- the path to the fits file we wish to ingest
            - ``pathToArchiveRoot`` -- path to the root of the archive

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
        self.log.debug('starting the ``import_single_frame`` method')

        # DECIDE WHICH EXTENSION WE NEED
        basename = os.path.basename(pathToFitsFile)
        if "mask" in basename[:4] or "_sb.fits" in basename:
            headerExtension = 1
        else:
            headerExtension = 0

        # OPEN FITS FILE & GRAB FITS HEADER
        hdulist = fits.open(pathToFitsFile)
        hdr = hdulist[0].header

        # ADD EXTRA INFO TO THE DICTIONARY
        hdr["filename"] = (basename, "filename")
        hdr["filePath"] = (
            str(os.path.abspath(pathToFitsFile)), "the path to the file")
        hdr["headerExtension"] = (
            headerExtension, "the fits header extension that was ingested")

        # CONVERT BOOLEAN ENTRIES TO STRINGS
        dictCopy = hdr
        print(repr(hdr))
        for k, v in list(hdr.items()):
            if type(v) == bool:
                if v is True:
                    dictCopy[k] = "T"
                else:
                    dictCopy[k] = "F"
            if k == "RELEASE":
                dictCopy["RRELEASE"] = v
                del dictCopy[k]
        hdr = dictCopy

        # FIX EDGE-CASE CRAP
        if "________________OG530_/ADA_GUID_DEC".replace("_", " ") in hdr:
            hdr["ESO_ADA_GUID_DEC"] = hdr[
                "________________OG530_/ADA_GUID_DEC".replace("_", " ")]
            del hdr[
                "________________OG530_/ADA_GUID_DEC".replace("_", " ")]

        # DETERMINE FITS TYPE
        mysqlTableName = self.fitstype(hdr=hdr)
        print(mysqlTableName)

        # ADD ROW TO DATABASE

        # SEND FILE TO NESTED FOLDERS

        self.log.debug('completed the ``import_single_frame`` method')
        return None

    # use the tab-trigger below for new method
    def fitstype(
            self,
            hdr):
        """*determine the type of fits file - which MySQL table to add it it*

        **Key Arguments:**
            - ``hdr`` -- the fitsheader

        **Return:**
            - ``mysqlTableName`` -- the MySQL table to add the details to

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
        self.log.debug('starting the ``fitstype`` method')

        ## VARIABLES ##
        instrument = ""
        mode = 0

        if "corrupted" in hdr:
            mysqlTableName = "corrupted_files"
        elif "sens_merge" in hdr["filename"]:
            mysqlTableName = "corrupted_files"
        elif "_sb.fits" in hdr["filename"] and "merge" in hdr["filename"]:
            mysqlTableName = "sofi_spectra_binary_table_extension"
        elif "_sb.fits" in hdr["filename"]:
            mysqlTableName = "efosc_spectra_binary_table_extension"
        else:

            if "INSTRUME" not in hdr:
                message = ""
                for k, v in list(hdr.items()):
                    message += "%s: %s\n" % (k, v)
                message += '"INSTRUME" keyword missing or blank for file %s[%s]' % (
                    hdr['filePath'], hdr['headerExtension'],)
                self.log.critical(message)
                raise ValueError(message)

            if hdr["INSTRUME"].lower() == "efosc":
                instrument = "efosc"
            elif hdr["INSTRUME"].lower() == "sofi":
                instrument = "sofi"
            else:
                message = '"INSTRUME" keyword missing or blank'
                self.log.critical(message)
                raise ValueError(message)

            if "ESO TPL NAME" in list(hdr.keys()):
                if "spectr" in hdr["ESO TPL NAME"].lower():
                    mode = "spectra"
                elif "image" in hdr["ESO TPL NAME"].lower():
                    mode = "imaging"
            if mode == 0 and "ESO DPR TECH" in list(hdr.keys()):
                if "spectr" in hdr["ESO DPR TECH"].lower():
                    mode = "spectra"
                elif "image" in hdr["ESO DPR TECH"].lower():
                    mode = "imaging"
            if mode == 0 and "OBSTECH" in list(hdr.keys()):
                if "spectr" in hdr["OBSTECH"].lower():
                    mode = "spectra"
                elif "image" in hdr["OBSTECH"].lower():
                    mode = "imaging"
            elif mode == 0 and "ESO INS GRIS1 NAME" in list(hdr.keys()):
                if "foc_wedge" in hdr["ESO INS GRIS1 NAME"].lower():
                    mode = "imaging"

            if mode == 0:
                message = '"ESO DPR TECH" and "ESO TPL NAME" and "OBSTECH" missing or incorrect for file %s, mode set to "%s"' % (
                    hdr["filePath"], mode)
                self.log.critical(message)
                raise ValueError(message)

            mysqlTableName = """%s_%s""" % (instrument, mode)

        self.log.debug('completed the ``fitstype`` method')
        return mysqlTableName

    # use the tab-trigger below for new method
    # xt-class-method

    # 5. @flagged: what actions of the base class(es) need ammending? ammend them here
    # Override Method Attributes
    # method-override-tmpx
