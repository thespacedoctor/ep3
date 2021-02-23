#!/usr/bin/env python
# encoding: utf-8
"""
*Export frames needed for a ESO data release*

:Author:
    David Young

:Date Created:
    February 11, 2021
"""
from builtins import object
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from fundamentals.mysql import readquery
import shutil
# OR YOU CAN REMOVE THE CLASS BELOW AND ADD A WORKER FUNCTION ... SNIPPET TRIGGER BELOW
# xt-worker-def


class export_ssdr(object):
    """
    *The worker class for the export_ssdr module*

    **Key Arguments:**
        - ``log`` -- logger
        - ``dbConn`` -- the database connection
        - ``settings`` -- the settings dictionary
        - ``exportPath`` -- path to export the frames to
        - ``ssdr`` -- the number of the data release to export files for
        - ``instrument`` -- instrument to export frames for (efosc or sofi). Default *False*, i.e. release both efosc and sofi
        - ``fileType`` - the type of files to release (image, weight, spec1d, spec2d, bintable). Default *False*, i.e. release all file types

    **Usage:**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_). 

    To initiate a export_ssdr object, use the following:

    ```eval_rst
    .. todo::

        - create cl-util for this class
        - add a tutorial about ``export_ssdr`` to documentation
        - create a blog post about what ``export_ssdr`` does
    ```

    To export all frames needed for SSDR4:

    ```python
    from ep3 import export_ssdr
    exporter = export_ssdr(
        log=log,
        dbConn=dbConn,
        settings=settings,
        exportPath=pathToOutputDir + "/exported",
        ssdr=4,
        instrument=False,
        fileType=False
    )
    exporter.export() 
    ```

    Or to export EFOSC 1D spectral frames (not needed for Phase III release):

    ```python
    from ep3 import export_ssdr
    exporter = export_ssdr(
        log=log,
        dbConn=dbConn,
        settings=settings,
        exportPath=pathToOutputDir + "/exported",
        ssdr=4,
        instrument='efosc',
        fileType='spec1d'
    )
    exporter.export() 
    ```

    """

    def __init__(
            self,
            log,
            dbConn,
            settings,
            exportPath,
            ssdr,
            instrument=False,
            fileType=False
    ):
        self.log = log
        log.debug("instansiating a new 'export_ssdr' object")
        self.settings = settings
        self.dbConn = dbConn
        self.ssdr = ssdr
        if instrument:
            self.instrument = instrument.lower()
        else:
            self.instrument = instrument

        if fileType:
            self.fileType = fileType.lower()
        else:
            self.fileType = fileType

        self.exportPath = exportPath

        # xt-self-arg-tmpx

        # 2. @flagged: what are the default attrributes each object could have? Add them to variable attribute set here
        # Variable Data Atrributes

        # 3. @flagged: what variable attrributes need overriden in any baseclass(es) used
        # Override Variable Data Atrributes

        # Initial Actions

        return None

    def export(self):
        """
        *export the fits files requested*

        **Return:**
            - ``export_ssdr``

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
        self.log.debug('starting the ``export`` method')

        if self.ssdr != "all":
            data_rel = f" and data_rel = 'SSDR{self.ssdr}'"
        else:
            data_rel = ""

        exportDict = {}
        if self.instrument is False or self.instrument == "efosc":
            # NEED TO EXPLICATELY ASK FOR EFOSC IMAGING
            if self.fileType == "image":
                print(
                    "EFOSC images are not supported yet - need to create a database view to export the frames from")
                return
            if self.fileType == "weight":
                print("EFOSC image weights is not a valid option")
                return
            if self.fileType == "spec1d":
                exportDict[
                    "efosc_spec1d"] = f"select archivePath, filename, exportFilename from view_ssdr_supplimentary_efosc_spectra_1d where exportFilename is not null {data_rel}"
            if self.fileType is False or self.fileType == "spec2d":
                exportDict[
                    "efosc_spec2d"] = f"select archivePath, filename, exportFilename from view_ssdr_efosc_spectra_2d where exportFilename is not null {data_rel}"
            if self.fileType is False or self.fileType == "bintable":
                exportDict[
                    "efosc_specbin"] = f"select archivePath, filename, exportFilename from view_ssdr_efosc_spectra_binary_tables where exportFilename is not null {data_rel}"

        if self.instrument is False or self.instrument == "sofi":
            # NEED TO EXPLICATELY ASK FOR EFOSC IMAGING
            if self.fileType is False or self.fileType == "image":
                exportDict[
                    "sofi_images"] = f"select archivePath, filename, exportFilename from view_ssdr_sofi_imaging where exportFilename is not null {data_rel}"
            if self.fileType is False or self.fileType == "weight":
                exportDict[
                    "sofi_weights"] = f"select archivePath, filename, exportFilename from view_ssdr_sofi_imaging_weights where exportFilename is not null {data_rel}"
            if self.fileType == "spec1d":
                exportDict[
                    "sofi_spec1d"] = f"select archivePath, filename, exportFilename from view_ssdr_supplimentary_sofi_spectra_1d where exportFilename is not null {data_rel}"
            if self.fileType is False or self.fileType == "spec2d":
                exportDict[
                    "sofi_spec2d"] = f"select archivePath, filename, exportFilename from view_ssdr_sofi_spectra_2d where exportFilename is not null {data_rel}"
            if self.fileType is False or self.fileType == "bintable":
                exportDict[
                    "sofi_specbin"] = f"select archivePath, filename, exportFilename from view_ssdr_sofi_spectra_binary_tables where exportFilename is not null {data_rel}"

        # RUN THE QUERIES AND COPY FILES TO EXPORT LOCATION
        for path, sqlQuery in exportDict.items():
            if self.ssdr == "all":
                exportPath = self.exportPath + f"/ssdr_{self.ssdr}/" + path.replace("_", "/")
            else:
                exportPath = self.exportPath + f"/ssdr{self.ssdr}/" + path.replace("_", "/")
            # RECURSIVELY CREATE MISSING DIRECTORIES
            if not os.path.exists(exportPath):
                os.makedirs(exportPath)
            rows = readquery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.dbConn
            )

            src, dest = zip(*[(f"{self.settings['archive-root']}/{row['archivePath']}/{row['filename']}", f"{exportPath}/{row['exportFilename']}") for row in rows])

            for s, d in zip(src, dest):
                shutil.copyfile(s, d)

        self.log.debug('completed the ``export`` method')
        return export_ssdr

    # xt-class-method

    # 5. @flagged: what actions of the base class(es) need ammending? ammend them here
    # Override Method Attributes
    # method-override-tmpx
