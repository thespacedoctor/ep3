#!/usr/local/bin/python
# encoding: utf-8
"""
*Associate EFOSC images with the closest EFOSC spectrum in time of the same object*

:Author:
    David Young

:Date Created:
    January 7, 2015

.. todo::
    

Usage:
    pm_associate_efosc_images_with_spectra -s <pathToSettingsFile>

    -h, --help            show this help message
    -s, --settings        the settings file
"""
################# GLOBAL IMPORTS ####################
import sys
import os
import readline
import glob
import pickle
from docopt import docopt
from dryxPython import logs as dl
from dryxPython import mysql as dms
from dryxPython import commonutils as dcu
from fundamentals import tools


def main(arguments=None):
    """
    *The main function used when ``associate_efosc_images_with_spectra.py`` is run as a single script from the cl, or when installed as a cl command*
    """
    ########## IMPORTS ##########
    ## STANDARD LIB ##
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##

    # setup the command-line util settings
    from fundamentals import tools
    su = tools(
        arguments=arguments,
        docString=__doc__,
        logLevel="DEBUG"
    )
    arguments, settings, log, dbConn = su.setup()

    # unpack remaining cl arguments using `exec` to setup the variable names
    # automatically
    for arg, val in arguments.iteritems():
        if arg[0] == "-":
            varname = arg.replace("-", "") + "Flag"
        else:
            varname = arg.replace("<", "").replace(">", "")
        if isinstance(val, str) or isinstance(val, unicode):
            exec(varname + " = '%s'" % (val,))
        else:
            exec(varname + " = %s" % (val,))
        if arg == "--dbConn":
            dbConn = val
        log.debug('%s = %s' % (varname, val,))

    ## START LOGGING ##
    startTime = dcu.get_now_sql_datetime()
    log.info(
        '--- STARTING TO RUN THE associate_efosc_images_with_spectra.py AT %s' %
        (startTime,))

    # call the worker function
    # x-if-settings-or-database-credientials
    assoc = associate_efosc_images_with_spectra(
        log=log,
        dbConn=dbConn
    )
    assoc.get()

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info(
        '-- FINISHED ATTEMPT TO RUN THE associate_efosc_images_with_spectra.py AT %s (RUNTIME: %s) --' %
        (endTime, runningTime, ))

    return

###################################################################
# CLASSES                                                         #
###################################################################


class associate_efosc_images_with_spectra():

    """
    *The worker class for the associate_efosc_images_with_spectra module*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger

    .. todo::

    """
    # Initialisation

    def __init__(
            self,
            log,
            dbConn=False,

    ):
        self.log = log
        log.debug(
            "instansiating a new 'associate_efosc_images_with_spectra' object")
        self.dbConn = dbConn
        # xt-self-arg-tmpx

        # Initial Actions
        self._select_efosc_image_data()
        self._select_efosc_spectra_data()

        return None

    def close(self):
        del self
        return None

    # Method Attributes
    def get(self):
        """
        *get the associate_efosc_images_with_spectra object*

        **Return:**
            - ``associate_efosc_images_with_spectra``

        .. todo::

        """
        self.log.debug('starting the ``get`` method')

        self._match_images_to_spectra()

        self.log.debug('completed the ``get`` method')
        return None

    def _select_efosc_image_data(
            self):
        """
        *select efosc image names*

        **Key Arguments:**
            # -

        **Return:**
            - None

        .. todo::

        """
        self.log.debug('starting the ``_select_efosc_image_data`` method')

        sqlQuery = u"""
            select currentFilename, object, MJD_OBS, primaryId from efosc_imaging where esoPhaseIII = 1;

        """ % locals()
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.dbConn,
            log=self.log
        )

        self.imageObjects = []
        self.imageFilename = []
        self.imageMJD = []
        self.imageID = []

        for row in rows:
            self.imageObjects.append(row["object"])
            self.imageFilename.append(row["currentFilename"])
            self.imageMJD.append(row["MJD_OBS"])
            self.imageID.append(row["primaryId"])

        self.log.debug('completed the ``_select_efosc_image_data`` method')
        return None

    # use the tab-trigger below for new method
    def _select_efosc_spectra_data(
            self):
        """
        *select efosc spectra data*

        **Key Arguments:**
            # -

        **Return:**
            - None

        .. todo::

        """
        self.log.debug('starting the ``_select_efosc_spectra_data`` method')

        sqlQuery = u"""
            select currentFilename, MJD_OBS, primaryId, object from efosc_spectra where esoPhaseIII = 1 and currentFilename like "%%_sb.fits"
        """ % locals()
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.dbConn,
            log=self.log
        )

        self.spectrumDict = {}

        for row in rows:
            if row["object"] in self.spectrumDict.keys():
                self.spectrumDict[row["object"]]["mjd"].append(row["MJD_OBS"])
                self.spectrumDict[row["object"]][
                    "filename"].append(row["currentFilename"])
                self.spectrumDict[row["object"]]["id"].append(row["primaryId"])
            else:
                self.spectrumDict[row["object"]] = {}
                self.spectrumDict[row["object"]]["mjd"] = [row["MJD_OBS"]]
                self.spectrumDict[row["object"]][
                    "filename"] = [row["currentFilename"]]
                self.spectrumDict[row["object"]]["id"] = [row["primaryId"]]

        self.log.debug('completed the ``_select_efosc_spectra_data`` method')
        return None

    def _match_images_to_spectra(
            self):
        """
        *match images to spectra*

        **Key Arguments:**
            # -

        **Return:**
            - None

        .. todo::

        """
        self.log.debug('starting the ``_match_images_to_spectra`` method')

        self.matchedSpectrumFilename = []
        self.matchedSpectrumId = []

        for iobject, ifilename, imjd, iid in zip(self.imageObjects, self.imageFilename, self.imageMJD, self.imageID):
            if iobject in self.spectrumDict.keys():
                closestMatch = 1000000000000
                for aMjd, aFilename, aId in zip(self.spectrumDict[iobject]["mjd"], self.spectrumDict[iobject]["filename"], self.spectrumDict[iobject]["id"]):
                    mjdDiff = abs(imjd - aMjd)
                    if mjdDiff < closestMatch:
                        closestMatch = mjdDiff
                        matchSpectrumFilename = aFilename
                        matchSpectrumId = aId
                self.matchedSpectrumFilename.append(matchSpectrumFilename)
                self.matchedSpectrumId.append(matchSpectrumId)
            else:
                self.matchedSpectrumFilename.append(None)
                self.matchedSpectrumId.append(None)

        for iobject, ifilename, imjd, iid, sfilename, sid in zip(self.imageObjects, self.imageFilename, self.imageMJD, self.imageID, self.matchedSpectrumFilename, self.matchedSpectrumId):
            sqlQuery = u"""
                update efosc_imaging set ASSOC_SPECTRUM_NAME = "%(sfilename)s", ASSOC_SPECTRUM_ID = %(sid)s where primaryId = %(iid)s   and lock_row = 0
            """ % locals()

            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=self.dbConn,
                log=self.log
            )

        self.log.debug('completed the ``_match_images_to_spectra`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method


if __name__ == '__main__':
    main()
