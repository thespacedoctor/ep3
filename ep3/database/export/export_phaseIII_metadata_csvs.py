#!/usr/local/bin/python
# encoding: utf-8
"""
*Export views of efosc/sofi imaging and spectra to the exports folder of webapp*

:Author:
    David Young

:Date Created:
    February 4, 2015

.. todo::
    
    @review: when complete pull all general functions and classes into dryxPython

Usage:
    pm_export_phaseIII_metadata_csvs -s <pathToSettingsFile>
    
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
from dryxPython import commonutils as dcu
from fundamentals import tools
# from ..__init__ import *


def tab_complete(text, state):
    return (glob.glob(text + '*') + [None])[state]


def main(arguments=None):
    """
    *The main function used when ``export_phaseIII_metadata_csvs.py`` is run as a single script from the cl, or when installed as a cl command*
    """
    # setup the command-line util settings
    su = tools(
        arguments=arguments,
        docString=__doc__,
        logLevel="DEBUG",
        options_first=False
    )
    arguments, settings, log, dbConn = su.setup()

    # tab completion for raw_input
    readline.set_completer_delims(' \t\n;')
    readline.parse_and_bind("tab: complete")
    readline.set_completer(tab_complete)

    # unpack remaining cl arguments using `exec` to setup the variable names
    # automatically
    for arg, val in arguments.items():
        if arg[0] == "-":
            varname = arg.replace("-", "") + "Flag"
        else:
            varname = arg.replace("<", "").replace(">", "")
        if isinstance(val, ("".__class__, u"".__class__)) :
            exec(varname + " = '%s'" % (val,))
        else:
            exec(varname + " = %s" % (val,))
        if arg == "--dbConn":
            dbConn = val
        log.debug('%s = %s' % (varname, val,))

    ## START LOGGING ##
    startTime = dcu.get_now_sql_datetime()
    log.info(
        '--- STARTING TO RUN THE export_phaseIII_metadata_csvs.py AT %s' %
        (startTime,))

    # call the worker function
    # x-if-settings-or-database-credientials
    exporter = export_phaseIII_metadata_csvs(
        log=log,
        dbConn=dbConn,
        settings=settings
    )
    exporter.get()

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info('-- FINISHED ATTEMPT TO RUN THE export_phaseIII_metadata_csvs.py AT %s (RUNTIME: %s) --' %
             (endTime, runningTime, ))

    return

###################################################################
# CLASSES                                                         #
###################################################################


class export_phaseIII_metadata_csvs():

    """
    *The worker class for the export_phaseIII_metadata_csvs module*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``settings`` -- settings

    .. todo::

        - @review: when complete, clean export_phaseIII_metadata_csvs class
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract class to another module
    """
    # Initialisation
    # 1. @flagged: what are the unique attrributes for each object? Add them
    # to __init__

    def __init__(
            self,
            log,
            settings,
            dbConn=False,
    ):
        self.log = log
        log.debug("instansiating a new 'export_phaseIII_metadata_csvs' object")
        self.dbConn = dbConn
        self.settings = settings
        # xt-self-arg-tmpx

        # 2. @flagged: what are the default attrributes each object could have? Add them to variable attribute set here
        # Variable Data Atrributes

        # 3. @flagged: what variable attrributes need overriden in any baseclass(es) used
        # Override Variable Data Atrributes

        # Initial Actions

        return None

    def close(self):
        del self
        return None

    # 4. @flagged: what actions does each object have to be able to perform? Add them here
    # Method Attributes
    def get(self):
        """
        *get the export_phaseIII_metadata_csvs object*

        **Return:**
            - ``export_phaseIII_metadata_csvs``

        .. todo::

            - @review: when complete, clean get method
            - @review: when complete add logging
        """
        self.log.debug('starting the ``get`` method')

        import dryxPython.csvtools as dct
        import codecs

        releaseVersions = ["ssdr1", "ssdr2"]
        instruments = ["efosc", "sofi"]
        fileTypes = ["imaging", "spectra"]

        exportDir = self.settings[
            "stats cache directory"] + "/phaseIII/csv_exports/"

        dcu.dryx_mkdir(
            log=self.log,
            directoryPath=exportDir
        )

        for rv in releaseVersions:
            for inst in instruments:
                for fileType in fileTypes:
                    for csvType in ["human", "machine"]:
                        sqlQuery = "select * from view_%(inst)s_%(fileType)s_%(rv)s" % locals(
                        )

                        if csvType == "human":
                            csvTitle = "%(inst)s %(fileType)s metadata for ESO Phase III, PESSTO %(rv)s" % locals(
                            )
                        else:
                            csvTitle = False

                        csvOutput = dct.sqlquery_to_csv_file.sqlquery_to_csv_file(
                            dbConn=self.dbConn,
                            log=self.log,
                            sqlQuery=sqlQuery,
                            csvType=csvType,  # human or machine
                            csvTitle=csvTitle,
                            csvFilename="filename",
                            # plainText | webpageDownload | webpageView
                            returnFormat="plainText"
                        )

                        if csvType == "human":
                            filepath = "%(exportDir)s/%(inst)s_%(fileType)s_%(rv)s_metadata.dat" % locals(
                            )
                        else:
                            filepath = "%(exportDir)s/%(inst)s_%(fileType)s_%(rv)s_metadata.csv" % locals(
                            )
                        writeFile = codecs.open(
                            filepath, encoding='utf-8', mode='w')
                        writeFile.write(csvOutput)

        import os
        import zipfile
        hostDirPath = self.settings["stats cache directory"] + "/phaseIII"
        zipfile = zipfile.ZipFile(
            hostDirPath + '/phaseIII_csv_exports.zip', 'w')
        os.chdir(exportDir)
        for root, dirs, files in os.walk(exportDir):
            for file in files:
                zipfile.write(os.path.join(root, file).replace(exportDir, ""))
        zipfile.close()

        self.log.debug('completed the ``get`` method')
        return export_phaseIII_metadata_csvs
    # xt-class-method

    # 5. @flagged: what actions of the base class(es) need ammending? ammend them here
    # Override Method Attributes
    # method-override-tmpx

# xt-class-tmpx


###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# xt-worker-def

# use the tab-trigger below for new function
# xt-def-with-logger

###################################################################
# PRIVATE (HELPER) FUNCTIONS                                      #
###################################################################

if __name__ == '__main__':
    main()
