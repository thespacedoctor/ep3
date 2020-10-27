#!/usr/local/bin/python
# encoding: utf-8
"""
*Export the PESSTO Atels in format useful for Phase III*

:Author:
    David Young

.. todo::
    
    @review: when complete pull all general functions and classes into dryxPython

Usage:
    pm_export_pessto_atels -s <pathToSettingsFile>

    -h, --help            show this help message
    -v, --version         show version
    -s, --settings        the settings file
"""
from builtins import str
import sys
import os
import codecs
from docopt import docopt
import re
import string
from fundamentals.mysql import readquery
from fundamentals import tools
# from ..__init__ import *

def main(arguments=None):
    """
    *The main function used when ``export_pessto_atels.py`` is run as a single script from the cl, or when installed as a cl command*
    """
    # setup the command-line util settings
    su = tools(
        arguments=arguments,
        docString=__doc__,
        logLevel="DEBUG"
    )
    arguments, settings, log, dbConn = su.setup()

    # unpack remaining cl arguments using `exec` to setup the variable names
    # automatically
    for arg, val in list(arguments.items()):
        if arg[0] == "-":
            varname = arg.replace("-", "") + "Flag"
        else:
            varname = arg.replace("<", "").replace(">", "")
        if isinstance(val, ("".__class__, u"".__class__)):
            exec(varname + " = '%s'" % (val,))
        else:
            exec(varname + " = %s" % (val,))
        if arg == "--dbConn":
            dbConn = val
        log.debug('%s = %s' % (varname, val,))

    ## START LOGGING ##
    startTime = dcu.get_now_sql_datetime()
    log.info(
        '--- STARTING TO RUN THE export_pessto_atels.py AT %s' %
        (startTime,))

    # call the worker function
    # x-if-settings-or-database-credientials
    export_pessto_atels(
        log=log,
        dbConn=dbConn
    )

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info('-- FINISHED ATTEMPT TO RUN THE export_pessto_atels.py AT %s (RUNTIME: %s) --' %
             (endTime, runningTime, ))

    return

###################################################################
# CLASSES                                                         #
###################################################################
# xt-class-module-worker-tmpx
# xt-class-tmpx

###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# LAST MODIFIED : October 20, 2014
# CREATED : October 20, 2014
# AUTHOR : DRYX
def export_pessto_atels(
        dbConn,
        log,
        outputDirectory=""):
    """
    *export pessto atels*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    

    **Return**

    - None
    

    .. todo::

        - @review: when complete, clean export_pessto_atels function
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract function to another module
    """
    log.debug('starting the ``export_pessto_atels`` function')

    sqlQuery = u"""
        SELECT atelNumber, title, userText FROM atel_fullcontent where title like "%%PESSTO%%"
    """ % locals()
    rows = readquery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn
    )

    # open files to write to
    atelObjectsFile = outputDirectory + "/pessto_atel_objects.dat"
    uncapturedAtels = outputDirectory + "/pessto_uncaptured_atels.dat"
    writeFile = codecs.open(atelObjectsFile, encoding='utf-8', mode='w')
    writeFile2 = codecs.open(uncapturedAtels, encoding='utf-8', mode='w')
    for row in rows:
        atelNumber = row["atelNumber"]
        title = row["title"]
        userText = row["userText"]
        # print atelNumber
        # print title

        # convert bytes to unicode
        if isinstance(userText, ("".__class__, u"".__class__)):
            userText = str(userText, encoding="utf-8", errors="replace")
        userText = userText.encode("utf8")

        matchObject = re.search(r"<pre>(.*?)</pre>", userText, re.S)
        if matchObject:

            theseLines = string.split(matchObject.group(1), '\n')
            for line in theseLines:
                writeFile.write("""%(atelNumber)s  |  %(line)s\n""" % locals())
        else:
            writeFile2.write(
                """# %(atelNumber)s  \n  %(userText)s\n\n""" % locals())

        # log.debug("""row: `%(row)s`""" % locals())

    log.debug('completed the ``export_pessto_atels`` function')
    return None

# use the tab-trigger below for new function
# xt-def-with-logger

###################################################################
# PRIVATE (HELPER) FUNCTIONS                                      #
###################################################################

if __name__ == '__main__':
    main()
