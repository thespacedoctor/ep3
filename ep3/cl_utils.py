#!/usr/bin/env python
# encoding: utf-8
"""
Documentation for ep3 can be found here: http://ep3.readthedocs.org

Usage:
    ep3 init
    ep3 import [-s <pathToSettingsFile>]  
    ep3 clean [-s <pathToSettingsFile>]  
    ep3 match [<transientId> <fitsObject>] [-s <pathToSettingsFile>]  
    ep3 rewrite [-s <pathToSettingsFile>]  
    ep3 export <ssdr> <instrument> <fileType> <exportPath> [-s <pathToSettingsFile>]  
    ep3 esolist table <pathToCsv> <ssdr> [-s <pathToSettingsFile>]  
    ep3 esolist rename <pathToDownloads> [-s <pathToSettingsFile>]  
    ep3 transcat <pathToOutputDir> [-s <pathToSettingsFile>]
    ep3 reports [-s <pathToSettingsFile>]

Options:
    clean                                  run the MySQL stored procedures to clean up FITS keyword values in database space
    esolist                                tools to work with the file listing as hosted in the ESO SAF (useful for preping a new data release)
    export                                 export the frames needed for a data release
    import                                 import the NTT data into the database (headers) and the archive file-system
    init                                   setup the ep3 settings file for the first time
    match                                  match frames against transient names and coordinates, then update coordinates and object names in FITS frames and rename files accordingly
    rename                                 rename the frames downloaded from the ESO SAF
    rewrite                                refresh the fits headers based on what is in the database and regenerate all binary table files
    table                                  create a database table with the ESO CSV listing of files in a specific SSDR
    transcat                               generate the transient catalogue
    reports                                write out some useful reports for the release description

    <exportPath>                           path to export to frames to
    <fileType>                             image, weight, spec1d, spec2d, bintable or all
    <fitsObject>                           the object name in the FITS frames you want to manually force a match with frames whose coordinates are close to <transientId>
    <instrument>                           efosc, sofi or all
    <pathToCsv>                            path the the CSV file to
    <pathToDownloads>                      path to a directory of downloaded files
    <ssdr>                                 the SSDR number
    <transientId>                          the transient ID of the source you want to manually force a match with frames with object name <fitsObject>
    <pathToOutputDir>                      path to the directory to output the catalogue to

    -h, --help                             show this help message
    -v, --version                          show version
    -s, --settings <pathToSettingsFile>    the settings file
"""
import sys
import os
os.environ['TERM'] = 'vt100'
import readline
import glob
import pickle
from docopt import docopt
from fundamentals import tools, times
from subprocess import Popen, PIPE, STDOUT
from fundamentals.mysql import writequery
from os.path import expanduser


def tab_complete(text, state):
    return (glob.glob(text + '*') + [None])[state]


def main(arguments=None):
    """
    *The main function used when `cl_utils.py` is run as a single script from the cl, or when installed as a cl command*
    """
    # setup the command-line util settings
    su = tools(
        arguments=arguments,
        docString=__doc__,
        logLevel="ERROR",
        options_first=False,
        projectName="ep3",
        defaultSettingsFile=True
    )
    arguments, settings, log, dbConn = su.setup()

    # tab completion for raw_input
    readline.set_completer_delims(' \t\n;')
    readline.parse_and_bind("tab: complete")
    readline.set_completer(tab_complete)

    # UNPACK REMAINING CL ARGUMENTS USING `EXEC` TO SETUP THE VARIABLE NAMES
    # AUTOMATICALLY
    a = {}
    for arg, val in list(arguments.items()):
        if arg[0] == "-":
            varname = arg.replace("-", "") + "Flag"
        else:
            varname = arg.replace("<", "").replace(">", "")
        a[varname] = val
        if arg == "--dbConn":
            dbConn = val
            a["dbConn"] = val
        log.debug('%s = %s' % (varname, val,))

    ## START LOGGING ##
    startTime = times.get_now_sql_datetime()
    log.info(
        '--- STARTING TO RUN THE cl_utils.py AT %s' %
        (startTime,))

    # set options interactively if user requests
    if "interactiveFlag" in a and a["interactiveFlag"]:

        # load previous settings
        moduleDirectory = os.path.dirname(__file__) + "/resources"
        pathToPickleFile = "%(moduleDirectory)s/previousSettings.p" % locals()
        try:
            with open(pathToPickleFile):
                pass
            previousSettingsExist = True
        except:
            previousSettingsExist = False
        previousSettings = {}
        if previousSettingsExist:
            previousSettings = pickle.load(open(pathToPickleFile, "rb"))

        # x-raw-input
        # x-boolean-raw-input
        # x-raw-input-with-default-value-from-previous-settings

        # save the most recently used requests
        pickleMeObjects = []
        pickleMe = {}
        theseLocals = locals()
        for k in pickleMeObjects:
            pickleMe[k] = theseLocals[k]
        pickle.dump(pickleMe, open(pathToPickleFile, "wb"))

    if a["init"]:
        home = expanduser("~")
        filepath = home + "/.config/ep3/ep3.yaml"
        try:
            cmd = """open %(filepath)s""" % locals()
            p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        except:
            pass
        try:
            cmd = """start %(filepath)s""" % locals()
            p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        except:
            pass
        return

    if a["import"]:
        from ep3 import importer
        remainingFrameCount = 1
        while remainingFrameCount > 0:
            ingester = importer(
                log=log,
                settings=settings
            )
            remainingFrameCount = ingester.ingest()
            print(f'{remainingFrameCount} frames remain to be imported into the archive from the dropbox folder')

    if a["clean"]:

        from ep3 import clean
        cleaner = clean(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        cleaner.clean()

    if a["match"]:
        if not a["transientId"] or not a["fitsObject"]:
            a["transientId"] = False
            a["fitsObject"] = False
        from ep3 import crossmatcher
        matcher = crossmatcher(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        matcher.match(
            transientId=a["transientId"],
            fitsObject=a["fitsObject"]
        )

    if a["rewrite"]:
        from ep3 import fits_writer
        writer = fits_writer(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        newFramePaths, newTablePaths = writer.write()

        print("Here are all of the fits frames that have been refreshed:")
        for i in newFramePaths:
            print(i)

        print("\n\n\nHere are all of the fits binary tables that have been rewritten:")
        for i in newTablePaths:
            print(i)

    if a["export"]:
        if a["instrument"] == "all":
            a["instrument"] = False
        if a["fileType"] == "all":
            a["fileType"] = False

        home = expanduser("~")
        a["exportPath"] = a["exportPath"].replace("~", home)

        from ep3 import export_ssdr
        exporter = export_ssdr(
            log=log,
            dbConn=dbConn,
            settings=settings,
            exportPath=a["exportPath"],
            ssdr=a["ssdr"],
            instrument=a["instrument"],
            fileType=a["fileType"]
        )
        exporter.export()

    if a["esolist"] and a["table"]:

        home = expanduser("~")
        a["pathToCsv"] = a["pathToCsv"].replace("~", home)

        from ep3 import ssdr_snapshot
        ssdr = ssdr_snapshot(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        ssdr.add_database_table(
            ssdr_ver=a["ssdr"],
            pathToCsvListing=a["pathToCsv"])

    if a["esolist"] and a["rename"]:

        home = expanduser("~")
        a["pathToDownloads"] = a["pathToDownloads"].replace("~", home)

        from ep3 import ssdr_snapshot
        ssdr = ssdr_snapshot(
            log=log,
            dbConn=False,
            settings=settings
        )
        count = ssdr.ssdr_file_reset(
            pathToDownloadDir=a["pathToDownloads"])
        print(f"Successfully renamed {count} files.")

    if a["transcat"]:

        from ep3 import transient_catalogue
        cat = transient_catalogue(
            log=log,
            dbConn=dbConn,
            outputDirectory=a["pathToOutputDir"],
            settings=settings
        )
        fitsPath = cat.create()
        print(f"Here's the exported catalogue: {fitsPath}")

    if a["reports"]:
        from ep3.reports import object_spectra_breakdowns
        object_spectra_breakdowns(
            log=log,
            settings=settings,
            dbConn=dbConn
        )

    # CALL FUNCTIONS/OBJECTS

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = times.get_now_sql_datetime()
    runningTime = times.calculate_time_difference(startTime, endTime)
    log.info('-- FINISHED ATTEMPT TO RUN THE cl_utils.py AT %s (RUNTIME: %s) --' %
             (endTime, runningTime, ))

    return

if __name__ == '__main__':
    main()
