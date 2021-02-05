#!/usr/bin/env python
# encoding: utf-8
"""
Documentation for ep3 can be found here: http://ep3.readthedocs.org

Usage:
    ep3 init
    ep3 import [-s <pathToSettingsFile>]  
    ep3 clean [-s <pathToSettingsFile>]  
    ep3 match [<transientId> <fitsObject>] [-s <pathToSettingsFile>]  

Options:
    init                                   setup the ep3 settings file for the first time
    import                                 import the NTT data into the database (headers) and the archive file-system
    clean                                  run the MySQL stored procedures to clean up FITS keyword values in database space
    match                                  match frames against transient names and coordinates, then update coordinates and object names in FITS frames and rename files accordingly
    <transientId>                          the transient ID of the source you want to manually force a match with frames with object name <fitsObject>
    <fitsObject>                           the object name in the FITS frames you want to manually force a match with frames whose coordinates are close to <transientId>


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
        from os.path import expanduser
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
        procedures = [
            "ep3_clean_transientBucketSummaries()",
            "ep3_basic_keyword_value_corrections()",
            "ep3_force_match_object_to_frame()",
            "ep3_set_file_associations()",
            "ep3_flag_frames_for_release()",
            "ep3_set_data_rel_versions()",
            "ep3_flag_transient_frames_where_transient_not_in_frame()",
            "ep3_set_zeropoint_in_efosc_images()",
            "ep3_set_maglim_magat_in_images()",
            "ep3_binary_table_keyword_updates()",
            "ep3_create_spectrum_binary_table_rows()"]

        for p in procedures:
            sqlQuery = f"""CALL {p};"""
            print(f"Running the `{p}` procedure")
            writequery(
                log=log,
                sqlQuery=sqlQuery,
                dbConn=dbConn
            )

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
