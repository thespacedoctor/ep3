#!/usr/bin/env python
# encoding: utf-8
"""
**

:Author:
    David Young

:Date Created:
    July 22, 2014

Usage:
    pm_ntt_data_coordinate_crossmatch_checks -f <filetype> -s <pathToSettingsFile>

Options:
    -h, --help          show this help message
    -f, --filetype      imaging | spectra | all
    -s, --settingsFile  path to the settings file
"""
################# GLOBAL IMPORTS ####################
import sys
import os
import math
from operator import itemgetter
from docopt import docopt
import numpy as np
from dryxPython import mysql as dms
from dryxPython import logs as dl
from dryxPython import astrotools as dat
from dryxPython import commonutils as dcu
from fundamentals import tools
import pessto_marshall_engine as pme
# from ..__init__ import *


def main(arguments=None):
    """
    *The main function used when ``ntt_data_coordinate_crossmatch_checks`` is run as a single script from the cl, or when installed as a cl command*
    """
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
        '--- STARTING TO RUN THE ntt_data_coordinate_crossmatch_checks AT %s' %
        (startTime,))

    # call the worker function
    # x-if-settings-or-database-credientials
    ntt_data_coordinate_crossmatch_checks(
        log=log,
        dbConn=dbConn,
        setup=filetype
    )

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info('-- FINISHED ATTEMPT TO RUN THE ntt_data_coordinate_crossmatch_checks AT %s (RUNTIME: %s) --' %
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
# LAST MODIFIED : July 22, 2014
# CREATED : July 22, 2014
# AUTHOR : DRYX
def ntt_data_coordinate_crossmatch_checks(
        dbConn,
        log,
        setup="imaging",
        dataRel=1):
    """
    *object image vs image crossmatch checks*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger

    **Return:**
        - None

    .. todo::

        - @review: when complete, clean ntt_data_coordinate_crossmatch_checks function
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract function to another module
    """
    log.debug(
        'completed the ````ntt_data_coordinate_crossmatch_checks`` function')

    # setup variables
    degreesToArcsecRatio = 60. * 60.

    if setup == "spectra" or setup == "all":
        thisSetup = "spectra"
        rmsLimit = 2.  # arcsec
    elif setup == "imaging":
        thisSetup = setup
        rmsLimit = 12.  # arcsec

    # grab the distinct objects for setup
    get_unique_objects_in_pessto_ntt_data_files = pme.commonutils.get_unique_objects_in_pessto_ntt_data_files.get_unique_objects_in_pessto_ntt_data_files
    objectList = get_unique_objects_in_pessto_ntt_data_files(
        dbConn=dbConn,
        log=log,
        instrument="all",
        setup=thisSetup,
        dataRel=dataRel
    )
    totalCount = len(objectList)
    print "There are %(totalCount)s unique objects observed in the efosc & sofi %(thisSetup)s" % locals()

    if "imag" in setup:
        fileType = "image"
        fileTypes = "images"
    elif "spec" in setup:
        fileType = "spectrum"
        fileTypes = "spectra"

    # calculation the coordinates info for each object
    objectAttributes = calculate_coordinate_info_for_objects_in_list(
        dbConn,
        log,
        objectList=objectList,
        setup=thisSetup,
        dataRel=dataRel
    )

    masterSusFilesList = []
    if setup == "all":
        # grab the distinct all Imaging objects
        get_unique_objects_in_pessto_ntt_data_files = pme.commonutils.get_unique_objects_in_pessto_ntt_data_files.get_unique_objects_in_pessto_ntt_data_files
        allImagingObjectList = get_unique_objects_in_pessto_ntt_data_files(
            dbConn=dbConn,
            log=log,
            instrument="all",  # efosc | sofi | all
            setup="Imaging",  # Imaging | spectra | all
            dataRel=1
        )
        allImagingObjectCount = len(allImagingObjectList)
        print "There are %(allImagingObjectCount)s unique objects observed in the all Imaging data" % locals()

        # calculation the coordinates info for each imaging object
        objectImageAttributes = calculate_coordinate_info_for_objects_in_list(
            dbConn,
            log,
            objectList=allImagingObjectList,
            setup="imaging",
            dataRel=dataRel
        )

        noImagingCounterpartList = []
        print "| # | Spectral Object | RA~spec | DEC~spec | std (arcsec) | # images | rogue images |" % locals()
        print "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |" % locals()

        # match each imaging object against spectral objects
        objectNumber = 0

        for thisObject in objectImageAttributes:

            objectName = thisObject['object']
            transientBucketId = thisObject['transientBucketId']
            log.debug(
                "imaging tbid: %(transientBucketId)s, name %(objectName)s" % locals())

            specTransBucketIds = []
            specTransBucketIds[:] = [l[1] for l in objectList]
            if transientBucketId not in specTransBucketIds:
                thisObject['spectralMatch'] = False
                noImagingCounterpartList.append(objectName)
                log.debug(
                    "no spectral counterpart for tbid %(transientBucketId)s" % locals())
                continue
            else:
                log.debug(
                    "found a match for tbid %(transientBucketId)s" % locals())
                thisObject['spectralMatch'] = True
                objectNumber += 1

            # grab matched spectral-object mean coordinates
            sRa = None
            sDec = None

            iterCount = 0
            for spectralObject in objectAttributes[:]:
                iterCount += 1
                if spectralObject["transientBucketId"] == transientBucketId:
                    spectraltransientBucketId = spectralObject[
                        "transientBucketId"]
                    log.debug(
                        'spectral tbid: %(spectraltransientBucketId)s' % locals())
                    sRa = spectralObject["meanRa"]
                    sDec = spectralObject["meanDec"]
                    break
            log.debug(
                'iterated over %(iterCount)s objects in objectAttributes' % locals())

            if not sDec:
                log.error(
                    'cound not find sDec for object %(objectName)s (%(transientBucketId)s)' % locals())
                continue

            totalFiles = len(thisObject["filePathList"])
            susFiles = {}
            count = 0
            separationFromSpecMean = []
            areImagesSeparationsOk = True
            for r, d, f, n in zip(thisObject["raArray"], thisObject["decArray"], thisObject["filePathList"], thisObject["fileNameList"]):
                count += 1
                # determine angular distance from spectral coordinates
                angularSeparation = dat.getAngularSeparation(
                    ra1=r,
                    dec1=d,
                    ra2=sRa,
                    dec2=sDec
                )
                if angularSeparation > 60:
                    areImagesSeparationsOk = False
                log.debug("%(f)s sep: %(angularSeparation)s" % locals())
                separationFromSpecMean.append(angularSeparation)
            thisObject["separationFromSpecArray"] = np.array(
                separationFromSpecMean)
            thisStd = np.std(thisObject["separationFromSpecArray"])

            if not areImagesSeparationsOk:
                for s, f, n in zip(thisObject["separationFromSpecArray"], thisObject["filePathList"], thisObject["fileNameList"]):
                    if (s > 60):
                        f = f.replace("/misc/pessto/data/ntt_data/archive/",
                                      "http://www.pessto.org/private/data/ntt/")
                        susFiles["s%(s)07.2f%(count)s" % locals()] = [
                            "[%(n)s](%(f)s)" % locals(), s]

            import collections
            osusFiles = collections.OrderedDict(
                reversed(sorted(susFiles.items())))

            susFiles = ""
            for k, v in osusFiles.iteritems():
                link = v[0]
                sep = v[1]
                masterSusFilesList.append([link, sep])
                susFiles = """%(susFiles)s %(link)s&nbsp;\\\\((%(sep)0.2f'')\\\\) """ % locals(
                )

            if not areImagesSeparationsOk:
                print """| %(objectNumber)s | %(objectName)s | %(sRa)0.5f| %(sDec)0.5f | %(thisStd)0.2f | %(totalFiles)s | %(susFiles)s |""" % locals()

        # print the image list again as a seperate table
        print "\n\n| imaging link | angle from mean | notes |" % locals()
        print "| :---- | :---- | :---- |" % locals()
        for item in masterSusFilesList:
            sep = item[1]
            link = item[0]
            print """| %(link)s | %(sep)0.2f |   |""" % locals()

        print "These spectral objects have no imaging counterpart: %(noImagingCounterpartList)s " % locals()

    else:
        # print objects with too high an STD in either RA or DEC
        print "| Object | RA~mar | DEC~mar | std~rms (arcsec) | # %(fileTypes)s | rogue %(fileTypes)s |" % locals()
        print "| :--- | :--- | :--- | :--- | :--- | :--- |" % locals()
        for thisObject in objectAttributes:
            if (thisObject['stdRms'] * degreesToArcsecRatio) > rmsLimit:
                thisRaStd = thisObject['stdRa'] * degreesToArcsecRatio
                thisDecStd = thisObject['stdDec'] * degreesToArcsecRatio
                thisStdRms = thisObject['stdRms'] * degreesToArcsecRatio
                objectName = thisObject['object']

                # grab marshall ra and dec
                sqlQuery = """
                    select raDeg, decDeg from pessto_marshall_object_summaries where name = "%(objectName)s" order by raDeg limit 1
                """ % locals()
                marshallRows = dms.execute_mysql_read_query(
                    sqlQuery=sqlQuery,
                    dbConn=dbConn,
                    log=log
                )
                if len(marshallRows):
                    mRa = marshallRows[0]["raDeg"]
                    mDec = marshallRows[0]["decDeg"]
                else:
                    mRa = "not in marshall"
                    mDec = "not in marshall"

                totalFiles = len(thisObject["filePathList"])

                susFiles = {}
                count = 0
                for r, d, f, n in zip(thisObject["raArray"], thisObject["decArray"], thisObject["filePathList"], thisObject["fileNameList"]):
                    count += 1
                    angularSeparation = dat.getAngularSeparation(
                        ra1=r,
                        dec1=d,
                        ra2=thisObject["meanRa"],
                        dec2=thisObject["meanDec"]
                    )
                    if (angularSeparation > 1.5 * thisStdRms) or totalFiles == 2:
                        f = f.replace("/misc/pessto/data/ntt_data/archive/",
                                      "http://www.pessto.org/private/data/ntt/")
                        susFiles["s%(angularSeparation)07.2f%(count)s" % locals()] = [
                            "[%(n)s](%(f)s)" % locals(), angularSeparation]

                import collections
                osusFiles = collections.OrderedDict(
                    reversed(sorted(susFiles.items())))

                susFiles = ""
                for k, v in osusFiles.iteritems():
                    link = v[0]
                    sep = v[1]
                    masterSusFilesList.append([link, sep])
                    susFiles = """%(susFiles)s %(link)s&nbsp;\\\\((%(sep)0.2f'')\\\\) """ % locals(
                    )

                if isinstance(mRa, str):
                    print """| %(objectName)s | %(mRa)s | %(mDec)s | %(thisStdRms)0.2f | %(totalFiles)s | %(susFiles)s |""" % locals()
                else:
                    print """| %(objectName)s | %(mRa)0.5f| %(mDec)0.5f | %(thisStdRms)0.2f | %(totalFiles)s | %(susFiles)s |""" % locals()

        # print the image list again as a seperate table
        print "\n\n| %(fileType)s link | angle from mean | notes |" % locals()
        print "| :---- | :---- | :---- |" % locals()
        for item in masterSusFilesList:
            sep = item[1]
            link = item[0]
            print """| %(link)s | %(sep)0.2f |   |""" % locals()

    log.debug(
        'completed the ``ntt_data_coordinate_crossmatch_checks`` function')
    return None

# use the tab-trigger below for new function
# LAST MODIFIED : July 24, 2014
# CREATED : July 24, 2014
# AUTHOR : DRYX
# copy usage method(s) into function below and select the following snippet from the command palette:
# x-setup-worker-function-parameters-from-usage-method


def calculate_coordinate_info_for_objects_in_list(
        dbConn,
        log,
        objectList,
        setup,
        dataRel):
    """
    *calculate mean coordinated from objects in list*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        # copy usage method(s) here and select the following snippet from the command palette:
        # x-setup-docstring-keys-from-selected-usage-options

    **Return:**
        - None

    .. todo::

        - @review: when complete, clean calculate_mean_coordinated_from_objects_in_list function
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract function to another module
    """
    log.debug(
        'completed the ````calculate_mean_coordinated_from_objects_in_list`` function')

    if dataRel:
        dataRel = """and DATA_REL = "ssdr%(dataRel)s" """ % locals()
    else:
        dataRel = ""
    noWeights = """and currentFilename not like "%%weight%%" """

    objectAttributes = []
    for anObject in objectList:
        thisObject = anObject[0]
        transientBucketId = anObject[1]
        sqlQuery = """
            select currentFilename, currentFilepath, ra, decl from efosc_%(setup)s where object = "%(thisObject)s"  %(dataRel)s %(noWeights)s 
            union all
            select currentFilename, currentFilepath, ra, decl from sofi_%(setup)s where object = "%(thisObject)s" %(dataRel)s %(noWeights)s 
        """ % locals()
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=dbConn,
            log=log
        )
        raArray = []
        decArray = []
        filePathList = []
        filenameList = []
        for row in rows:
            raArray.append(row["ra"])
            decArray.append(row["decl"])
            filePathList.append(row["currentFilepath"])
            filenameList.append(row["currentFilename"])
        tinyDict = {}
        tinyDict["object"] = thisObject
        tinyDict["transientBucketId"] = transientBucketId
        tinyDict["filePathList"] = filePathList
        tinyDict["fileNameList"] = filenameList
        tinyDict["raArray"] = np.array(raArray)
        tinyDict["decArray"] = np.array(decArray)
        tinyDict["meanRa"] = np.mean(tinyDict["raArray"])
        tinyDict["meanDec"] = np.mean(tinyDict["decArray"])
        tinyDict["stdRa"] = np.std(
            tinyDict["raArray"]) * np.cos(tinyDict["meanDec"] * math.pi / 180.)
        tinyDict["stdDec"] = np.std(tinyDict["decArray"])
        tinyDict["stdRms"] = np.sqrt(
            (tinyDict["stdRa"] ** 2 + tinyDict["stdDec"] ** 2) / 2)
        objectAttributes.append(tinyDict)

    objectAttributes = list(objectAttributes)
    objectAttributes = sorted(objectAttributes, key=itemgetter('stdRms'))
    objectAttributes = reversed(objectAttributes)
    objectAttributes = list(objectAttributes)

    log.debug(
        'completed the ``calculate_mean_coordinated_from_objects_in_list`` function')
    return objectAttributes

# use the tab-trigger below for new function
# xt-def-with-logger

###################################################################
# PRIVATE (HELPER) FUNCTIONS                                      #
###################################################################


############################################
# CODE TO BE DEPECIATED                    #
############################################

if __name__ == '__main__':
    main()

###################################################################
# TEMPLATE FUNCTIONS                                              #
###################################################################
