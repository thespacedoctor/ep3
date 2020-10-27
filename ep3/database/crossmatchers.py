#!/usr/local/bin/python
# encoding: utf-8
"""
*Functions/Classes to crossmatch against the PESSTO Marshall database tables.*

:Author:
    David Young
"""

import sys
import os

def conesearch_marshall_transientBucket_objects(
        dbConn,
        log,
        ra,
        dec,
        radiusArcSec,
        nearest=True):
    """
    *conesearch against marshall db*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    - ``ra`` -- the RA to search against
    - ``dec`` -- the DEC to search against
    - ``radiusArcSec`` -- radius in arc-sec
    - ``nearest`` -- report only the nearest object
    

    **Return**

    - ``match`` --
    
    """
    log.debug(
        'completed the ````conesearch_marshall_transientBucket_objects`` function')

    from HMpTy.mysql import conesearch
    cs = conesearch(
        log=log,
        dbConn=dbConn,
        tableName="view_transientBucketMaster",
        columns="transientBucketId, name",
        ra=ra,
        dec=dec,
        radiusArcsec=radiusArcSec,
        closest=nearest
    )
    matchIndies, matches = cs.search()

    matches = matches.list

    transientBucketId, ra, dec, objectName = (None, None, None, None)
    transientBucketIdList, raList, decList, objectNameList = (
        [], [], [], [],)

    for m in matches:
        transientBucketIdList.append(m["transientBucketId"])
        raList.append(m["raDeg"])
        decList.append(m["decDeg"])
        objectNameList.append(m["name"])

    if nearest and len(matches):
        transientBucketId, ra, dec, objectName = (transientBucketIdList[0], raList[
                                                  0], decList[0], objectNameList[0])

    log.debug(
        'completed the ``conesearch_marshall_transientBucket_objects`` function')

    if nearest:
        return transientBucketId, ra, dec, objectName
    else:
        return transientBucketIdList, raList, decList, objectNameList
