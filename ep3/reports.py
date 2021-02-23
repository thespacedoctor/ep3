#!/usr/bin/env python
# encoding: utf-8
"""
*Write some reports useful for the release description etc*

:Author:
    David Young

:Date Created:
    February 15, 2021
"""
from builtins import object
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from fundamentals.mysql import readquery
from fundamentals.mysql import writequery
import collections
from fundamentals.renderer import list_of_dictionaries


def object_spectra_breakdowns(
        log,
        settings,
        dbConn,
        data_rel=False):
    """object spectra breakdowns

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``data_rel`` -- filter by data release version. Default **False**

    **Return:**
        - None

    ```eval_rst
    .. todo::

        - add a tutorial about ``subtract_calibrations`` to documentation
    ```

    ```python
    from ep3.reports import object_spectra_breakdowns
    object_spectra_breakdowns(
        log=log,
        settings=settings,
        dbConn=dbConn
    )
    ```
    """
    log.debug('starting the ``object_spectra_breakdowns`` function')

    # CLEAN UP TRANSIENT-PAPER ASSOCIATIONS
    sqlQuery = f"""call ep3_match_papers_to_transientId()"""
    writequery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn
    )
    if data_rel:
        data_rel = f'where data_rel = "SSDR{data_rel}" '
    else:
        data_rel = f''

    # GRAB THE EFOSC SPECTRUM BREAKDOWNS
    sqlQuery = f"""
        SELECT transientBucketId, object, grism, count(*) as 'count' FROM view_ssdr_efosc_spectra_binary_tables {data_rel} group by object, grism;
    """
    rows = readquery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn
    )
    objects = {}
    tids = []

    # SORT THE DATA INTO INDIVIDUAL OBJECTS
    for r in rows:
        if r["OBJECT"] not in objects:
            objects[r["OBJECT"]] = {
                "spectra": "",
                "count": 0,
                "transientBucketId": 0
            }
        objects[r["OBJECT"]]["count"] += r["count"]
        objects[r["OBJECT"]]["spectra"] += f"{r['count']}x{r['grism']}, "
        objects[r["OBJECT"]]["transientBucketId"] = r["transientBucketId"]
        tids.append(r["transientBucketId"])

    # NOW GRAB SOFI SPECTRUM BREAKDOWNS
    sqlQuery = f"""
        SELECT transientBucketId, object, grism, count(*) as 'count' FROM view_ssdr_sofi_spectra_binary_tables {data_rel} group by object, grism;
    """
    rows = readquery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn
    )

    # SORT THE DATA INTO INDIVIDUAL OBJECTS
    for r in rows:
        if r["OBJECT"] not in objects:
            objects[r["OBJECT"]] = {
                "spectra": "",
                "count": 0,
                "transientBucketId": 0
            }
        objects[r["OBJECT"]]["count"] += r["count"]
        objects[r["OBJECT"]]["spectra"] += f"{r['count']}x{r['grism']}, "
        objects[r["OBJECT"]]["transientBucketId"] = r["transientBucketId"]
        tids.append(r["transientBucketId"])

    # SORT ALL ALPHABETICALLY
    objects = collections.OrderedDict(sorted(objects.items()))
    tids = list(set(tids))
    tids = (",").join([str(t) for t in tids])

    # NOW GET SPECTRAL TYPES FROM PESSTO_TRAN_CAT
    sqlQuery = f"""
        SELECT transientBucketId, if(TRANSIENT_CLASSIFICATION_PECULIAR_FLAG,concat(TRANSIENT_CLASSIFICATION,"-p"),TRANSIENT_CLASSIFICATION) as TRANSIENT_CLASSIFICATION from PESSTO_TRAN_CAT
    """
    rows = readquery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn
    )
    specTypes = {r["transientBucketId"]: r[
        "TRANSIENT_CLASSIFICATION"] for r in rows}
    sqlQuery = f"""
        SELECT transientBucketId, recentClassification as TRANSIENT_CLASSIFICATION from transientBucketSummaries where transientBucketId in ({tids})
    """
    rows = readquery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn
    )
    altSpecType = {r["transientBucketId"]: r[
        "TRANSIENT_CLASSIFICATION"] for r in rows}
    for k, v in altSpecType.items():
        if v and v[0] == "I":
            altSpecType[k] = f"SN {v}"
        if v and "vari" in v.lower():
            altSpecType[k] = f"Variable Star"

    # NOW GET PAPERS
    sqlQuery = f"""
        SELECT transientBucketId, reference from transient_object_papers
    """
    rows = readquery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn
    )
    # SORT THE PAPERS INTO INDIVIDUAL OBJECTS
    papers = {}
    for r in rows:
        if r["transientBucketId"] not in papers:
            papers[r["transientBucketId"]] = ""
        papers[r["transientBucketId"]] += f"{r['reference']}, "

    # DETERMINE WHICH OBJECTS ARE FOLLOWUP TARGETS
    followupIds = []
    csvEntries = []
    for k, v in objects.items():
        v["spectra"] = v["spectra"].replace("#", "")[:-2]
        if v["count"] < 2:
            followup = 0
        elif v["spectra"] in ("1xGr13", "2xGr13", "3xGr13", "1xGr11", "2xGr11", "2xGr11, 1xGr13", "1xGr11, 1xGr13", "1xGr11, 2xGr13"):
            followup = 0
        else:
            followup = 1
            followupIds.append(str(v["transientBucketId"]))
            if v["transientBucketId"] in specTypes:
                stype = specTypes[v["transientBucketId"]]
            else:
                stype = altSpecType[v["transientBucketId"]]
            ref = ""
            if v["transientBucketId"] in papers:
                ref = papers[v["transientBucketId"]][:-2]
            csvEntries.append({
                "Target": k,
                "Type": stype,
                "Number of Spectra": v["spectra"],
                "Comments": ref
            })

    snCount = 0
    for c in csvEntries:
        if c["Type"][:2] == "SN ":
            snCount += 1

    dataSet = list_of_dictionaries(
        log=log,
        listOfDictionaries=csvEntries,
    )
    csvData = dataSet.csv(filepath="/tmp/table3.csv")
    tableData = dataSet.table(filepath=None)

    print(tableData)

    print("CSV version of this table can be found at '/tmp/table3.csv'. Import it into an excel worksheet and add to the release description")

    print(followupIds)
    log.debug('completed the ``object_spectra_breakdowns`` function')
    return None


def data_release_stats(
        dbConn,
        log):
    """print out some other data release stats

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger

    **Return:**
        - None

    ```eval_rst
    .. todo::

        - add usage info
        - create a sublime snippet for usage
        - add a tutorial about ``subtract_calibrations`` to documentation
    ```

    ```python
    usage code 
    ```
    """
    log.debug('starting the ``data_release_stats`` function')

    sqlQuery = f"""
        select count(*) as objects from (
select distinct transientBucketId from view_ssdr_efosc_spectra_binary_tables
union
select distinct transientBucketId from view_ssdr_sofi_spectra_binary_tables) as a;
    """
    rows = readquery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn
    )
    count = rows[0]["objects"]
    print(f"PESSTO has taken spectra of {count} distinct objects")

    log.debug('completed the ``data_release_stats`` function')
    return None
