import os
from nose import with_setup

## SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
def setUpModule():
    "set up test fixtures"
    from .. import mysql
    from dryxPython import mysql as dms

    moduleDirectory = os.path.dirname(__file__) + "/../../tests"

    # SETUP PATHS TO COMMONG DIRECTORIES FOR TEST DATA
    global pathToInputDataDir, pathToOutputDir, pathToOutputDataDir, pathToInputDir
    pathToInputDir = moduleDirectory+"/input/"
    pathToInputDataDir = pathToInputDir + "data/"
    pathToOutputDir = moduleDirectory+"/output/"
    pathToOutputDataDir = pathToOutputDir+"data/"

    # SETUP THE TEST LOG FILE
    global testlog
    testlog = open(pathToOutputDir + "tests.log", 'w')

    # SETUP THE MOCK DBCONN
    global dbConn
    dbSettings = pathToInputDir + "database_credentials.yaml"
    print dbSettings
    dbConn = dms.set_db_connection(
        dbSettings
    )

    return None

def tearDownModule():
    "tear down test fixtures"
    # CLOSE THE TEST LOG FILE
    testlog.close()
    return None


## SETUP AND TEARDOWN FIXTURE FUNCTIONS
def setUpFunc():
    "set up the test fixtures"

    # mysql.get_object_peak_mag(dbConn, log, transientBucketId)
    return None

def tearDownFunc():
    "tear down the test fixtures"

    return None

## TEST CLASS METHODS
@with_setup(setUpModule, tearDownFunc)
def test_get_object_peak_mag():

    # pm.get_object_peak_mag(
    #     dbConn=dbConn,
    #     log=log,
    #     transientBucketId=
    # )
    # UNEDITED #
    # Get the peak apparent magnitude for a specific object #
    # get_object_peak_mag #
    # pm:var:get_peak_apparent_magnitude_for_specific_object.sublime-snippet #

    print dbConn


    assert 1 == 1
