from __future__ import print_function
from builtins import str
import os
import unittest
import shutil
import unittest
import yaml
from ep3.utKit import utKit
from fundamentals import tools
from os.path import expanduser
home = expanduser("~")

packageDirectory = utKit("").get_project_root()
settingsFile = packageDirectory + "/test_settings.yaml"
# settingsFile = home + \
#     "/git_repos/_misc_/settings/ep3/test_settings.yaml"

su = tools(
    arguments={"settingsFile": settingsFile},
    docString=__doc__,
    logLevel="DEBUG",
    options_first=False,
    projectName=None,
    defaultSettingsFile=False
)
arguments, settings, log, dbConn = su.setup()

# SETUP PATHS TO COMMON DIRECTORIES FOR TEST DATA
moduleDirectory = os.path.dirname(__file__)
pathToInputDir = moduleDirectory + "/input/"
pathToOutputDir = moduleDirectory + "/output/"

try:
    shutil.rmtree(pathToOutputDir + "/esodownloads")
except:
    pass
# COPY INPUT TO OUTPUT DIR
shutil.copytree(pathToInputDir + "/esodownloads",
                pathToOutputDir + "/esodownloads")

# Recursively create missing directories
if not os.path.exists(pathToOutputDir):
    os.makedirs(pathToOutputDir)


# xt-setup-unit-testing-files-and-folders
# xt-utkit-refresh-database

class test_ssdr_snapshot(unittest.TestCase):

    def test_ssdr_snapshot_function(self):

        from ep3 import ssdr_snapshot
        ssdr = ssdr_snapshot(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        ssdr.add_database_table(
            ssdr_ver=3.1,
            pathToCsvListing=pathToInputDir + "/wdb_query_7563_eso.csv")

    def test_ssdr_renamer_function(self):

        from ep3 import ssdr_snapshot
        ssdr = ssdr_snapshot(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        count = ssdr.ssdr_file_reset(
            pathToDownloadDir=pathToOutputDir + "/esodownloads")
        print(f"Successfully renamed {count} files.")

    def test_ssdr_snapshot_function_exception(self):

        from ep3 import ssdr_snapshot
        try:
            this = ssdr_snapshot(
                log=log,
                settings=settings,
                fakeKey="break the code"
            )
            this.get()
            assert False
        except Exception as e:
            assert True
            print(str(e))

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
