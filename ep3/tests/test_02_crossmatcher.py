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

settings["dropbox"] = f"{pathToOutputDir}/dropbox"
settings["archive-root"] = f"{pathToOutputDir}/archive"
# try:
#     shutil.rmtree(pathToOutputDir)
# except:
#     pass
# # COPY INPUT TO OUTPUT DIR
# shutil.copytree(pathToInputDir, pathToOutputDir)

# Recursively create missing directories
if not os.path.exists(pathToOutputDir):
    os.makedirs(pathToOutputDir)


class test_crossmatcher(unittest.TestCase):

    def test_crossmatcher_function(self):

        from ep3 import crossmatcher
        matcher = crossmatcher(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        matcher.match()

        matcher.match(transientId=867435, fitsObject="ASASSN15oi")
        matcher.match(transientId=2768144, fitsObject="AT2016ad")
        matcher.match(transientId=2784332, fitsObject="AT2016bc")
        matcher.match(transientId=7539752, fitsObject="SN2017ens")
        matcher.match(transientId=10171766, fitsObject="name")
        matcher.match(transientId=19928753, fitsObject="BRUTUS8204_3")
        matcher.match(transientId=2570994, fitsObject="SN2016jbu")

    def test_crossmatcher_function_exception(self):

        from ep3 import crossmatcher
        try:
            this = crossmatcher(
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
