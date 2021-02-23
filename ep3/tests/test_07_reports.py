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

# try:
#     shutil.rmtree(pathToOutputDir)
# except:
#     pass
# # COPY INPUT TO OUTPUT DIR
# shutil.copytree(pathToInputDir, pathToOutputDir)

# # Recursively create missing directories
# if not os.path.exists(pathToOutputDir):
#     os.makedirs(pathToOutputDir)


# xt-setup-unit-testing-files-and-folders
# xt-utkit-refresh-database


class test_reports(unittest.TestCase):

    def test_reports_function(self):

        from ep3.reports import object_spectra_breakdowns
        object_spectra_breakdowns(
            log=log,
            settings=settings,
            dbConn=dbConn
        )

    def test_report_stats_function(self):
        from ep3.reports import data_release_stats
        data_release_stats(
            log=log,
            dbConn=dbConn
        )

    def test_reports_function_exception(self):

        from ep3 import reports
        try:
            this = reports(
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
