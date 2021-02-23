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

# Recursively create missing directories
if not os.path.exists(pathToOutputDir):
    os.makedirs(pathToOutputDir)


# xt-setup-unit-testing-files-and-folders
# xt-utkit-refresh-database

class test_clean(unittest.TestCase):

    def test_clean_function(self):

        from ep3 import clean
        cleaner = clean(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        cleaner.clean()

    def test_clean_astrom_function(self):

        from ep3 import clean
        cleaner = clean(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        cleaner.fix_image_astrometry_keywords()

    def test_clean_sofi_mjd_function(self):

        from ep3 import clean
        cleaner = clean(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        cleaner.fix_sofi_mjd_keywords()

    def test_add_sofi_imaging_median_weight_values_function(self):

        from ep3 import clean
        cleaner = clean(
            log=log,
            dbConn=dbConn,
            settings=settings
        )
        cleaner.add_sofi_imaging_median_weight_values()

    def test_clean_function_exception(self):

        from ep3 import clean
        try:
            cleaner = clean(
                log=log,
                dbConn=dbConn,
                settings=settings,
                fakeKey="break the code"
            )
            cleaner.get()
            assert False
        except Exception as e:
            assert True
            print(str(e))

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
