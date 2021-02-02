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

try:
    shutil.rmtree(pathToOutputDir)
except:
    pass
# COPY INPUT TO OUTPUT DIR
shutil.copytree(pathToInputDir, pathToOutputDir)

# Recursively create missing directories
if not os.path.exists(pathToOutputDir):
    os.makedirs(pathToOutputDir)


# xt-setup-unit-testing-files-and-folders
utKit("").refresh_database()


class test_importer(unittest.TestCase):

    def test_01_filter_directory_of_fits_frame_function(self):

        from ep3 import importer
        ingester = importer(
            log=log,
            settings=settings
        )
        ingester.ingest()

    def test_02_select_files_to_archive_function(self):

        from ep3 import importer
        ingester = importer(
            log=log,
            settings=settings
        )
        for table in ingester.tables:
            primaryIds, filePaths, archivePaths = ingester.select_files_to_archive(
                table)
            for a in archivePaths:
                print(a)

    def test_03_clean_up_function(self):

        from fundamentals.mysql import writequery
        procedures = ["ep3_update_currentfilenames()",
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

    def test_importer_function_exception(self):

        from ep3 import importer
        try:
            this = importer(
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
