import os
import nose
import shutil
from .. import classification_breakdown_csv_export
from pessto_marshall_engine import utKit

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log, dbConn, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()

# xnose-class-to-test-main-command-line-function-of-module


class test_classification_breakdown_csv_export(unittest.TestCase):

    def test_classification_breakdown_csv_export_function(self):
        kwargs = {}
        kwargs["dbConn"] = dbConn
        kwargs["log"] = log
        kwargs[
            "csvExportDirectory"] = "/Users/Dave/Dropbox/github_projects/pessto_webapp/htdocs/assets/csv"
        # xt-kwarg_key_and_value

        classification_breakdown_csv_export.classification_breakdown_csv_export(
            **kwargs)

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
