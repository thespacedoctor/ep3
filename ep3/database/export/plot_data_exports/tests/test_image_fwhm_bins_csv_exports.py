import os
import nose
import shutil
from .. import image_fwhm_bins_csv_exports
from pessto_marshall_engine import utKit

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log, dbConn, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()

# xnose-class-to-test-main-command-line-function-of-module


class test_image_fwhm_bins_csv_exports(unittest.TestCase):

    def test_image_fwhm_bins_csv_exports_function(self):
        kwargs = {}
        kwargs["dbConn"] = dbConn
        kwargs["log"] = log
        kwargs[
            "csvExportDirectory"] = "/Users/Dave/Dropbox/github_projects/pessto_webapp/htdocs/assets/csv"
        # xt-kwarg_key_and_value

        image_fwhm_bins_csv_exports.image_fwhm_bins_csv_exports(**kwargs)

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
