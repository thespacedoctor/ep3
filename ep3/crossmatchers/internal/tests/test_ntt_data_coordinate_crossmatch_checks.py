import os
import nose
import shutil
from .. import ntt_data_coordinate_crossmatch_checks
from pessto_marshall_engine import utKit

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log, dbConn, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()

# xnose-class-to-test-main-command-line-function-of-module


class test_ntt_data_coordinate_crossmatch_checks(unittest.TestCase):

    def test_ntt_data_coordinate_crossmatch_checks_function(self):
        kwargs = {}
        kwargs["dbConn"] = dbConn
        kwargs["log"] = log
        # xt-kwarg_key_and_value

        ntt_data_coordinate_crossmatch_checks.ntt_data_coordinate_crossmatch_checks(
            **kwargs)

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
