import os
import nose
import shutil
import yaml
from .. import export_pessto_atels
from pessto_marshall_engine import utKit

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log, dbConn, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()


stream = file(
    "/Users/Dave/git_repos/webapps/pessto_webapp/settings/pessto_marshall_mac.yaml", 'r')
settings = yaml.load(stream)
stream.close()

# xnose-class-to-test-main-command-line-function-of-module


class test_export_pessto_atels(unittest.TestCase):

    def test_export_pessto_atels_function(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["dbConn"] = dbConn
        kwargs["outputDirectory"] = "/Users/Dave/Desktop/pessto_atel_data"
        # xt-kwarg_key_and_value

        export_pessto_atels(**kwargs)

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
