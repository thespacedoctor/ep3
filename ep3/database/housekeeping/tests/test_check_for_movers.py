import os
import nose
import shutil
import yaml
from .. import check_for_movers
from pessto_marshall_engine import utKit

stream = file(
    "/Users/Dave/git_repos/marshall_webapp/marshall_webapp/settings/pessto_marshall_mac.yaml", 'r')
settings = yaml.load(stream)
stream.close()

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log, dbConn, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()

# xnose-class-to-test-main-command-line-function-of-module


class test_check_for_movers(unittest.TestCase):

    def test_check_for_movers_function(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["dbConn"] = dbConn
        # xt-kwarg_key_and_value

        checker = check_for_movers(**kwargs)
        checker.get()

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
