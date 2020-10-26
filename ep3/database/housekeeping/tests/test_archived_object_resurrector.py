import os
import nose
import shutil
import yaml
from .. import archived_object_resurrector
from pessto_marshall_engine import utKit

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log, dbConn, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()

stream = file(
    "/Users/Dave/git_repos/marshall_webapp/marshall_webapp/settings/pessto_marshall_mac.yaml", 'r')
settings = yaml.load(stream)
stream.close()

# xnose-class-to-test-main-command-line-function-of-module


class test_archived_object_resurrector(unittest.TestCase):

    def test_archived_object_resurrector_function(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["dbConn"] = dbConn
        # xt-kwarg_key_and_value

        resurrector = archived_object_resurrector(**kwargs)
        resurrector.get()

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
