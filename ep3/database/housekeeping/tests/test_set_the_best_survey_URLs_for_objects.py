import os
import nose
import shutil
import yaml
from .. import set_the_best_survey_URLs_for_objects
from pessto_marshall_engine import utKit

# load settings
stream = file(
    "/Users/Dave/git_repos/nearby_galaxy_catalogue_builder/example_settings.yaml", 'r')
settings = yaml.load(stream)
stream.close()


# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log, dbConn, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()

# xnose-class-to-test-main-command-line-function-of-module


class test_set_the_best_survey_URLs_for_objects(unittest.TestCase):

    def test_set_the_best_survey_URLs_for_objects_function(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["dbConn"] = dbConn
        kwargs["settings"] = settings
        # xt-kwarg_key_and_value

        testObject = set_the_best_survey_URLs_for_objects(**kwargs)
        testObject.get()

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
