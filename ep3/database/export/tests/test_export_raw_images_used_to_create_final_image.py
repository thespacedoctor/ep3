import os
import nose
from .. import export_raw_images_used_to_create_final_image
from pessto_marshall_engine import utKit

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log, dbConn, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()


class test_command_line(unittest.TestCase):

    def test_command_line_method_01(self):
        kwargs = {}
        kwargs["<pathToSettingsFile>"] = pathToInputDir + \
            "project_settings.yaml"
        kwargs["<filename>"] = "SN2012ec_20130128_Ks_merge_56475_1.fits"
        kwargs["<exportDirectory>"] = "/Users/Dave/Desktop/tmp"
        export_raw_images_used_to_create_final_image.main(kwargs)

    # x-class-method-to-test-a-command-line-usage
# xnose-class-to-test-named-worker-function
