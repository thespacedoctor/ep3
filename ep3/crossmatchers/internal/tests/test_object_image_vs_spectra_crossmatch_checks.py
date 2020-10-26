import os
import nose
import shutil
from .. import object_image_vs_spectra_crossmatch_checks
from pessto_marshall_engin import utKit

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log, dbConn, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()

# xnose-class-to-test-main-command-line-function-of-module
# xnose-class-to-test-named-worker-function
