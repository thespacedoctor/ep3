import os
import nose
import shutil
from .. import get_image_url_from_crts_voevent_links
from pessto_marshall_engine import utKit

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log, dbConn, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()

# xnose-class-to-test-main-command-line-function-of-module


class test_get_image_url_from_crts_voevent_links(unittest.TestCase):

    def test_get_image_url_from_crts_voevent_links_function(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs[
            "url"] = "http://voeventnet.caltech.edu/feeds/ATEL/CRTS/1301031090774135654.atel.html"
        # xt-kwarg_key_and_value
        downloader = get_image_url_from_crts_voevent_links(**kwargs)
        url = downloader.get()
        print url
        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
