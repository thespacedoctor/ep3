import os
import nose
from .. import fitsmanipulator

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE


def setUpModule():
    import logging
    import logging.config
    import yaml

    "set up test fixtures"
    moduleDirectory = os.path.dirname(__file__) + "/../../../tests"

    # SETUP PATHS TO COMMONG DIRECTORIES FOR TEST DATA
    global pathToInputDataDir, pathToOutputDir, pathToOutputDataDir, pathToInputDir, pathToFilesToRewrite, pathToFilesToRewrite_bk
    pathToInputDir = moduleDirectory + "/input/"
    pathToInputDataDir = pathToInputDir + "data/"
    pathToOutputDir = moduleDirectory + "/output/"
    pathToOutputDataDir = pathToOutputDir + "data/"
    pathToFilesToRewrite = pathToInputDataDir + "/fits_files_for_rewrite_tests/"
    pathToFilesToRewrite_bk = pathToInputDataDir + \
        "/fits_files_for_rewrite_tests_bk/"

    # SETUP THE TEST LOG FILE
    global testlog
    testlog = open(pathToOutputDir + "tests.log", 'w')

    # SETUP LOGGING
    loggerConfig = """
    version: 1
    formatters:
        file_style:
            format: '* %(asctime)s - %(name)s - %(levelname)s (%(filename)s > %(funcName)s > %(lineno)d) - %(message)s  '
            datefmt: '%Y/%m/%d %H:%M:%S'
        console_style:
            format: '* %(asctime)s - %(levelname)s: %(filename)s:%(funcName)s:%(lineno)d > %(message)s'
            datefmt: '%H:%M:%S'
        html_style:
            format: '<div id="row" class="%(levelname)s"><span class="date">%(asctime)s</span>   <span class="label">file:</span><span class="filename">%(filename)s</span>   <span class="label">method:</span><span class="funcName">%(funcName)s</span>   <span class="label">line#:</span><span class="lineno">%(lineno)d</span> <span class="pathname">%(pathname)s</span>  <div class="right"><span class="message">%(message)s</span><span class="levelname">%(levelname)s</span></div></div>'
            datefmt: '%Y-%m-%d <span class= "time">%H:%M <span class= "seconds">%Ss</span></span>'
    handlers:
        console:
            class: logging.StreamHandler
            level: DEBUG
            formatter: console_style
            stream: ext://sys.stdout
    root:
        level: DEBUG
        handlers: [console]"""

    logging.config.dictConfig(yaml.load(loggerConfig))
    global log
    log = logging.getLogger(__name__)

    # SETUP DB CONNECTION
    import pymysql as ms
    dbConfig = """
    version: 1
    db: pessto_marshall_sandbox
    host: localhost
    user: root
    password: root
    """
    connDict = yaml.load(dbConfig)
    global dbConn
    dbConn = ms.connect(
        host=connDict['host'],
        user=connDict['user'],
        passwd=connDict['password'],
        db=connDict['db'],
    )
    dbConn.autocommit(True)

    # REFRESH FILE STRUCTURE
    import shutil
    try:
        shutil.rmtree(pathToFilesToRewrite)
    except:
        pass
    shutil.copytree(pathToFilesToRewrite_bk, pathToFilesToRewrite)

    return None


def tearDownModule():
    "tear down test fixtures"
    # CLOSE THE TEST LOG FILE
    testlog.close()
    return None


class emptyLogger:
    info = None
    error = None
    debug = None
    critical = None
    warning = None


class test_generate_primary_fits_header_from_database(unittest.TestCase):

    def test_generate_primary_fits_header_from_database_works_as_expected(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["dbConn"] = dbConn
        kwargs["primaryId"] = 2087
        kwargs["dbTable"] = "sofi_imaging"
        fitsmanipulator.generate_primary_fits_header_from_database(**kwargs)

    def test_generate_primary_fits_header_from_database_works_for_binary_spectrum(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["dbConn"] = dbConn
        kwargs["primaryId"] = 1963
        kwargs["dbTable"] = "sofi_spectra"
        fitsHeader = fitsmanipulator.generate_primary_fits_header_from_database(
            **kwargs)

    def test_generate_primary_fits_header_from_database_works_for_binary_spectrum(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["dbConn"] = dbConn
        kwargs["primaryId"] = 5163
        kwargs["dbTable"] = "efosc_spectra"
        fitsHeader = fitsmanipulator.generate_primary_fits_header_from_database(
            **kwargs)


class test_rewrite_fits_file(unittest.TestCase):

    def test_rewrite_fits_file(self):
        # kwargs = {}
        # kwargs["log"] = log
        # kwargs["dbConn"] = dbConn
        # kwargs["primaryId"] = 2087
        # kwargs["dbTable"] = "sofi_imaging"
        # fitsHeader = fitsmanipulator.generate_primary_fits_header_from_database(**kwargs)

        # kwargs = {}
        # kwargs["log"] = log
        # kwargs["dbConn"] = dbConn
        # kwargs["pathToFitsFile"] = pathToFilesToRewrite + "SN2012ec_20130128_Ks_merge_56475_2.fits"
        # kwargs["primaryId"] = 2087
        # kwargs["fitsHeader"] = fitsHeader
        # kwargs["instrument"] = "sofi"
        # kwargs["binaryTable"] = False
        # fitsmanipulator.rewrite_fits_file(**kwargs)

        # kwargs = {}
        # kwargs["log"] = log
        # kwargs["dbConn"] = dbConn
        # kwargs["primaryId"] = 3
        # kwargs["dbTable"] = "sofi_imaging"
        # fitsHeader = fitsmanipulator.generate_primary_fits_header_from_database(**kwargs)

        # kwargs = {}
        # kwargs["log"] = log
        # kwargs["dbConn"] = dbConn
        # kwargs["pathToFitsFile"] = pathToFilesToRewrite + "SN2012ec_20130128_Ks_merge_56475_2.weight.fits"
        # kwargs["fitsHeader"] = fitsHeader
        # kwargs["primaryId"] = 3
        # kwargs["instrument"] = "sofi"
        # kwargs["binaryTable"] = False
        # fitsmanipulator.rewrite_fits_file(**kwargs)

        kwargs = {}
        kwargs["log"] = log
        kwargs["dbConn"] = dbConn
        kwargs["primaryId"] = 1963
        kwargs["dbTable"] = "sofi_spectra"
        fitsHeader = fitsmanipulator.generate_primary_fits_header_from_database(
            **kwargs)

        kwargs = {}
        kwargs["log"] = log
        kwargs["dbConn"] = dbConn
        kwargs["pathToFitsFile"] = pathToFilesToRewrite + \
            "SN2012ec_20130113_GB_merge_56478_1_sb.fits"
        kwargs["fitsHeader"] = fitsHeader
        kwargs["primaryId"] = 1963
        kwargs["instrument"] = "sofi"
        kwargs["binaryTable"] = True
        fitsmanipulator.rewrite_fits_file(**kwargs)


class test_execute_required_fits_header_rename_and_rewrites(unittest.TestCase):

    def test_execute_required_fits_header_rename_and_rewrites(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["dbConn"] = dbConn
        fitsmanipulator.execute_required_fits_header_rename_and_rewrites(
            **kwargs)


class test_build_bintable_data(unittest.TestCase):

    def test_build_bintable_data_works_for_sofi_spectrum(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["pathToFitsFile"] = pathToFilesToRewrite + \
            "SN2012ec_20130113_GB_merge_56478_1_sc.fits"
        fitsmanipulator.build_bintable_data(**kwargs)


class test_generate_bintable_extension_header(unittest.TestCase):

    def test_generate_bintable_extension_header_works_for_sofi_spectrum(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["pathToFitsFile"] = pathToFilesToRewrite + \
            "SN2012ec_20130113_GB_merge_56478_1_sc.fits"
        binTableHdu, pixelCount = fitsmanipulator.build_bintable_data(**kwargs)

        kwargs = {}
        kwargs["log"] = log
        kwargs["dbConn"] = dbConn
        kwargs["oneDSpectrumPath"] = pathToFilesToRewrite + \
            "SN2012ec_20130113_GB_merge_56478_1_sc.fits"
        kwargs["pixelCount"] = pixelCount
        kwargs["primaryId"] = 13
        kwargs["instrument"] = "sofi"
        fitsmanipulator.generate_bintable_extension_header(**kwargs)
