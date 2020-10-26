import os
import nose
from .. import processors

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE


def setUpModule():
    import logging
    import logging.config
    import yaml

    "set up test fixtures"
    moduleDirectory = os.path.dirname(__file__) + "/../../../tests"

    # SETUP PATHS TO COMMONG DIRECTORIES FOR TEST DATA
    global pathToInputDataDir, pathToOutputDir, pathToOutputDataDir, pathToInputDir
    pathToInputDir = moduleDirectory + "/input/"
    pathToInputDataDir = pathToInputDir + "data/"
    pathToOutputDir = moduleDirectory + "/output/"
    pathToOutputDataDir = pathToOutputDir + "data/"

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

# class test_crossmatch_ntt_data_against_transientBucket(unittest.TestCase):
#     def test_that_crossmatch_function_works_as_expected(self):
#         kwargs = {}
#         kwargs["log"] = log
#         kwargs["dbConn"] = dbConn
#         processors.crossmatch_ntt_data_against_transientBucket(**kwargs)

# class test_update_filename_and_filepath_in_database(unittest.TestCase):
#     def test_update_filename_and_filepath_in_database_works_as_expected(self):
#         kwargs = {}
#         kwargs["log"] = log
#         kwargs["dbConn"] = dbConn
#         kwargs["pathToArchiveRoot"] = "/tmp"
#         processors.update_filename_and_filepath_in_database(**kwargs)

# class test_create_spectra_binary_table_extension_rows_in_db(unittest.TestCase):
#     def test_create_spectra_binary_table_extension_rows_in_db_works_as_expected(self):
#         kwargs = {}
#         kwargs["log"] = log
#         kwargs["dbConn"] = dbConn
#         processors.create_spectra_binary_table_extension_rows_in_db(**kwargs)


# class test_update_sofi_spectra_mjd_keywords(unittest.TestCase):

#     def test_update_sofi_spectra_mjd_keywords_works_as_expexted(self):
#         kwargs = {}
#         kwargs["log"] = log
#         kwargs["dbConn"] = dbConn
#         processors.update_sofi_spectra_mjd_keywords.update_sofi_spectra_mjd_keywords(
#             **kwargs)
