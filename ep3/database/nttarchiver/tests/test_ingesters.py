import os
from nose import with_setup
import nose
from .. import ingesters

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE


def setUpModule():
    import logging
    import logging.config
    import yaml

    # "set up test fixtures"
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
        html_log_file:
            class: logging.FileHandler
            level: DEBUG
            formatter: html_style
            filename: "tests/output/test_log.html"
            mode: w
    root:
        level: DEBUG
        handlers: [console, html_log_file]"""

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
    dbConn.close()

    return None


class test_ingest_fits_file:

    def test_error_raised_if_file_does_not_exist(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["dbConn"] = dbConn
        kwargs["pathToFitsFile"] = pathToInputDataDir + \
            ">>>>>>>>>>> not a file <<<<<<<<<<<"
        nose.tools.assert_raises(IOError, ingesters.ingest_fits_file, **kwargs)

    def test_ingest_function_works_correctly(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["dbConn"] = dbConn
        log.debug('here I am:')
        kwargs["pathToFitsFile"] = pathToInputDataDir + \
            "ntt_fits_files/EFOSC.2013-04-02T06:15:54.521.fits"
        ingesters.ingest_fits_file(**kwargs)


class test_get_fits_file_type(unittest.TestCase):

    def test_raise_error_if_fitsFileHeaderDictionary_not_dictionary(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["fitsFileHeaderDictionary"] = ">>>>>>>>>>> not a dictionary <<<<<<<<<<<"
        nose.tools.assert_raises(
            TypeError, ingesters.get_fits_file_type, **kwargs)

    def test_error_raise_if_instrument_keyword_not_found(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["fitsFileHeaderDictionary"] = {'INSTRUME': ['WRONG!', 'Instrument used.'], 'ESO TPL NAME': [
            'spectr', 'mode used.'], 'ESO DPR TECH': ['spectr', 'mode used.'], 'filename': ['removeme_file.fits', 'file to be removed'], }
        nose.tools.assert_raises(
            ValueError, ingesters.get_fits_file_type, **kwargs)

    def test_error_raise_if_mode_keyword_not_found(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["fitsFileHeaderDictionary"] = {'ESO DPR TECH': ['wrong!', 'mode used.'], 'ESO TPL NAME': ['WRONG!', 'mode used.'], 'OBSTECH': ['WRONG!', 'mode used.'], 'INSTRUME': [
            'efosc', 'Instrument used.'], 'filename': ['removeme_file.fits', 'file to be removed'], 'filePath': ['path/removeme_file.fits', 'path to file to be removed']}
        nose.tools.assert_raises(
            ValueError, ingesters.get_fits_file_type, **kwargs)

    def test_function_returns_mysqlTableName(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["fitsFileHeaderDictionary"] = {'ESO DPR TECH': ['image', 'mode used.'], 'ESO TPL NAME': ['WRONG!', 'mode used.'], 'OBSTECH': ['WRONG!', 'mode used.'], 'INSTRUME': [
            'efosc', 'Instrument used.'], 'filename': ['removeme_file.fits', 'file to be removed'], 'filePath': ['path/removeme_file.fits', 'path to file to be removed']}
        ingesters.get_fits_file_type(**kwargs)


class test_recursive_ingest_of_fits_files_in_directory(unittest.TestCase):

    def test_raise_error_if_filePath_does_not_exist(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["dbConn"] = dbConn
        kwargs["pathToRootDirectory"] = ">>>>>>>>>>> not a list <<<<<<<<<<<"
        nose.tools.assert_raises(
            IOError, ingesters.recursive_ingest_of_fits_files_in_directory, **kwargs)

    # def test_function_works_as_expected(self):
    #     kwargs = {}
    #     kwargs["log"] = log
    #     kwargs["dbConn"] = dbConn
    #     kwargs["pathToRootDirectory"] = pathToInputDataDir + "ntt_fits_files"
    #     ingesters.recursive_ingest_of_fits_files_in_directory(**kwargs)
