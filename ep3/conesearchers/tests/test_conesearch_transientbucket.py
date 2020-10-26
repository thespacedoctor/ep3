import os
import nose
from .. import conesearch_transientbucket

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE


def setUpModule():
    import logging
    import logging.config
    import yaml

    "set up test fixtures"
    moduleDirectory = os.path.dirname(__file__) + "/../../tests"

    # SETUP PATHS TO COMMONG DIRECTORIES FOR TEST DATA
    global pathToOutputDir, pathToInputDir
    pathToInputDir = moduleDirectory + "/input/"
    pathToOutputDir = moduleDirectory + "/output/"

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


class test_command_line_code_in_main(unittest.TestCase):

    def test_01_command_line_method(self):
        """*--host=<host> --user=<user> --passwd=<passwd> --dbName=<dbName> --raDeg=<raDeg> --decDeg=<decDeg> --radiusArcSec=<radius>*"""
        kwargs = {}
        kwargs["--host"] = "localhost"
        kwargs["--user"] = "root"
        kwargs["--passwd"] = "root"
        kwargs["--dbName"] = "pessto_marshall_sandbox"
        kwargs["--raDeg"] = 10.234
        kwargs["--decDeg"] = -23.453
        kwargs["--radiusArcSec"] = 5.0
        conesearch_transientbucket.main(kwargs)

    def test_02_command_line_method_with_settings_file(self):
        """*-s <pathToSettingsFile> --raDeg=<raDeg> --decDeg=<decDeg> --radiusArcSec=<radius>*"""
        kwargs = {}
        kwargs["--settingsFile"] = pathToInputDir + "project_settings.yaml"
        kwargs["--raDeg"] = 10.234
        kwargs["--decDeg"] = -23.453
        kwargs["--radiusArcSec"] = 5.0
        conesearch_transientbucket.main(kwargs)
