import os
import nose
from .. import ingest_fits_files_in_ntt_dropbox_folder

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE


def setUpModule():
    import logging
    import logging.config
    import yaml

    from pessto_marshall_engine.commonutils.getpackagepath import getpackagepath

    # SETUP PATHS TO COMMONG DIRECTORIES FOR TEST DATA
    moduleDirectory = os.path.dirname(__file__)
    global pathToOutputDir, pathToInputDir, pathToDropboxFolder
    pathToInputDir = moduleDirectory + "/input/"
    pathToOutputDir = moduleDirectory + "/output/"
    pathToDropboxFolder = pathToInputDir + "ntt_fits_files"

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
    import dryxPython.mysql.execute_mysql_script as dms
    dms.execute_mysql_script(
        log=log,
        user=connDict['user'],
        passwd=connDict['password'],
        db=connDict['db'],
        host=connDict['host'],
        pathToMysqlScript=pathToInputDir + "test_database.sql",
        force=True
    )

    return None


def tearDownModule():
    "tear down test fixtures"
    # CLOSE THE TEST LOG FILE
    testlog.close()
    return None


class test_command_line(unittest.TestCase):

    def test_command_line_method_01(self):
        kwargs = {}
        kwargs["--settingsFile"] = pathToInputDir + \
            "project_settings_for_nttarchiver_ingesting.yaml"
        ingest_fits_files_in_ntt_dropbox_folder.main(kwargs)

    # def test_command_line_method_02(self):
    #     kwargs = {}
    #     kwargs["--host"] = "localhost"
    #     kwargs["--user"] = "root"
    #     kwargs["--passwd"] = "root"
    #     kwargs["--dbName"] = "pessto_marshall_sandbox"
    #     kwargs["--pathToDropboxFolder"] = pathToDropboxFolder
    #     kwargs["--pathToArchiveRoot"] = "/tmp/marshall_archive"
    #     ingest_fits_files_in_ntt_dropbox_folder.main(kwargs)

    # x-class-method-to-test-a-command-line-usage
# x-class-to-test-named-worker-function
