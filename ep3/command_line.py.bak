from pessto_marshall_engine.database.nttarchiver import ingesters
from pessto_marshall_engine.database.nttarchiver import processors
from pessto_marshall_engine.database.nttarchiver import fitsmanipulator
from pessto_marshall_engine.database import conesearchers


def _set_up_command_line_tool():
    import logging
    import logging.config
    import yaml

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
        level: WARNING
        handlers: [console]"""

    logging.config.dictConfig(yaml.load(loggerConfig))
    log = logging.getLogger(__name__)

    return log

# LAST MODIFIED : August 27, 2013
# CREATED : August 27, 2013
# AUTHOR : DRYX


def pm_ingest_directory_into_database(clArgs=None):
    """
    *Recursively ingest the fits files in a directory into the PESSTO marshall database

    Usage:
        pm_ingest_directory_into_database --host=<host> --user=<user> --passwd=<passwd> --dbName=<dbName>  --pathToDropboxRoot=<pathToDropboxRoot> --pathToArchiveRoot=<pathToArchiveRoot>

        -h, --help    show this help message*
    """
    ################ > IMPORTS ################
    ## STANDARD LIB ##
    import sys
    import os
    from subprocess import Popen, PIPE
    ## THIRD PARTY ##
    from docopt import docopt
    ## LOCAL APPLICATION ##
    import dryxPython.commonutils as dcu

    if clArgs == None:
        clArgs = docopt(pm_ingest_directory_into_database.__doc__)

    # SETUP AN EMPTY LOGGER
    log = _set_up_command_line_tool()

    # SETUP DB CONNECTION
    import pymysql as ms
    global dbConn
    dbConn = ms.connect(
        host=clArgs['--host'],
        user=clArgs['--user'],
        passwd=clArgs['--passwd'],
        db=clArgs['--dbName'],
    )
    dbConn.autocommit(True)

    host = clArgs['--host']
    user = clArgs['--user']
    passwd = clArgs['--passwd']
    db = clArgs['--dbName']
    pathToDropboxRoot = clArgs["--pathToDropboxRoot"]
    pathToArchiveRoot = clArgs["--pathToArchiveRoot"]

    # TEST THE ARGUMENTS
    if not os.path.exists(pathToArchiveRoot):
        message = "the path to the archive root %s does not exist on this machine" % (
            pathToArchiveRoot,)
        log.critical(message)
        raise IOError(message)

    if not os.path.exists(pathToDropboxRoot):
        message = "the path to the Dropbox folder %s does not exist on this machine" % (
            pathToDropboxRoot,)
        log.critical(message)
        raise IOError(message)

    # @completed:
    print "## REMOVING UNNECESSARY FILES AND FOLDERS ##"
    ingesters.clean_dropbox_folder(
        dbConn,
        log,
        folderPath=pathToDropboxRoot
    )

    # @completed:
    dcu.recursively_remove_empty_directories(
        log,
        basePath=pathToDropboxRoot,
    )

    # @completed:
    print "## INGEST THE FITS FILES ##"
    kwargs = {}
    kwargs["log"] = log
    kwargs["dbConn"] = dbConn
    kwargs["pathToDropboxRoot"] = pathToDropboxRoot
    kwargs["pathToArchiveRoot"] = pathToArchiveRoot
    ingesters.recursive_ingest_of_fits_files_in_directory(**kwargs)

    # @completed:
    print "## CLEANING OBJECT NAMES IN TRANSIENTBUCKET ##"
    pwd = os.getcwd()
    moduleDirectory = os.path.dirname(__file__)
    databaseFile = moduleDirectory + \
        "/database/housekeeping/helper_scripts/mysql/clean_objectnames_in_transientbucket.sql"
    cmd = "mysql -u %s --password=%s %s -h %s -f < %s" % (
        user, passwd, db, host, databaseFile)
    process = Popen(cmd,
                    stdout=PIPE, stdin=PIPE, shell=True)
    output = process.communicate()[0]
    process.wait()

    # @completed:
    print "## ASSOCIATE NEW FILES WITH THEIR RESPECTIVE TRANSIENTBUCKET OBJECTS ##"
    processors.crossmatch_ntt_data_against_transientBucket(dbConn, log)

    # @completed:
    print "## RUN THE NTT DATA PROCESSING SQL SCRIPTS ##"
    pwd = os.getcwd()
    moduleDirectory = os.path.dirname(__file__)
    os.chdir(moduleDirectory + "/database/nttarchiver/helper_scripts/mysql")
    databaseFile = "main.sql"
    cmd = "mysql -u %s --password=%s %s -h %s -f < %s" % (
        user, passwd, db, host, databaseFile)
    process = Popen(cmd,
                    stdout=PIPE, stdin=PIPE, shell=True)
    output = process.communicate()[0]
    process.wait()
    print output
    os.chdir(pwd)

    # @completed:
    print "## CREATE THE BINARY TABLE EXTENSION ROWS IN DATABASE -- NEED DUPLICATED FROM 1D SPECTRA ##"
    processors.create_spectra_binary_table_extension_rows_in_db(
        log=log,
        dbConn=dbConn
    )

    # @completed:
    print "## FILL THE BINARY SPECTRA EXTENSION MYSQL TABLES ##"
    databaseFile = moduleDirectory + \
        "/database/nttarchiver/helper_scripts/mysql/update_add_binary_tables_spectrum_rows.sql"
    cmd = "mysql -u %s --password=%s %s -h %s -f < %s" % (
        user, passwd, db, host, databaseFile)
    process = Popen(cmd,
                    stdout=PIPE, stdin=PIPE, shell=True)
    output = process.communicate()[0]
    process.wait()
    print output
    os.chdir(pwd)

    # @completed:
    print "## MOVE THE FILES TO THE ARCHIVE ##"
    processors.move_files_to_local_archive(
        pathToArchiveRoot=pathToArchiveRoot,
        pathToDropbox=pathToDropboxRoot,
        dbConn=dbConn,
        log=log
    )

    # @completed:
    print "## UPDATE FILENAMES AND PATHS IF CHANGED ##"
    processors.update_filename_and_filepath_in_database(
        log=log,
        dbConn=dbConn,
        pathToArchiveRoot=pathToArchiveRoot
    )

    print "## DO ALL THE REQUIRED REWRITES OF THE FITS HEADERS ##"
    # STEP.
    fitsmanipulator.execute_required_fits_header_rename_and_rewrites(
        dbConn,
        log
    )

    return None

# LAST MODIFIED : August 30, 2013
# CREATED : August 30, 2013
# AUTHOR : DRYX


def pm_associate_ntt_data_with_transientBucket_objects(clArgs=None):
    """
    *associate ntt data with transientBucket objects

    Usage:
        pm_associate_ntt_data_with_transientBucket_objects --host=<host> --user=<user> --passwd=<passwd> --dbName=<dbName>

        -h, --help    show this help message*
    """
    ################ > IMPORTS ################
    ## STANDARD LIB ##
    import sys
    import os
    ## THIRD PARTY ##
    from docopt import docopt
    ## LOCAL APPLICATION ##

    if clArgs == None:
        clArgs = docopt(
            pm_associate_ntt_data_with_transientBucket_objects.__doc__)

    # SETUP AN EMPTY LOGGER
    log = _set_up_command_line_tool()

    # SETUP DB CONNECTION
    import pymysql as ms
    global dbConn
    dbConn = ms.connect(
        host=clArgs['--host'],
        user=clArgs['--user'],
        passwd=clArgs['--passwd'],
        db=clArgs['--dbName'],
    )
    dbConn.autocommit(True)

    # TEST THE ARGUMENTS

    ## VARIABLES ##
    processors.crossmatch_ntt_data_against_transientBucket(dbConn, log)

    return None

# LAST MODIFIED : September 9, 2013
# CREATED : September 9, 2013
# AUTHOR : DRYX


def pm_rewrite_ntt_data_fitsheaders(clArgs=None):
    """
    *rewrite ntt data fitsheaders for files that have had a recent update to its fits header keywords, or move new files from the dropbox location to the local archive

    Usage:
        pm_rewrite_ntt_data_fitsheaders --host=<host> --user=<user> --passwd=<passwd> --dbName=<dbName>

        -h, --help    show this help message*
    """
    ################ > IMPORTS ################
    ## STANDARD LIB ##
    import sys
    import os
    ## THIRD PARTY ##
    from docopt import docopt
    ## LOCAL APPLICATION ##

    # SETUP AN EMPTY LOGGER
    log = _set_up_command_line_tool()

    if clArgs == None:
        clArgs = docopt(pm_rewrite_ntt_data_fitsheaders.__doc__)

    # SETUP DB CONNECTION
    import pymysql as ms
    global dbConn
    dbConn = ms.connect(
        host=clArgs['--host'],
        user=clArgs['--user'],
        passwd=clArgs['--passwd'],
        db=clArgs['--dbName'],
    )
    dbConn.autocommit(True)
    # TEST THE ARGUMENTS

    ## VARIABLES ##
    fitsmanipulator.execute_required_fits_header_rename_and_rewrites(
        dbConn=dbConn,
        log=log
    )

    return None

# LAST MODIFIED : October 16, 2013
# CREATED : October 16, 2013
# AUTHOR : DRYX


def pm_conesearch_transientBucket(clArgs=None):
    """
    *perform a conesearch on the transientBucket

    Usage:
        pm_conesearch_transientBucket --host=<host> --user=<user> --passwd=<passwd> --dbName=<dbName> --raDeg=<raDeg> --decDeg=<decDeg> --arcSec=<radius>

        -h, --help    show this help message
        -v, --version show version*
    """
    ################ > IMPORTS ################
    ## STANDARD LIB ##
    import sys
    import os
    ## THIRD PARTY ##
    from docopt import docopt
    ## LOCAL APPLICATION ##

    # SETUP AN EMPTY LOGGER
    log = _set_up_command_line_tool()

    if clArgs == None:
        clArgs = docopt(pm_conesearch_transientBucket.__doc__)

    # UNPACK clArgs
    raDeg = float(clArgs['--raDeg'])
    decDeg = float(clArgs['--decDeg'])
    radius = float(clArgs['--arcSec'])

    # SETUP DB CONNECTION
    if "--dbConn" in clArgs:
        dbConn = clArgs["--dbConn"]
    else:
        import pymysql as ms
        host = clArgs['--host']
        user = clArgs['--user']
        passwd = clArgs['--passwd']
        db = clArgs['--dbName']
        dbConn = ms.connect(
            host=host,
            user=user,
            passwd=passwd,
            db=db,
        )
        dbConn.autocommit(True)

    transientBucketIdList, raList, decList, objectNameList = crossmatchers.conesearch_marshall_transientBucket_objects(
        dbConn=dbConn,
        log=log,
        ra=raDeg,
        dec=decDeg,
        radiusArcSec=radius,
        nearest=False
    )

    for t, r, d, o in zip(transientBucketIdList, raList, decList, objectNameList):
        print """%s | %s | %s | %s""" % (o.ljust(25), str(r).ljust(20), str(d).ljust(20), str(t).ljust(10))

    return None

# ## LAST MODIFIED : October 1, 2013
# ## CREATED : October 1, 2013
# ## AUTHOR : DRYX
# def pm_send_files_to_qub_archive(clArgs=None):
#     """move files from the marshall dropbox location to the qub archive nested folder structure

#     Usage:
# pm_send_files_to_qub_archive --pathToArchiveRoot=<pathToArchiveRoot>
# --pathToDropboxRoot=<pathToDropboxRoot> --host=<host> --user=<user>
# --passwd=<passwd> --dbName=<dbName>

#         -h, --help    show this help message
#         -v, --version show version
#     """
#     ################ > IMPORTS ################
#     ## STANDARD LIB ##
#     import sys
#     import os
#     ## THIRD PARTY ##
#     from docopt import docopt
#     ## LOCAL APPLICATION ##

#     ## SETUP AN EMPTY LOGGER
#     log = _set_up_command_line_tool()

#     if clArgs == None:
#         clArgs = docopt(pm_send_files_to_qub_archive.__doc__)

#     ## UNPACK clArgs
#     pathToDropboxRoot = clArgs["--pathToDropboxRoot"]
#     pathToArchiveRoot = clArgs["--pathToArchiveRoot"]


#     if clArgs.has_key("--dbConn"):
#         dbConn = clArgs["--dbConn"]
#     else:
#         host=clArgs['--host']
#         user=clArgs['--user']
#         passwd=clArgs['--passwd']
#         db=clArgs['--dbName']

#         # SETUP DB CONNECTION
#         import pymysql as ms
#         dbConn = ms.connect(
#             host=host,
#             user=user,
#             passwd=passwd,
#             db=db,
#         )
# dbConn.autocommit(True)

#     ## TEST THE ARGUMENTS
#     if not os.path.exists(pathToArchiveRoot):
#         message = "the path to the archive root %s does not exist on this machine" % (pathToArchiveRoot,)
#         log.critical(message)
#         raise IOError(message)

#     if not os.path.exists(pathToDropboxRoot):
#         message = "the path to the Dropbox folder %s does not exist on this machine" % (pathToDropboxRoot,)
#         log.critical(message)
#         raise IOError(message)

#     ## VARIABLES ##
#     processors.move_files_to_local_archive(
#         pathToArchiveRoot=pathToArchiveRoot,
#         pathToDropbox=pathToDropboxRoot,
#         dbConn=dbConn,
#         log=log
#     )

#     processors.update_filename_and_filepath_in_database(
#         log=log,
#         dbConn=dbConn,
#         pathToArchiveRoot=pathToArchiveRoot
#     )

#     processors.create_spectra_binary_table_extension_rows_in_db(
#         log=log,
#         dbConn=dbConn
#     )

#     databaseFile = os.path.dirname(__file__)+"/database/nttarchiver/helper_scripts/mysql/update_add_binary_tables_spectrum_rows.sql"
#     from subprocess import Popen, PIPE
#     process = Popen('mysql %s -u%s -p%s' % (db, user, passwd),
#         stdout=PIPE, stdin=PIPE, shell=True)
#     output = process.communicate('source ' + databaseFile)[0]

#     fitsmanipulator.execute_required_fits_header_rename_and_rewrites(
#         dbConn,
#         log
#     )

#     return None
