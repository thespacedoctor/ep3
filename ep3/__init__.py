import webapp

from fundamentals import utKit


# OVERRIDES
class utKit(utKit):

    """
    *Override dryx utKit*

    **Key Arguments:**


    .. todo::

        - @review: when complete, clean utKit class
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract class to another module
    """
    # Variable Data Atrributes

    def __init__(
            self,
            moduleDirectory):
        """
        *init*  

        **Key Arguments:**
            # -

        **Return:**
            - None

        .. todo::

            - @review: when complete, clean __init__ method
            - @review: when complete add logging
        """
        self.moduleDirectory = moduleDirectory
        self.pathToInputDir = moduleDirectory + "/input/"
        self.pathToOutputDir = moduleDirectory + "/output/"
        # Override Variable Data Atrributes
        self.dbConfig = """
        version: 1
        db: pessto_marshall_sandbox
        host: localhost
        user: root
        password: root
        """

        # SETUP LOGGING
        self.loggerConfig = """
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
                level: WARNING
                formatter: console_style
                stream: ext://sys.stdout
        root:
            level: WARNING
            handlers: [console]"""

        return None

    # use the tab-trigger below for new method
    # xt-class-method

import crossmatchers
import commonutils
