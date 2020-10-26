#!/usr/local/bin/python
# encoding: utf-8
"""
*Get the image URLs from CRTS VOEvent Links*

:Author:
    David Young

:Date Created:
    December 19, 2014

.. todo::
    
    @review: when complete pull all general functions and classes into dryxPython

# xdocopt-usage-tempx
"""
################# GLOBAL IMPORTS ####################
import sys
import os
import re
import readline
import glob
import pickle
from docopt import docopt
from dryxPython import webcrawlers as dwc
from dryxPython import logs as dl
from dryxPython import commonutils as dcu
from fundamentals import tools
# from ..__init__ import *

###################################################################
# CLASSES                                                         #
###################################################################


class get_image_url_from_crts_voevent_links():

    """
    *The worker class for the get_image_url_from_crts_voevent_links module*

    **Key Arguments:**
        - ``log`` -- logger
        - ``url`` -- the voevent page url

    .. todo::

        - @review: when complete, clean get_image_url_from_crts_voevent_links class
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract class to another module
    """
    # Initialisation
    # 1. @flagged: what are the unique attrributes for each object? Add them
    # to __init__

    def __init__(
        self,
        log,
        url
    ):
        self.log = log
        self.log.debug(
            "instansiating a new 'get_image_url_from_crts_voevent_links' object")
        self.url = url
        # xt-self-arg-tmpx

        # 2. @flagged: what are the default attrributes each object could have? Add them to variable attribute set here
        # Variable Data Atrributes

        # 3. @flagged: what variable attrributes need overriden in any baseclass(es) used
        # Override Variable Data Atrributes

        # Initial Actions
        self._download_the_page()
        self._parse_the_page_to_find_first_image_url()

        return None

    def close(self):
        del self
        return None

    # 4. @flagged: what actions does each object have to be able to perform? Add them here
    # Method Attributes
    def get(self):
        return self.imageUrl

    def _download_the_page(
            self):
        """
        *download the page*

        **Key Arguments:**
            # -

        **Return:**
            - None

        .. todo::

            - @review: when complete, clean _download_the_page method
            - @review: when complete add logging
        """
        self.log.debug('starting the ``_download_the_page`` method')

        self.localUrl = dwc.singleWebDocumentDownloader(
            url=self.url,
            downloadDirectory="/tmp",
            log=self.log,
            timeStamp=1,
            credentials=False
        )

        self.log.debug('completed the ``_download_the_page`` method')
        return None

    # use the tab-trigger below for new method
    def _parse_the_page_to_find_first_image_url(
            self):
        """
        *parse the page to find first image url*

        **Key Arguments:**
            # -

        **Return:**
            - None

        .. todo::

            - @review: when complete, clean _parse_the_page_to_find_first_image_url method
            - @review: when complete add logging
        """
        self.log.debug(
            'completed the ````_parse_the_page_to_find_first_image_url`` method')

        self.imageUrl = False
        if not self.localUrl:
            return None

        reBaseUrl = re.compile(r'(ht.*)/.*$')

        import codecs
        pathToReadFile = self.localUrl
        try:
            self.log.debug("attempting to open the file %s" %
                           (pathToReadFile,))
            readFile = codecs.open(pathToReadFile, encoding='utf-8', mode='r')
            thisData = readFile.read()
            readFile.close()
        except IOError, e:
            message = 'could not open the file %s' % (pathToReadFile,)
            self.log.critical(message)
            raise IOError(message)
        readFile.close()

        matchObject = re.search(
            r'<tr><td>Image 1</td><td>Image 2</td></tr>\s+<tr>[\S ]*?src="(?P<imageUrl>[\S ]*?)"', thisData, re.S)
        if matchObject:
            imageUrl = matchObject.group("imageUrl")
            while imageUrl[0] == ".":
                imageUrl = imageUrl[1:]
            baseUrl = reBaseUrl.sub("\g<1>", self.url)
            self.imageUrl = baseUrl + imageUrl

        self.log.debug(
            'completed the ``_parse_the_page_to_find_first_image_url`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method

if __name__ == '__main__':
    main()
