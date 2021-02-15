#!/usr/bin/env python
# encoding: utf-8
"""
*Tools for building the ESO Transient and Photometry Catalogues*

:Author:
    David Young

:Date Created:
    February 15, 2021
"""
from builtins import object
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from xlrd import open_workbook, cellname, cellnameabs, colname
from astropy.io import fits
from datetime import datetime, date, time
import numpy as np
import collections
# OR YOU CAN REMOVE THE CLASS BELOW AND ADD A WORKER FUNCTION ... SNIPPET TRIGGER BELOW
# xt-worker-def


class catalogues(object):
    """
    *The worker class for the catalogues module*

    **Key Arguments:**
        - ``log`` -- logger
        - ``pathToXLS`` -- path to the excel xls file
        - ``pathToFits`` -- path to the output fits file
        - ``settings`` -- the settings dictionary

    **Usage:**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_). 

    To initiate a catalogues object, use the following:

    ```eval_rst
    .. todo::

        - add usage info
        - create a sublime snippet for usage
        - create cl-util for this class
        - add a tutorial about ``catalogues`` to documentation
        - create a blog post about what ``catalogues`` does
    ```

    ```python
    usage code 
    ```

    """
    # Initialisation
    # 1. @flagged: what are the unique attrributes for each object? Add them
    # to __init__

    def __init__(
            self,
            log,
            pathToXLS=False,
            pathToFits=False,
            settings=False
    ):
        self.log = log
        log.debug("instansiating a new 'catalogues' object")
        self.settings = settings
        self.pathToXLS = pathToXLS
        self.pathToFits = pathToFits
        # xt-self-arg-tmpx

        # 2. @flagged: what are the default attrributes each object could have? Add them to variable attribute set here
        # Variable Data Atrributes

        # 3. @flagged: what variable attrributes need overriden in any baseclass(es) used
        # Override Variable Data Atrributes

        # Initial Actions

        return None

    def convert(self):
        """
        *get the catalogues object*

        **Return:**
            - ``catalogues``

        **Usage:**

        ```eval_rst
        .. todo::

            - add usage info
            - create a sublime snippet for usage
            - create cl-util for this method
            - update the package tutorial if needed
        ```

        ```python
        usage code 
        ```
        """
        self.log.debug('starting the ``convert`` method')

        # Variable Data Atrributes
        self.primaryHeaderSheet = None
        self.visibleSheets = []
        self.extHeaderSheets = []
        self.dataTableSheets = []

        self.wb = open_workbook(self.pathToXLS, formatting_info=True)

        # Find the primary header, extension headers and the data tables
        for sheet in self.wb.sheets():
            if sheet.visibility == 0:
                # sheet is visible
                if "primary" in sheet.name.lower() and "header" in sheet.name.lower():
                    self.primaryHeaderSheet = sheet
                if "ext" in sheet.name.lower() and "header" in sheet.name.lower():
                    self.extHeaderSheets.append(sheet)
                if "data" in sheet.name.lower() and "table" in sheet.name.lower():
                    self.dataTableSheets.append(sheet)
                self.visibleSheets.append(sheet)

        # sort the extension headers
        self.extHeaderSheets = list(self.extHeaderSheets)
        self.extHeaderSheets = sorted(
            self.extHeaderSheets, key=lambda x: x.name, reverse=False)

        # sort the data tables
        self.dataTableSheets = list(self.dataTableSheets)
        self.dataTableSheets = sorted(
            self.dataTableSheets, key=lambda x: x.name, reverse=False)

        # generate the prinary header HDU
        primaryHeader = self._generate_fits_header(
            sheet=self.primaryHeaderSheet, primary=True)
        primHdu = fits.PrimaryHDU(header=primaryHeader, data=None)
        primHdu.verify('fix')

        # generate all the extension headers and the data units (i.e. all binary
        # Table HDUs)
        binaryTableHDUList = []
        for eh, dt in zip(self.extHeaderSheets, self.dataTableSheets):
            # header first
            extensionHeader = self._generate_fits_header(
                sheet=eh, primary=False)
            # then the data
            binaryTableHDU = self._generate_data_table_unit(
                dataSheet=dt, extSheet=eh)
            # attach header and verify
            binaryTableHDU.header = extensionHeader
            binaryTableHDU.verify('fix')
            binaryTableHDUList.append(binaryTableHDU)

        hduList = [primHdu]
        hduList.extend(binaryTableHDUList)
        self.hduList = hduList
        self._write_fits_file()

        self.log.debug('completed the ``convert`` method')
        return catalogues

    def _generate_fits_header(
            self,
            sheet,
            primary=True):
        """
        *generate primary fits header*

        **Key Arguments:**
            - ``sheet`` -- sheet to generate header from
            - ``primary`` -- is this a primary HDU header?

        **Return:**
            - ``fitsHeader`` -- the header

        .. todo::

        """
        self.log.debug('starting the ``_generate_fits_header`` method')

        # GENERATE THE FITS HEADER
        if primary:
            primHdu = fits.PrimaryHDU()
            fitsHeader = primHdu.header
        else:
            fitsHeader = fits.Header()

        # find col indexes for keywords, values and comments (first row in
        # excel)
        for col in range(sheet.ncols):
            if str(sheet.cell(0, col).value).lower() == "keyword":
                keywordIndex = col
            elif str(sheet.cell(0, col).value).lower() == "value":
                valueIndex = col
            elif str(sheet.cell(0, col).value).lower() == "comment":
                commentIndex = col

        # for all remaining rows, grab the header data
        for row in range(1, sheet.nrows):
            # get keyword
            keyword = sheet.cell(row, keywordIndex).value
            # get and tidy value
            value = sheet.cell(row, valueIndex).value
            try:
                value = value.strip()
            except:
                pass
            if ("tindx" in keyword.lower() or "tpric" in keyword.lower()) and value == 1:
                value = True
            if isinstance(value, str):
                value = value.strip()
                # if not len(value):
                #     continue
                if value == 'T' or value.lower() == 'true':
                    value = True
                elif value == 'F' or value.lower() == 'false':
                    value = False
                elif value.lower() == "%now%":
                    now = datetime.now()
                    now = now.strftime("%Y-%m-%dT%H:%M:%S")
                    value = now
            if isinstance(value, float):
                if value % 1 == 0:
                    value = int(value)
            # get comment
            comment = sheet.cell(row, commentIndex).value

            # # create card and add to header
            # if isinstance(keyword, unicode):
            #     keyword = keyword.encode("utf8")
            # if isinstance(value, unicode):
            #     value = value.encode("utf8")
            # if isinstance(comment, unicode):
            #     comment = comment.encode("utf8")

            if keyword in fitsHeader:
                fitsHeader[keyword] = (value, comment)
            else:
                card = fits.Card(keyword, value, comment)
                fitsHeader.append(card)

        self.log.debug('completed the ``_generate_fits_header`` method')
        return fitsHeader

    def _generate_data_table_unit(
            self,
            dataSheet,
            extSheet):
        """
        *generate data table unit*

        **Key Arguments:**
            - ``dataSheet`` -- the excel sheet containing the data
            - ``extSheet`` -- sheet containing the extension  data header

        **Return:**
            - ``binTableHdu`` -- the table HDU (pre-header population)

        .. todo::

        """
        self.log.debug('starting the ``_generate_data_table_unit`` method')

        # read the format dictionary in from the extension header sheet
        for col in range(extSheet.ncols):
            if str(extSheet.cell(0, col).value).lower() == "keyword":
                keywordIndex = col
            elif str(extSheet.cell(0, col).value).lower() == "value":
                valueIndex = col
            elif str(extSheet.cell(0, col).value).lower() == "comment":
                commentIndex = col

        # read in the column formats from the ext header sheet
        typesDict = {}
        formDict = {}
        nullDict = {}
        for row in range(1, extSheet.nrows):
            for col in range(extSheet.ncols):
                rowKey = extSheet.cell(row, keywordIndex).value
                rowValue = extSheet.cell(row, valueIndex).value
                try:
                    rowValue = rowValue.strip()
                except:
                    pass
                rowComment = extSheet.cell(row, commentIndex).value
                if "ttype" in rowKey.lower():
                    typesDict[rowValue] = rowKey
                elif "tform" in rowKey.lower():
                    formDict[rowKey] = rowValue
                elif "tnull" in rowKey.lower():
                    if isinstance(rowValue, float):
                        if rowValue % 1 == 0:
                            rowValue = int(rowValue)
                    nullDict[rowKey] = rowValue

        # determine length of data table
        tableLength = 0
        dataSheetName = dataSheet.name
        self.log.debug("""datasheet name: `%(dataSheetName)s`""" % locals())
        # print dataSheet.colinfo_map
        for row in range(dataSheet.nrows):
            tableLength = row
            rowBlank = True
            for col in range(dataSheet.ncols):
                if not dataSheet.colinfo_map[col].hidden:
                    if dataSheet.cell_type(row, col) != 0 and dataSheet.cell_type(row, col) != 6:
                        rowBlank = False
                        break
            if rowBlank:
                tableLength = row
                break

        self.log.debug("data sheet tableLength :%(tableLength)s" % locals())

        # read in the column names from the datasheet - create a dictionary of
        masterColDict = {}
        allColumns = []
        for col in range(dataSheet.ncols):
            if not dataSheet.colinfo_map[col].hidden:
                colName = dataSheet.cell(0, col).value
                if not len(colName) or colName not in typesDict:
                    continue
                colValues = []
                for row in range(1, tableLength):
                    cellValue = dataSheet.cell(row, col).value
                    try:
                        cellValue = cellValue.strip()
                    except:
                        pass
                    if isinstance(cellValue, str) and len(cellValue) == 0:
                        cellValue = None

                    colValues.append(cellValue)

                # FIND TYPE NUMBER
                colType = typesDict[colName]
                colTypeIndex = int(colType.replace("TTYPE", ""))
                colTypeIndex = """TTYPE%(colTypeIndex)05.0f""" % locals()
                colForm = colType.replace("TTYPE", "TFORM")
                colForm = formDict[colForm]
                colNull = colType.replace("TTYPE", "TNULL")
                if colNull in nullDict:
                    colNull = nullDict[colNull]
                else:
                    colNull = None
                colValues = np.array(colValues)

                masterColDict[colTypeIndex] = {"name": colName,
                                               "type": colType, "form": colForm, "array": colValues, "null": colNull}
                if "mjd" in colName.lower():
                    tmp = masterColDict[colTypeIndex]
                    self.log.debug(
                        """masterColDict[colTypeIndex]: `%(tmp)s`""" % locals())
        # sort masterColDict by keys

        omasterColDict = collections.OrderedDict(sorted(masterColDict.items()))

        for k, v in omasterColDict.items():
            try:
                self.log.debug(
                    "attempting to generate the column for the FITS binary table")
                thisColumn = fits.Column(
                    name=str(v["name"]), format=v["form"], array=v["array"], null=v["null"])
            except Exception as e:
                self.log.debug(v["name"])
                self.log.debug(v["form"])
                self.log.debug(v["array"])
                self.log.debug(v["null"])
                self.log.error(
                    "could not generate the column for the FITS binary table - failed with this error: %s " % (str(e),))
                sys.exit(0)

            print(thisColumn)

            allColumns.append(thisColumn)
        print(allColumns)
        binTableHdu = fits.BinTableHDU.from_columns(allColumns)

        self.log.debug('completed the ``_generate_data_table_unit`` method')

        return binTableHdu

    def _write_fits_file(
            self):
        """
        *write fits file*

        **Key Arguments:**

        **Return:**
            - None

        .. todo::

        """
        self.log.debug('starting the ``_write_fits_file`` method')

        # build and fix HDU list
        hduList = fits.HDUList(self.hduList)
        hduList.verify("fix")

        # write to file
        hduList.writeto(self.pathToFits, checksum=True, clobber=True)

        self.log.debug('completed the ``_write_fits_file`` method')
        return None

    # xt-class-method

    # 5. @flagged: what actions of the base class(es) need ammending? ammend them here
    # Override Method Attributes
    # method-override-tmpx
