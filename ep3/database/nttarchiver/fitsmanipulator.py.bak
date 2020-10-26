#!/usr/local/bin/python
# encoding: utf-8
"""
*Functions to manipulate the NTT fits headers*

:Author:
    David Young

:Date Created:
    September 8, 2013

:Notes:
    - If you have any questions requiring this script please email me: davidrobertyoung@gmail.com

.. todo::
    
    @soon: delete this file when cron jobs are working on VM03
"""
################# GLOBAL IMPORTS ####################
import sys
import os

###################################################################
# CLASSES                                                         #
###################################################################

###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# LAST MODIFIED : September 8, 2013
# CREATED : September 8, 2013
# AUTHOR : DRYX


def generate_primary_fits_header_from_database(
        dbConn,
        log,
        primaryId,
        dbTable):
    """

    *generate fits header from database using the results from the sqlQuery*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``primaryId`` -- primary id of the row in database
        - ``dbTable`` -- database table name containing file metadata

    **Return:**
        - ``fitsHeader`` -- the fits header generated from results of sql query

    .. todo::


    """
    ################ > IMPORTS ################
    ## STANDARD LIB ##
    import datetime
    from decimal import Decimal
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##
    import pyfits as pf
    import dryxPython.mysql as dms
    import dryxPython.astrotools as dat

    log.debug(
        'completed the ````generate_primary_fits_header_from_database`` function')
    # TEST THE ARGUMENTS

    ## VARIABLES ##
    if "imaging" in dbTable:
        thisTable = dbTable.replace("imaging", "image")
    if "spectra" in dbTable:
        thisTable = dbTable.replace("spectra", "spectrum_1d")

    sqlQuery = """
        SELECT filename, currentFilename, updatedFilename, filetype_key_calibration from %s where primaryId = %s
    """ % (dbTable, primaryId)
    thisFile = dms.execute_mysql_read_query(
        sqlQuery,
        dbConn,
        log
    )

    log.debug('sqlQuery for primary header: %s' % (sqlQuery,))

    # if row[0]["filetype_key_reduction_stage"] == 13:
    #     filetype = "science"
    if thisFile[0]["filetype_key_calibration"] == 11:
        thisTable += "_weight"
        log.debug('thisTable: %s' % (thisTable,))
    elif "_sb.fits" in thisFile[0]["filename"]:
        thisTable = thisTable.replace("_1d", "_binary_table")
        log.debug('thisTable: %s' % (thisTable,))
    elif "_si.fits" in thisFile[0]["currentFilename"]:
        thisTable = thisTable.replace("_1d", "_2d")
        log.debug('thisTable: %s' % (thisTable,))

    sqlQuery = """
        SELECT mysql_keyword, fits_keyword, fits_comment from fits_header_keywords where %s = 1
    """ % (thisTable,)
    cardSyntax = dms.execute_mysql_read_query(
        sqlQuery,
        dbConn,
        log
    )
    mysqlKeywords = []
    fitsKeywords = []
    fitsComments = []
    for cs in cardSyntax:
        mysqlKeywords.append(cs["mysql_keyword"])
        fitsKeywords.append(cs["fits_keyword"])
        fitsComments.append(cs["fits_comment"])

    sqlQuery = """
        SELECT %s from %s where primaryId = %s
    """ % (",".join(mysqlKeywords), dbTable, primaryId)
    log.debug('sqlQuery: %s' % (sqlQuery,))
    fileMeta = dms.execute_mysql_read_query(
        sqlQuery,
        dbConn,
        log
    )

    fileMeta = fileMeta[0]
    for k, v in fileMeta.items():
        if "HIERARCH" not in k and isinstance(v, str) and len(v) > 68:
            fileMeta[k] = v[0:68]
        if isinstance(v, datetime.datetime):
            if k == "DATE_OBS":
                fileMeta[k] = dat.getDateFromMJD(
                    fileMeta["MJD_OBS"]
                )
            fileMeta[k] = str(v).replace(" ", "T")
        elif v == "T":
            fileMeta[k] = True
        elif v == "F":
            fileMeta[k] = False
        elif isinstance(v, Decimal):
            fileMeta[k] = float(v)

    # GENERATE THE FITS HEADER
    primHdu = pf.PrimaryHDU()
    cardList = primHdu.header.ascardlist()
    index = -1
    for m, f, c in zip(mysqlKeywords, fitsKeywords, fitsComments):
        index += 1
        if m == "RA" and fileMeta[m]:
            raSex = dat.ra_to_sex(
                ra=fileMeta[m],
                delimiter=':'
            )
            fitsComments[index] = raSex + " RA (J2000) pointing (deg)"
        elif m == "DECL" and fileMeta[m]:
            decSex = dat.dec_to_sex(
                dec=fileMeta[m],
                delimiter=':'
            )
            fitsComments[index] = decSex + " DEC (J2000) pointing (deg)"
        elif m == "ESO_ADA_GUID_RA" and fileMeta[m]:
            raSex = dat.ra_to_sex(
                ra=fileMeta[m],
                delimiter=':'
            )
            fitsComments[index] = raSex + " Guide Star RA J2000"
        elif m == "ESO_ADA_GUID_DEC" and fileMeta[m]:
            decSex = dat.dec_to_sex(
                dec=fileMeta[m],
                delimiter=':'
            )
            fitsComments[index] = decSex + " Guide Star DEC J2000"
        elif m == "ESO_TEL_MOON_RA" and fileMeta[m]:
            raSex = dat.ra_to_sex(
                ra=fileMeta[m],
                delimiter=':'
            )
            fitsComments[index] = raSex + " RA (J2000) (deg)"
        elif m == "ESO_TEL_MOON_DEC" and fileMeta[m]:
            decSex = dat.dec_to_sex(
                dec=fileMeta[m],
                delimiter=':'
            )
            fitsComments[index] = decSex + " DEC (J2000) (deg)"

    for m, f, c in zip(mysqlKeywords, fitsKeywords, fitsComments):
        if m in ["SIMPLE", "BITPIX", "NAXIS", "EXTEND"]:
            cardList[str(f)].comment = str(c)
            cardList[str(f)].value = fileMeta[m]
        elif "prov" in m[:6].lower() and fileMeta[m] is None:
            continue
        else:
            card = pf.Card(str(f), fileMeta[m], str(c))
            cardList.append(card)

    if "_sb.fits" in thisFile[0]["filename"]:
        # log.debug('primary header for binary: %s' % (cardList,))
        pass
    fitsHeader = pf.Header(cards=cardList)

    log.debug('cardList for PRIMARY HEADER table for file %s\n: %s' %
              (thisFile[0]["filename"], cardList,))

    log.debug(
        'completed the ``generate_primary_fits_header_from_database`` function')
    return fitsHeader


# LAST MODIFIED : April 9, 2013
# CREATED : April 9, 2013
# AUTHOR : DRYX
def generate_bintable_extension_header(log, dbConn, pixelCount, primaryId, instrument, oneDSpectrumPath):
    """
    *Given the primary header from the final NTT Pipeline reduced spectrum, build the extension header needed for the ESO Phase III ready spectrum bintable.*

    **Key Arguments:**
        - ``log`` -- logger
        - ``dbConn`` -- mysql database connection
        - ``pixelCount`` -- number of pixels in the 1D image
        - ``primaryId`` -- the primaryId of the row in the database table refering to this file (1d spectrum FITS file)
        - ``instrument`` -- sofi or efosc
        - ``oneDSpectrumPath`` -- path to the associated 1D spectrum file

    **Return:**
        - ``extensionHeader`` -- the primary header needed for the ESO Phase III ready spectrum bintable.
    """
    ################ > IMPORTS ################
    ## STANDARD LIB ##
    import datetime
    ## THIRD PARTY ##
    import pyfits as pf
    import numpy as np
    ## LOCAL APPLICATION ##
    import dryxPython.mysql as dms
    import dryxPython.astrotools as dat

    ################ > VARIABLE SETTINGS ######
    oneDHduList = pf.open(oneDSpectrumPath)
    # READ IN THE SCIENCE DATA AND PRIMARY HEADER
    oneDFitsData = oneDHduList[0].data
    oneDHduList.close()
    dbTable = instrument + "_spectra_binary_table_extension"
    thisTable = dbTable.replace("spectra", "spectrum")
    basename = os.path.basename(oneDSpectrumPath)

    sqlQuery = """
        SELECT mysql_keyword, fits_keyword, fits_comment from fits_header_keywords where %s = 1
    """ % (thisTable,)
    cardSyntax = dms.execute_mysql_read_query(
        sqlQuery,
        dbConn,
        log
    )
    mysqlKeywords = []
    fitsKeywords = []
    fitsComments = []
    for cs in cardSyntax:
        mysqlKeywords.append(cs["mysql_keyword"])
        fitsKeywords.append(cs["fits_keyword"])
        fitsComments.append(cs["fits_comment"])

    primaryIdKey = instrument + "_spectra_id"
    sqlQuery = """
        SELECT %s from %s where %s = %s
    """ % (",".join(mysqlKeywords), dbTable, primaryIdKey, primaryId)

    fileMeta = dms.execute_mysql_read_query(
        sqlQuery,
        dbConn,
        log
    )

    log.debug('sqlQuery to get metadata for binaryTable: %s' % (sqlQuery,))

    fileMeta = fileMeta[0]
    for k, v in fileMeta.iteritems():
        if isinstance(v, datetime.datetime):
            if k == "DATE_OBS":
                fileMeta[k] = dat.getDateFromMJD(
                    fileMeta["MJD_OBS"]
                )
            fileMeta[k] = str(v).replace(" ", "T")
        if v == "T":
            fileMeta[k] = True
        if v == "F":
            fileMeta[k] = False
        if k == "INHERIT":
            fileMeta[k] = True

    # GENERATE THE FITS HEADER
    extHdu = pf.PrimaryHDU()
    cardList = extHdu.header.ascardlist()
    del cardList["SIMPLE"]
    del cardList["EXTEND"]
    index = -1
    for m, f, c in zip(mysqlKeywords, fitsKeywords, fitsComments):
        index += 1
        if m == "RA" and fileMeta[m]:
            raSex = dat.ra_to_sex(
                ra=fileMeta[m],
                delimiter=':'
            )
            fitsComments[index] = raSex + " RA (J2000) pointing (deg)"
        elif m == "DECL" and fileMeta[m]:
            decSex = dat.dec_to_sex(
                dec=fileMeta[m],
                delimiter=':'
            )
            fitsComments[index] = decSex + " DEC (J2000) pointing (deg)"
        elif m == "ESO_ADA_GUID_RA" and fileMeta[m]:
            raSex = dat.ra_to_sex(
                ra=fileMeta[m],
                delimiter=':'
            )
            fitsComments[index] = raSex + " Guide Star RA J2000"
        elif m == "ESO_ADA_GUID_DEC" and fileMeta[m]:
            decSex = dat.dec_to_sex(
                dec=fileMeta[m],
                delimiter=':'
            )
            fitsComments[index] = decSex + " Guide Star DEC J2000"
        elif m == "ESO_TEL_MOON_RA" and fileMeta[m]:
            raSex = dat.ra_to_sex(
                ra=fileMeta[m],
                delimiter=':'
            )
            fitsComments[index] = raSex + " RA (J2000) (deg)"
        elif m == "ESO_TEL_MOON_DEC" and fileMeta[m]:
            decSex = dat.dec_to_sex(
                dec=fileMeta[m],
                delimiter=':'
            )
            fitsComments[index] = decSex + " DEC (J2000) (deg)"

    for m, f, c in zip(mysqlKeywords, fitsKeywords, fitsComments):
        if m in ["BITPIX", "NAXIS"]:
            cardList[str(f)].comment = str(c)
            cardList[str(f)].value = fileMeta[m]
        else:
            card = pf.Card(str(f), fileMeta[m], str(c))
            cardList.append(card)

    # NAXIS1
    thisCard = pf.Card('NAXIS1', pixelCount * 4 * 4, 'Length of data axis 1')
    cardList.insert(3, thisCard)
    log.debug('NAXIS1: %s' % (pixelCount * 4 * 4,))
    # NELEM
    nelem = len(oneDFitsData[1, 0])
    log.debug('nelem: %s' % (nelem,))
    thisCard = pf.Card('NELEM', nelem, 'Length of the data arrays')
    cardList.append(thisCard)
    # TFORM1
    thisCard = pf.Card('TFORM1', '%sE' %
                       (pixelCount,), 'Data format of field 1')
    cardList.append(thisCard)
    log.debug('TFORM1: %s' % (pixelCount,))
    # TFORM2
    thisCard = pf.Card('TFORM2', '%sE' %
                       (pixelCount,), 'Data format of field 2')
    cardList.append(thisCard)
    # TFORM3
    thisCard = pf.Card('TFORM3', '%sE' %
                       (pixelCount,), 'Data format of field 3')
    cardList.append(thisCard)
    # TFORM4
    thisCard = pf.Card('TFORM4', '%sE' %
                       (pixelCount,), 'Data format of field 4')
    cardList.append(thisCard)
    # TFORM5
    # thisCard = pf.Card('TFORM5', '%sE' % (pixelCount,), 'Data format of field 5')
    # cardList.append(thisCard)
    # TFORM6
    # thisCard = pf.Card('TFORM6', '%sE' % (pixelCount,), 'Data format of field 6')
    # cardList.append(thisCard)

    log.debug('cardList for EXTENSION BINARY table for file %s\n: %s' %
              (basename, cardList,))

    extensionHeader = pf.Header(cards=cardList)

    return extensionHeader


# LAST MODIFIED : September 9, 2013
# CREATED : September 9, 2013
# AUTHOR : DRYX
def rewrite_fits_file(
        dbConn,
        log,
        instrument,
        primaryId,
        pathToFitsFile,
        fitsHeader,
        binaryTable=False):
    """

    *rewrite fits file given the path to the fits file and the new header*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``instrument`` -- efosc or sofi
        - ``primaryId`` -- the primaryId of the file in the db table
        - ``pathToFitsFile`` -- path to the fits file to rewrite
        - ``fitsHeader`` -- the primary header to write into the fits file
        - ``binaryTable`` -- if binary table generate data and 2 headers

    **Return:**
        - None

    .. todo::


    """
    ################ > IMPORTS ################
    ## STANDARD LIB ##
    import os
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##
    import pyfits as pf
    import dryxPython.mysql as dms

    log.debug('starting the ``rewrite_fits_file`` function')
    # TEST THE ARGUMENTS

    ## VARIABLES ##
    if binaryTable:
        log.debug('found a binarytable: %s' % (pathToFitsFile,))
        dbTable = instrument + "_spectra"
        sqlQuery = """
            select primaryId, archivePath, currentFilepath, flux_scaling_factor from %s where primaryId = (select binary_table_associated_spectrum_id from %s where primaryId = %s);
        """ % (dbTable, dbTable, primaryId,)
        oneDSpectrumQuery = dms.execute_mysql_read_query(
            sqlQuery,
            dbConn,
            log
        )

        log.debug(
            'sqlQuery to find the binary_table_associated_spectrum_id: %s' %
            (sqlQuery,))

        oneDSpectrum = oneDSpectrumQuery[0]["currentFilepath"]
        oneDSpectrumPrimaryId = oneDSpectrumQuery[0]["primaryId"]

        # FOR TESTS
        # if oneDSpectrum == "/tmp/marshall_archive/2013/jan/sofi/spectra/reduced/SN2012ec_20130113_GB_merge_56478_1_sc.fits" or oneDSpectrum == "/misc/pessto/data/ntt_data/archive//2013/jan/sofi/spectra/reduced/SN2012ec_20130113_GB_merge_56478_1_sc.fits":
        #     oneDSpectrum = "/Users/Dave/Dropbox/github_projects/pessto_marshall_engine/pessto_marshall_engine/tests/input/data/fits_files_for_rewrite_tests/SN2012ec_20130113_GB_merge_56478_1_sc.fits"

        binTableHdu, pixelCount = build_bintable_data(
            log, pathToFitsFile=oneDSpectrum)
        binPrimHdu = pf.PrimaryHDU(header=fitsHeader, data=None)
        binExtHeader = generate_bintable_extension_header(
            log, dbConn, pixelCount, primaryId, instrument, oneDSpectrum)
        binTableHdu.header = binExtHeader
        binTableHdu.verify('fix')
        hduList = pf.HDUList([binPrimHdu, binTableHdu])
        basename = os.path.basename(pathToFitsFile)
        # newPath = oneDSpectrumQuery[0]["currentFilepath"]+"/"+basename
        # try:
        #     os.remove(pathToFitsFile)
        # except:
        #     pass
        # tableHduList.writeto(pathToOutputFile, checksum=True)
        # hduList.writeto(pathToFitsFile, clobber=True, checksum=True)
    else:
        print pathToFitsFile
        hduList = pf.open(pathToFitsFile)
        fitsData = hduList[0].data
        hduList.close()

        primHdu = pf.PrimaryHDU(
            data=fitsData,
            header=fitsHeader
        )
        primHdu.verify('fix')
        hduList = pf.HDUList([primHdu])

    try:
        os.remove(pathToFitsFile)
    except:
        pass
    hduList.writeto(pathToFitsFile, checksum=True, clobber=True)
    log.debug('Have rewritten this file: %s' % (pathToFitsFile,))

    log.debug('completed the ``rewrite_fits_file`` function')
    return None


# LAST MODIFIED : September 9, 2013
# CREATED : April 9, 2013
# AUTHOR : DRYX
def build_bintable_data(log, pathToFitsFile):
    """
    *Build the binary table data extensions of the final reduced spectrum*

    **Key Arguments:**
        - ``log`` -- logger
        - ``pathToFitsFile`` -- path to the final NTT Pipeline reduced spectrum.

    **Return:**
        - ``binTableHdu`` -- the binary table HDU
    """
    ################ > IMPORTS ################
    ## STANDARD LIB ##
    ## THIRD PARTY ##
    import numpy as np
    import pyfits as pf
    ## LOCAL APPLICATION ##

    ################ > VARIABLE SETTINGS ######

    ################ >ACTION(S) ################
    # OPEN THE FILE
    hduList = pf.open(pathToFitsFile)
    # READ IN THE SCIENCE DATA AND PRIMARY HEADER
    fluxData = hduList[0].data[0]
    # uwFluxData = hduList[0].data[1]
    backgroundData = hduList[0].data[2]
    sigmaData = hduList[0].data[3]
    primHeader = hduList[0].header
    log.debug('counting the pixels in file pathToFitsFile: %s' %
              (pathToFitsFile,))
    pixelCount = len(fluxData[0])
    log.debug('pixelCount %s' % (pixelCount,))

    # DETERMINE IF SOFI OR EFOSC
    voUCD = 'em.IR'
    try:
        instr = primCardList['HIERARCH ESO INS GRIS1 NAME'].key
        voUCD = 'em.opt'
    except:
        pass

    # log.debug('fluxData %s' % (len(fluxData[0]),))
    # log.debug('uwFluxData %s' % (len(uwFluxData[0]),))
    # log.debug('backgroundData %s' % (len(backgroundData[0]),))
    # log.debug('sigmaData %s' % (len(sigmaData[0]),))

    ########## BUILD THE PRIMARY HEADER FOR BINTABLE ##########
    # READ THE NEEDED KEYWORDS & VALUES
    primCardList = primHeader.ascardlist()
    # DETERMINE PIXEL WAVELENTH RESOLUTION
    minWave = primCardList['WAVELMIN'].value * 10.
    maxWave = primCardList['WAVELMAX'].value * 10.
    rangeWave = maxWave - minWave
    pixelGaps = len(fluxData[0]) - 1
    delPixel = rangeWave / pixelGaps
    fluxError = primCardList['SPEC_ERR'].value

    wlArray = []
    for item in range(len(fluxData[0])):
        wlArray.append(item * delPixel + minWave)

    # BINTABLE COLUMNS : wl in Angstrom, flux in erg cm**(-2) s**(-1)
    # angstrom**(-1)
    wlArray = np.array([wlArray[:]], dtype=object)
    fluxArray = np.array([fluxData[0]], dtype=object)
    # uwFluxDataArray = np.array([uwFluxData[0]], dtype=object)
    backgroundDataArray = np.array([backgroundData[0]], dtype=object)
    # fluxErrArray = np.array([np.sqrt(varianceData[0])])
    fluxErrArray = np.array([sigmaData[0]], dtype=object)
    # uwFluxErrArray = np.array([sigmaData[0]], dtype=object)

    log.debug('wlArray %s' % (wlArray,))
    log.debug('fluxArray %s' % (fluxArray,))
    log.debug('fluxErrArray %s' % (fluxErrArray,))
    # log.debug('uwFluxDataArray %s' % (uwFluxDataArray,))
    # log.debug('uwFluxErrArray %s' % (uwFluxErrArray,))
    log.debug('backgroundDataArray %s' % (backgroundDataArray,))

    format = '%sE' % (pixelCount,)
    log.debug('format %s' % (format,))

    wlCol = pf.Column(name='wavelength',
                      format='%sE' % (pixelCount,), unit='angstrom', array=wlArray)
    fluxCol = pf.Column(name='flux',
                        format='%sE' % (pixelCount,), unit='erg cm**(-2) s**(-1) angstrom**(-1)', array=fluxArray)
    fluxErrCol = pf.Column(name='fluxerr',
                           format='%sE' % (pixelCount,), unit='erg cm**(-2) s**(-1) angstrom**(-1)', array=fluxErrArray)
    # uwFluxCol = pf.Column(name='flux_unweighted', format='%sE' % (pixelCount,), unit='erg cm**(-2) s**(-1) angstrom**(-1)', array=uwFluxDataArray)
    # uwFluxErrCol = pf.Column(name='flux_unweighted_error', format='%sE' % (pixelCount,), unit='erg cm**(-2) s**(-1) angstrom**(-1)', array=uwFluxErrArray)
    backgroundCol = pf.Column(name='sky_background',
                              format='%sE' % (pixelCount,), unit='erg cm**(-2) s**(-1) angstrom**(-1)', array=backgroundDataArray)
    # coldefs = pf.ColDefs([wlCol, fluxCol, fluxErrCol, uwFluxCol, uwFluxErrCol, backgroundCol])
    coldefs = pf.ColDefs([wlCol, fluxCol, fluxErrCol, backgroundCol])
    binTableHdu = pf.new_table(coldefs)

    hduList.close()

    return binTableHdu, pixelCount

###################################################################
# PRIVATE (HELPER) FUNCTIONS                                      #
###################################################################

############################################
# CODE TO BE DEPECIATED                    #
############################################

if __name__ == '__main__':
    main()


###################################################################
# TEMPLATE FUNCTIONS                                              #
###################################################################
