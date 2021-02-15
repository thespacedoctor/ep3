

def update_sofi_mjd_keywords(
        dbConn,
        log):
    """
    *update sofi image mjd keywords -- correcting errors in pipeline output*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger


    **Return**

    - None


    .. todo::
    """
    ## STANDARD LIB ##
    import re
    ## THIRD PARTY ##
    ## LOCAL APPLICATION ##
    from fundamentals.mysql import readquery, writequery

    log.debug('starting the ``update_sofi_mjd_keywords`` function')
    # TEST THE ARGUMENTS

    fileType = ["sofi_imaging", "sofi_spectra"]

    for ft in fileType:
        ## VARIABLES ##
        # Get the required values for the sofi spectra to update mjd keyword
        # values
        if ft == "sofi_spectra":
            sqlQuery = """
                select tmid, mjd_obs,ndit, primaryId ,filename, ncombine, prov1,prov2,prov3,prov4,prov5,prov6,prov7,prov8,prov9,prov10,prov11,prov12,prov13,prov14,prov15,prov16 from %(ft)s where esophaseIII = 1  and lock_row = 0
            """ % locals()
        else:
            sqlQuery = """
                select tmid, mjd_obs,ndit, primaryId ,filename, ncombine, prov1,prov2,prov3,prov4,prov5,prov6,prov7,prov8,prov9,prov10,prov11,prov12,prov13,prov14,prov15,prov16,prov17,prov18,prov19,prov20,prov21,prov22,prov23,prov24,prov25,prov26,prov27,prov28,prov29,prov30,prov31,prov32,prov33,prov34,prov35,prov36,prov37,prov38,prov39,prov40,prov41,prov42,prov43,prov44,prov45,prov46,prov47,prov48,prov49,prov50,prov51,prov52,prov53,prov54,prov55,prov56,prov57,prov58,prov59,prov60 from %(ft)s where esophaseIII = 1  and lock_row = 0
            """ % locals()

        rows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=dbConn
        )

        for row in rows:
            if ft == "sofi_imaging":
                for i in range(1, 61):
                    if row["prov%(i)s" % locals()] is None:
                        continue
                    else:
                        ncomb = i
            else:
                for i in range(1, 17):
                    if row["prov%(i)s" % locals()] is None:
                        continue
                    else:
                        ncomb = i

            print("NCOMBINE: %(ncomb)s " % locals())

            if row["ncombine"] != ncomb or row["tmid"] == None:
                primaryId = row["primaryId"]
                sqlQuery = """
                    update %(ft)s set ncombine = "%(ncomb)s" where primaryId = %(primaryId)s and lock_row = 0
                """ % locals()
                writequery(
                    log=log,
                    sqlQuery=sqlQuery,
                    dbConn=dbConn,
                )
                print("UPDATE: %(sqlQuery)s " % locals())

            # get the last file used in the combination of the final file
            provKeyword = "prov" + str(row["ncombine"])
            lastFilename = row[provKeyword]

            # get the details of this last file
            sqlQuery = """
                select mjd_obs, exptime, filename from %(ft)s  where filename = "%(lastFilename)s"
            """ % locals()

            provRows = readquery(
                log=log,
                sqlQuery=sqlQuery,
                dbConn=dbConn
            )

            # note this should only be one file
            for provRow in provRows:
                # the new mjd_end takes into account the
                mjd_end = provRow["mjd_obs"] + row["ndit"] * (
                    provRow["exptime"] + 1.8) / (60. * 60. * 24.)
                telapse = (mjd_end - row["mjd_obs"]) * (60. * 60. * 24.)
                tmid = old_div((mjd_end + row["mjd_obs"]), 2.)

                primaryId = row["primaryId"]
                if "spectra" in ft:
                    sqlQuery = """
                        update %(ft)s  set mjd_end = %(mjd_end)s, telapse = %(telapse)s, tmid = %(tmid)s where primaryId = %(primaryId)s and lock_row = 0
                    """ % locals()
                else:
                    sqlQuery = """
                        update %(ft)s  set mjd_end = %(mjd_end)s, tmid = %(tmid)s where primaryId = %(primaryId)s and lock_row = 0
                    """ % locals()

                writequery(
                    sqlQuery=sqlQuery,
                    dbConn=self.dbConn,
                    log=self.log
                )

    log.debug('completed the ``update_sofi_mjd_keywords`` function')

    return None

# use the tab-trigger below for new function
# x-def-with-logger

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
