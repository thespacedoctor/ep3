## UPDATE SOFI WEIGHT ASSOCIATIONS
UPDATE sofi_imaging SET ASSON1 = replace(currentFilename,'.fits','.weight.fits'), ASSOC1 = "ANCILLARY.WEIGHTMAP", esoPhaseIII = 1 WHERE filetype_key_calibration = 13 and currentFilename like "%merge%";
UPDATE sofi_imaging SET ASSON1 = replace(currentFilename,'.fits','.weight.fits'), ASSOC1 = "ANCILLARY.WEIGHTMAP", esoPhaseIII = 1 WHERE filetype_key_calibration = 8 and currentFilename like "%merge%";
UPDATE sofi_imaging SET esoPhaseIII = 1 where filetype_key_calibration = 11 and currentFilename in (SELECT * FROM (SELECT replace(currentFilename,'.fits','.weight.fits') FROM sofi_imaging where filetype_key_calibration = 13 and currentFilename like "%merge%") as alias);
UPDATE sofi_imaging SET esoPhaseIII = 1 where filetype_key_calibration = 11 and currentFilename in (SELECT * FROM (SELECT replace(currentFilename,'.fits','.weight.fits') FROM sofi_imaging where filetype_key_calibration = 8 and currentFilename like "%merge%") as alias);
UPDATE sofi_imaging  SET ASSON1 = NULL, ASSOC1 = NULL where filetype_key_calibration = 11;


## UPDATE EFOSC SPECTRA
UPDATE efosc_spectra SET ASSON1 = SUBSTRING(replace(currentFilename,'_e.fits','_si.fits'),2), ASSOC1 = "ANCILLARY.2DSPECTRUM", esoPhaseIII = 1 WHERE filetype_key_calibration = 13 and filename like "%_e.fits" and currentFilename not like "%_si%";
UPDATE efosc_spectra SET ASSON1 = NULL, ASSOC1 = NULL where currentFilename like "%_si%";
UPDATE efosc_spectra SET esoPhaseIII = 1 where filetype_key_calibration = 13 and filename in (SELECT * FROM (SELECT replace(filename,'_e.fits','_2df.fits') FROM efosc_spectra where filetype_key_calibration = 13 and filename like "%_e.fits") as alias);
UPDATE efosc_spectra SET esoPhaseIII = 1 where filetype_key_calibration = 13 and esoPhaseIII = 0 and filename in (SELECT * FROM (SELECT replace(filename,'_f.fits','_2df.fits') FROM efosc_spectra where filetype_key_calibration = 13 and filename like "%_f.fits") as alias);



## UPDATE EFOSC IMAGES
UPDATE efosc_imaging SET esoPhaseIII = 1 where filetype_key_calibration = 13 and dryx_comment is null and do_not_release = 0;
UPDATE efosc_imaging SET esoPhaseIII = 1 where filetype_key_calibration = 10 and filetype_key_reduction_stage = 3 and transientBucketId != 0 and dryx_comment is null and do_not_release = 0;
update efosc_imaging set esoPhaseIII = 1 where filetype_key_calibration = 8 and filetype_key_reduction_stage = 2 and filename not like "EFOSC.%" and dryx_comment is null and do_not_release = 0;
update efosc_imaging set esoPhaseIII = 0 where filetype_key_calibration = 8 and filetype_key_reduction_stage = 4;
update efosc_imaging set esoPhaseIII = 0 where esoPhaseIII = 1 and currentFilename in (SELECT * FROM (SELECT replace(currentFilename,'_fr.fits','.fits') from efosc_imaging where currentFilename like "%\_fr.fits") as alias);
update efosc_imaging set data_rel = null where data_rel is not null and currentFilename in (SELECT * FROM (SELECT replace(currentFilename,'_fr.fits','.fits') from efosc_imaging where currentFilename like "%\_fr.fits") as alias);
update efosc_imaging set esoPhaseIII = 0,  data_rel = null where dryx_comment is not null;

## UPDATE SOFI SPECTRA
UPDATE sofi_spectra SET ASSON1 = SUBSTRING(replace(currentFilename,'_sc.fits','_si.fits'),1), ASSOC1 = "ANCILLARY.2DSPECTRUM", esoPhaseIII = 1 WHERE filetype_key_calibration = 13 and filename like "%_sc.fits" and currentFilename not like "%_si%";
UPDATE sofi_spectra SET ASSON1 = SUBSTRING(replace(currentFilename,'_f.fits','_si.fits'),1), ASSOC1 = "ANCILLARY.2DSPECTRUM", esoPhaseIII = 1 where filename like "%\_f.fits" and filetype_key_calibration = 13 and filename not in (SELECT * FROM (SELECT replace(filename,'_sc.fits','_f.fits') from sofi_spectra where filetype_key_calibration = 13 and filename like "%\_sc.fits") as alias);
UPDATE sofi_spectra SET ASSON1 = NULL, ASSOC1 = NULL where currentFilename like "%_si%";
UPDATE sofi_spectra SET esoPhaseIII = 1 where filetype_key_calibration = 13 and filename in (SELECT * FROM (SELECT replace(replace(filename,'_f.fits','_2df.fits'),'_sc.fits','_2df.fits') from sofi_spectra where filetype_key_calibration = 13 and (filename like '%_sc.fits' or filename like '%_f.fits')) as alias);


## UPDATE OLDER FILE ASSOCIATIONS (IN CASE OF OBJECT NAME UPDATES)
UPDATE sofi_imaging SET ASSON1 = replace(currentFilename,'.fits','.weight.fits'), ASSOC1 = "ANCILLARY.WEIGHTMAP" where esoPhaseIII = 1 and currentFilename not like "%weight%";
UPDATE sofi_spectra SET ASSON1 = replace(currentFilename,'.fits','_si.fits'), ASSOC1 = "ANCILLARY.2DSPECTRUM"  where  esoPhaseIII = 1 and filename not like "%_sb.fits";
UPDATE sofi_spectra SET ASSON1 = replace(currentFilename,'_sb.fits','_si.fits'), ASSOC1 = "ANCILLARY.2DSPECTRUM"  where  esoPhaseIII = 1 and filename like "%_sb.fits";
UPDATE efosc_spectra SET ASSON1 = replace(currentFilename,'.fits','_si.fits'), ASSOC1 = "ANCILLARY.2DSPECTRUM"  where  esoPhaseIII = 1 and filename not like "%_sb.fits";
UPDATE efosc_spectra SET ASSON1 = replace(currentFilename,'_sb.fits','_si.fits'), ASSOC1 = "ANCILLARY.2DSPECTRUM"  where  esoPhaseIII = 1 and filename like "%_sb.fits";

## ADD IN SSDR VERSION
UPDATE sofi_imaging SET `DATA_REL` = "SSDR1" where esoPhaseIII = 1 and date_obs < "2013-05-01" and do_not_release = 0;
UPDATE efosc_imaging SET `DATA_REL` = "SSDR1" where esoPhaseIII = 1 and date_obs < "2013-05-01" and do_not_release = 0;
UPDATE efosc_spectra SET `DATA_REL` = "SSDR1" where esoPhaseIII = 1 and date_obs < "2013-05-01" and (currentFilename like "%_sb.fits" or currentFilename like "%_si.fits") and do_not_release = 0;
UPDATE sofi_spectra SET `DATA_REL` = "SSDR1" where esoPhaseIII = 1 and date_obs < "2013-05-01" and (currentFilename like "%_sb.fits" or currentFilename like "%_si.fits") and do_not_release = 0;

UPDATE sofi_imaging SET `DATA_REL` = "SSDR2" where esoPhaseIII = 1 and date_obs > "2013-06-01" and date_obs < "2014-05-08" and do_not_release = 0;
UPDATE efosc_imaging SET `DATA_REL` = "SSDR2" where esoPhaseIII = 1 and date_obs > "2013-06-01" and date_obs < "2014-05-08" and do_not_release = 0;
UPDATE efosc_spectra SET `DATA_REL` = "SSDR2" where esoPhaseIII = 1 and date_obs > "2013-06-01" and date_obs < "2014-05-08" and (currentFilename like "%_sb.fits" or currentFilename like "%_si.fits") and do_not_release = 0;
UPDATE sofi_spectra SET `DATA_REL` = "SSDR2" where esoPhaseIII = 1 and date_obs > "2013-06-01" and date_obs < "2014-05-08" and (currentFilename like "%_sb.fits" or currentFilename like "%_si.fits") and do_not_release = 0;


UPDATE sofi_imaging SET `DATA_REL` = "SSDR3" where esoPhaseIII = 1 and date_obs > "2014-05-08" and date_obs < "2016-06-01" and do_not_release = 0;
UPDATE efosc_imaging SET `DATA_REL` = "SSDR3" where esoPhaseIII = 1 and date_obs > "2014-05-08" and date_obs < "2016-06-01" and do_not_release = 0;
UPDATE efosc_spectra SET `DATA_REL` = "SSDR3" where esoPhaseIII = 1 and date_obs > "2014-05-08" and date_obs < "2016-06-01" and (currentFilename like "%_sb.fits" or currentFilename like "%_si.fits") and do_not_release = 0;
UPDATE sofi_spectra SET `DATA_REL` = "SSDR3" where esoPhaseIII = 1 and date_obs > "2014-05-08" and date_obs < "2016-06-01" and (currentFilename like "%_sb.fits" or currentFilename like "%_si.fits") and do_not_release = 0;

UPDATE sofi_imaging SET  `DATA_REL` = NULL, esoPhaseIII = 0 where do_not_release = 1;
UPDATE efosc_imaging SET  `DATA_REL` = NULL, esoPhaseIII = 0 where do_not_release = 1;
UPDATE efosc_spectra SET  `DATA_REL` = NULL, esoPhaseIII = 0 where do_not_release = 1;
UPDATE sofi_spectra SET  `DATA_REL` = NULL, esoPhaseIII = 0 where do_not_release = 1;

## PRODCATG TO BE ADDED TO ALL FILES AS OF August 30, 2016
update efosc_spectra set PRODCATG = "ANCILLARY.2DSPECTRUM" where data_rel is not null and currentFilename like "%_si.fits";
update sofi_spectra set PRODCATG = "ANCILLARY.2DSPECTRUM" where data_rel is not null and currentFilename like "%_si.fits";
update sofi_imaging set PRODCATG = "ANCILLARY.WEIGHTMAP" where data_rel is not null and currentFilename like "%weight%";


-- # TITLE
-- update sofi_spectra set title = object where data_rel is not null and lock_row = 0;
-- update efosc_spectra set title = object where data_rel is not null and lock_row = 0;
-- update sofi_imaging set title = object where data_rel is not null and lock_row = 0;

## OTHER FIXES
update  efosc_spectra set SPECSYS = "TOPOCENT" where  SPECSYS is null and DATA_REL is not null;
update efosc_spectra set DISPELEM = ESO_INS_GRIS1_NAME where  data_rel is not null and DISPELEM is null and ESO_INS_GRIS1_NAME is not null;
