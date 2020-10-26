## CONVERT 1D EFOSC SPECTRA NAMES TO BINTABLE NAMES
DROP TEMPORARY TABLE IF EXISTS tmptable_1;
CREATE TEMPORARY TABLE tmptable_1 ENGINE=MyISAM ROW_FORMAT=COMPRESSED  KEY_BLOCK_SIZE=8 SELECT * from efosc_spectra where filename like "%\_e.fits" and filetype_key_calibration = 13 and esoPhaseIII = 1 and filename not in (SELECT * from (SELECT replace(filename,'_sb.fits','_e.fits') from efosc_spectra where filename like "%\_sb.fits") as alias) ;
UPDATE tmptable_1 SET filename = replace(filename,'_e.fits','_sb.fits'), currentFilename = replace(currentFilename,'_e.fits','_sb.fits'), updatedFilename = NULL, currentFilepath = replace(currentFilepath,'_e.fits','_sb.fits'), updatedFilepath = NULL, esoPhaseIII = 1, filepath = Null, esoPhaseIII = 1;
UPDATE tmptable_1 SET binary_table_associated_spectrum_id = primaryId;
UPDATE tmptable_1 SET primaryId = Null;
INSERT INTO efosc_spectra SELECT * FROM tmptable_1;
DROP TEMPORARY TABLE IF EXISTS tmptable_1;

## CONVERT 1D EFOSC SPECTRA NAMES TO BINTABLE NAMES -- SPECTROPHOT STD FLUX CAL
CREATE TEMPORARY TABLE tmptable_1 ENGINE=MyISAM ROW_FORMAT=COMPRESSED  KEY_BLOCK_SIZE=8 SELECT * from sofi_spectra where (filename like "%\_sc.fits")  and filetype_key_calibration = 13 and esoPhaseIII = 1 and filename not in (SELECT * from (SELECT replace(filename,'_sb.fits','_sc.fits') from sofi_spectra where filename like "%\_sb.fits") as alias);
UPDATE tmptable_1 SET filename = replace(filename,'_sc.fits','_sb.fits'), currentFilename = replace(currentFilename,'_sc.fits','_sb.fits'), updatedFilename = NULL,  currentFilepath = replace(currentFilepath,'_sc.fits','_sb.fits'), updatedFilepath = NULL, esoPhaseIII = 1, filepath = Null, esoPhaseIII = 1;
UPDATE tmptable_1 SET binary_table_associated_spectrum_id = primaryId;
UPDATE tmptable_1 SET primaryId = Null;
INSERT INTO sofi_spectra SELECT * FROM tmptable_1;
DROP TEMPORARY TABLE IF EXISTS tmptable_1;

## CONVERT 1D EFOSC SPECTRA NAMES TO BINTABLE NAMES -- TELURIC STD FLUX CAL ONLY
CREATE TEMPORARY TABLE tmptable_1 ENGINE=MyISAM ROW_FORMAT=COMPRESSED  KEY_BLOCK_SIZE=8 SELECT * from sofi_spectra where (filename like "%\_f.fits")  and filetype_key_calibration = 13 and esoPhaseIII = 1 and filename not in (SELECT * from (SELECT replace(filename,'_sb.fits','_f.fits') from sofi_spectra where filename like "%\_sb.fits") as alias);
UPDATE tmptable_1 SET filename = replace(filename,'_f.fits','_sb.fits'), currentFilename = replace(currentFilename,'_f.fits','_sb.fits'), updatedFilename = NULL, currentFilepath = replace(currentFilepath,'_f.fits','_sb.fits'), updatedFilepath = NULL, esoPhaseIII = 1, filepath = Null, esoPhaseIII = 1;
UPDATE tmptable_1 SET binary_table_associated_spectrum_id = primaryId;
UPDATE tmptable_1 SET primaryId = Null;
INSERT INTO sofi_spectra SELECT * FROM tmptable_1;
DROP TEMPORARY TABLE IF EXISTS tmptable_1;

## FIX KEYWORDS IN DUPLICATED BINARY TABLE ROWS
update sofi_spectra set NAXIS = 0 where filetype_key_calibration = 13 and filename like "%\_sb.fits%" and NAXIS != 0;
update efosc_spectra set NAXIS = 0 where filetype_key_calibration = 13 and filename like "%\_sb.fits%" and NAXIS != 0;
update sofi_spectra set BITPIX = 8 where filetype_key_calibration = 13 and filename like "%\_sb.fits%" and BITPIX != 8;
update efosc_spectra set BITPIX = 8 where filetype_key_calibration = 13 and filename like "%\_sb.fits%" and BITPIX != 8;
update sofi_spectra set esoPhaseIII = 1 where filetype_key_calibration = 13 and filename like "%\_sb.fits%" and esoPhaseIII = 0;
update efosc_spectra set esoPhaseIII = 1 where filetype_key_calibration = 13 and filename like "%\_sb.fits%" and esoPhaseIII = 0;


update efosc_spectra_binary_table_extension set SIMPLE = True where SIMPLE != True or SIMPLE is NULL;
update efosc_spectra_binary_table_extension b set TELAPSE = (SELECT TELAPSE from efosc_spectra s where s.primaryId=b.efosc_spectra_id);
update efosc_spectra_binary_table_extension b set OBJECT = (SELECT OBJECT from efosc_spectra s where s.primaryId=b.efosc_spectra_id);
update efosc_spectra_binary_table_extension b set TMID = (SELECT TMID from efosc_spectra s where s.primaryId=b.efosc_spectra_id);
update efosc_spectra_binary_table_extension b set TDMIN1 = (SELECT WAVELMIN from efosc_spectra s where s.primaryId=b.efosc_spectra_id)*10.;
update efosc_spectra_binary_table_extension b set TDMAX1 = (SELECT WAVELMAX from efosc_spectra s where s.primaryId=b.efosc_spectra_id)*10.;
update efosc_spectra_binary_table_extension b set TITLE = (SELECT OBJECT from efosc_spectra s where s.primaryId=b.efosc_spectra_id);
update efosc_spectra_binary_table_extension b set APERTURE = (SELECT APERTURE from efosc_spectra s where s.primaryId=b.efosc_spectra_id);
update efosc_spectra_binary_table_extension b set DECL = (SELECT DECL from efosc_spectra s where s.primaryId=b.efosc_spectra_id);
update efosc_spectra_binary_table_extension b set GCOUNT = (SELECT GCOUNT from efosc_spectra s where s.primaryId=b.efosc_spectra_id);
update efosc_spectra_binary_table_extension b set PCOUNT = (SELECT PCOUNT from efosc_spectra s where s.primaryId=b.efosc_spectra_id);
update efosc_spectra_binary_table_extension b set RA = (SELECT RA from efosc_spectra s where s.primaryId=b.efosc_spectra_id);
update efosc_spectra_binary_table_extension b set SPEC_BW = (SELECT SPEC_BW from efosc_spectra s where s.primaryId=b.efosc_spectra_id);
update efosc_spectra_binary_table_extension b set SPEC_VAL = (SELECT SPEC_VAL from efosc_spectra s where s.primaryId=b.efosc_spectra_id);
update efosc_spectra_binary_table_extension set BITPIX = 8 where BITPIX != 8 or BITPIX is NULL;
update efosc_spectra_binary_table_extension set TFIELDS = 4 where TFIELDS != 4 or TFIELDS is NULL;
update efosc_spectra_binary_table_extension set TTYPE1 = "WAVE" where TTYPE1 != "WAVE" or TTYPE1 is NULL;
update efosc_spectra_binary_table_extension set TUTYP1 = "Spectrum.Data.SpectralAxis.Value" where TUTYP1 != "Spectrum.Data.SpectralAxis.Value" or TUTYP1 is NULL;
update efosc_spectra_binary_table_extension set TUNIT1 = "angstrom" where TUNIT1 != "angstrom" or TUNIT1 is NULL;
update efosc_spectra_binary_table_extension set TUCD1 = "em.wl" where TUCD1 != "em.wl" or TUCD1 is NULL;
update efosc_spectra_binary_table_extension set TTYPE2 = "FLUX" where TTYPE2 != "FLUX" or TTYPE2 is NULL;
update efosc_spectra_binary_table_extension set TUTYP2 = "Spectrum.Data.FluxAxis.Value" where TUTYP2 != "Spectrum.Data.FluxAxis.Value" or TUTYP2 is NULL;
update efosc_spectra_binary_table_extension set TUNIT2 = "erg cm**(-2) s**(-1) angstrom**(-1)" where TUNIT2 != "erg cm**(-2) s**(-1) angstrom**(-1)" or TUNIT2 is NULL;
update efosc_spectra_binary_table_extension set TUCD2 = "phot.flux.density;em.wl;src.net;meta.main" where TUCD2 != "phot.flux.density;em.wl;src.net;meta.main" or TUCD2 is NULL;
update efosc_spectra_binary_table_extension set TTYPE3 = "ERR" where TTYPE3 != "ERR" or TTYPE3 is NULL;
update efosc_spectra_binary_table_extension set TUTYP3 = "Spectrum.Data.FluxAxis.Accuracy.StatError" where TUTYP3 != "Spectrum.Data.FluxAxis.Accuracy.StatError" or TUTYP3 is NULL;
update efosc_spectra_binary_table_extension set TUNIT3 = "erg cm**(-2) s**(-1) angstrom**(-1)" where TUNIT3 != "erg cm**(-2) s**(-1) angstrom**(-1)" or TUNIT3 is NULL;
update efosc_spectra_binary_table_extension set TUCD3 = "stat.error;phot.flux.density;meta.main" where TUCD3 != "stat.error;phot.flux.density;meta.main" or TUCD3 is NULL;
update efosc_spectra_binary_table_extension set TTYPE4 = "SKYBACK" where TTYPE4 != "SKYBACK" or TTYPE4 is NULL;
update efosc_spectra_binary_table_extension set TUTYP4 = "Spectrum.Data.BackgroundModel.Value" where TUTYP4 != "Spectrum.Data.BackgroundModel.Value" or TUTYP4 is NULL;
update efosc_spectra_binary_table_extension set TUNIT4 = "erg cm**(-2) s**(-1) angstrom**(-1)" where TUNIT4 != "erg cm**(-2) s**(-1) angstrom**(-1)" or TUNIT4 is NULL;
update efosc_spectra_binary_table_extension set TUCD4 = "phot.flux.density;em.wl" where TUCD4 != "phot.flux.density;em.wl" or TUCD4 is NULL;
update efosc_spectra_binary_table_extension set EXTNAME = "PHASE3BINTABLE" where EXTNAME != "PHASE3BINTABLE" or EXTNAME is NULL;
update efosc_spectra_binary_table_extension set INHERIT = True where INHERIT != True or INHERIT is NULL;
update efosc_spectra_binary_table_extension set INHERIT = True where INHERIT != True or INHERIT is NULL;
update efosc_spectra_binary_table_extension set XTENSION = "BINTABLE" where XTENSION != "BINTABLE" or XTENSION is NULL;
update efosc_spectra_binary_table_extension set NAXIS = 2 where NAXIS != 2 or NAXIS is NULL;
update efosc_spectra_binary_table_extension set NAXIS2 = 1 where NAXIS2 != 1 or NAXIS2 is NULL;
update efosc_spectra_binary_table_extension set PCOUNT = 0 where PCOUNT != 0 or PCOUNT is NULL;
update efosc_spectra_binary_table_extension set GCOUNT = 1 where GCOUNT != 1 or GCOUNT is NULL;
update efosc_spectra_binary_table_extension set VOCLASS = "SPECTRUM V1.0" where VOCLASS != "SPECTRUM V1.0" or VOCLASS is NULL;
update efosc_spectra_binary_table_extension set VOPUB = "ESO/SAF" where VOPUB != "ESO/SAF" or VOPUB is NULL;






update sofi_spectra_binary_table_extension set SIMPLE = True where SIMPLE != True or SIMPLE is NULL;
update sofi_spectra_binary_table_extension b set TELAPSE = (SELECT TELAPSE from sofi_spectra s where s.primaryId=b.sofi_spectra_id);
update sofi_spectra_binary_table_extension b set TMID = (SELECT TMID from sofi_spectra s where s.primaryId=b.sofi_spectra_id);
update sofi_spectra_binary_table_extension b set OBJECT = (SELECT OBJECT from sofi_spectra s where s.primaryId=b.sofi_spectra_id);
update sofi_spectra_binary_table_extension b set TDMIN1 = (SELECT WAVELMIN from sofi_spectra s where s.primaryId=b.sofi_spectra_id)*10.;
update sofi_spectra_binary_table_extension b set TDMAX1 = (SELECT WAVELMAX from sofi_spectra s where s.primaryId=b.sofi_spectra_id)*10.;
update sofi_spectra_binary_table_extension b set TITLE = (SELECT OBJECT from sofi_spectra s where s.primaryId=b.sofi_spectra_id);
update sofi_spectra_binary_table_extension b set APERTURE = (SELECT APERTURE from sofi_spectra s where s.primaryId=b.sofi_spectra_id);
update sofi_spectra_binary_table_extension b set DECL = (SELECT DECL from sofi_spectra s where s.primaryId=b.sofi_spectra_id);
update sofi_spectra_binary_table_extension b set GCOUNT = (SELECT GCOUNT from sofi_spectra s where s.primaryId=b.sofi_spectra_id);
update sofi_spectra_binary_table_extension b set PCOUNT = (SELECT PCOUNT from sofi_spectra s where s.primaryId=b.sofi_spectra_id);
update sofi_spectra_binary_table_extension b set RA = (SELECT RA from sofi_spectra s where s.primaryId=b.sofi_spectra_id);
update sofi_spectra_binary_table_extension b set SPEC_BW = (SELECT SPEC_BW from sofi_spectra s where s.primaryId=b.sofi_spectra_id);
update sofi_spectra_binary_table_extension b set SPEC_VAL = (SELECT SPEC_VAL from sofi_spectra s where s.primaryId=b.sofi_spectra_id);
update sofi_spectra_binary_table_extension set BITPIX = 8 where BITPIX != 8 or BITPIX is NULL;
update sofi_spectra_binary_table_extension set TFIELDS = 4 where TFIELDS != 4 or TFIELDS is NULL;
update sofi_spectra_binary_table_extension set TTYPE1 = "WAVE" where TTYPE1 != "WAVE" or TTYPE1 is NULL;
update sofi_spectra_binary_table_extension set TUTYP1 = "Spectrum.Data.SpectralAxis.Value" where TUTYP1 != "Spectrum.Data.SpectralAxis.Value" or TUTYP1 is NULL;
update sofi_spectra_binary_table_extension set TUNIT1 = "angstrom" where TUNIT1 != "angstrom" or TUNIT1 is NULL;
update sofi_spectra_binary_table_extension set TUCD1 = "em.wl" where TUCD1 != "em.wl" or TUCD1 is NULL;
update sofi_spectra_binary_table_extension set TTYPE2 = "FLUX" where TTYPE2 != "FLUX" or TTYPE2 is NULL;
update sofi_spectra_binary_table_extension set TUTYP2 = "Spectrum.Data.FluxAxis.Value" where TUTYP2 != "Spectrum.Data.FluxAxis.Value" or TUTYP2 is NULL;
update sofi_spectra_binary_table_extension set TUNIT2 = "erg cm**(-2) s**(-1) angstrom**(-1)" where TUNIT2 != "erg cm**(-2) s**(-1) angstrom**(-1)" or TUNIT2 is NULL;
update sofi_spectra_binary_table_extension set TUCD2 = "phot.flux.density;em.wl;src.net;meta.main" where TUCD2 != "phot.flux.density;em.wl;src.net;meta.main" or TUCD2 is NULL;
update sofi_spectra_binary_table_extension set TTYPE3 = "ERR" where TTYPE3 != "ERR" or TTYPE3 is NULL;
update sofi_spectra_binary_table_extension set TUTYP3 = "Spectrum.Data.FluxAxis.Accuracy.StatError" where TUTYP3 != "Spectrum.Data.FluxAxis.Accuracy.StatError" or TUTYP3 is NULL;
update sofi_spectra_binary_table_extension set TUNIT3 = "erg cm**(-2) s**(-1) angstrom**(-1)" where TUNIT3 != "erg cm**(-2) s**(-1) angstrom**(-1)" or TUNIT3 is NULL;
update sofi_spectra_binary_table_extension set TUCD3 = "stat.error;phot.flux.density;meta.main" where TUCD3 != "stat.error;phot.flux.density;meta.main" or TUCD3 is NULL;
update sofi_spectra_binary_table_extension set TTYPE4 = "SKYBACK" where TTYPE4 != "SKYBACK" or TTYPE4 is NULL;
update sofi_spectra_binary_table_extension set TUTYP4 = "Spectrum.Data.BackgroundModel.Value" where TUTYP4 != "Spectrum.Data.BackgroundModel.Value" or TUTYP4 is NULL;
update sofi_spectra_binary_table_extension set TUNIT4 = "erg cm**(-2) s**(-1) angstrom**(-1)" where TUNIT4 != "erg cm**(-2) s**(-1) angstrom**(-1)" or TUNIT4 is NULL;
update sofi_spectra_binary_table_extension set TUCD4 = "phot.flux.density;em.wl" where TUCD4 != "phot.flux.density;em.wl" or TUCD4 is NULL;
update sofi_spectra_binary_table_extension set EXTNAME = "PHASE3BINTABLE" where EXTNAME != "PHASE3BINTABLE" or EXTNAME is NULL;
update sofi_spectra_binary_table_extension set INHERIT = True where INHERIT != True or INHERIT is NULL;
update sofi_spectra_binary_table_extension set XTENSION = "BINTABLE" where XTENSION != "BINTABLE" or XTENSION is NULL;
update sofi_spectra_binary_table_extension set NAXIS = 2 where NAXIS != 2 or NAXIS is NULL;
update sofi_spectra_binary_table_extension set NAXIS2 = 1 where NAXIS2 != 1 or NAXIS2 is NULL;
update sofi_spectra_binary_table_extension set PCOUNT = 0 where PCOUNT != 0 or PCOUNT is NULL;
update sofi_spectra_binary_table_extension set GCOUNT = 1 where GCOUNT != 1 or GCOUNT is NULL;
update sofi_spectra_binary_table_extension set VOCLASS = "SPECTRUM V1.0" where VOCLASS != "SPECTRUM V1.0" or VOCLASS is NULL;
update sofi_spectra_binary_table_extension set VOPUB = "ESO/SAF" where VOPUB != "ESO/SAF" or VOPUB is NULL;





-- CREATE TEMPORARY TABLE tmptable_1 SELECT filename, primaryId from efosc_spectra where esoPhaseIII = 1 and filename like "%\_e.fits" and filename not in (SELECT * from (SELECT replace(filename,'_sb.fits','_e.fits') from efosc_spectra_binary_table_extension where filename like "%\_sb.fits") as alias);
-- UPDATE tmptable_1 SET filename = replace(filename,'_e.fits','_sb.fits');
-- INSERT INTO sofi_spectra_binary_table_extension(filename, efosc_spectra_id) VALUES(SELECT * FROM tmptable_1);
-- DROP TEMPORARY TABLE IF EXISTS tmptable_1;

-- CREATE TEMPORARY TABLE tmptable_1 SELECT filename, primaryId from sofi_spectra where esoPhaseIII = 1 and (filename like "%\_sc.fits") and filename not in (SELECT * from (SELECT replace(filename,'_sb.fits','_sc.fits') from sofi_spectra_binary_table_extension where filename like "%\_sb.fits") as alias);
-- UPDATE tmptable_1 SET filename = replace(filename,'_sc.fits','_sb.fits');
-- UPDATE tmptable_1 SET primaryId = Null;
-- INSERT INTO sofi_spectra_binary_table_extension(filename, sofi_spectra_id) VALUES(SELECT * FROM tmptable_1);
-- DROP TEMPORARY TABLE IF EXISTS tmptable_1;

-- CREATE TEMPORARY TABLE tmptable_1 SELECT filename, primaryId from sofi_spectra where esoPhaseIII = 1 and (filename like "%\_f.fits") and filename not in (SELECT * from (SELECT replace(filename,'_sb.fits','_f.fits') from sofi_spectra_binary_table_extension where filename like "%\_sb.fits") as alias);
-- UPDATE tmptable_1 SET filename = replace(filename,'_f.fits','_sb.fits');
-- UPDATE tmptable_1 SET primaryId = Null;
-- INSERT INTO sofi_spectra_binary_table_extension(filename, sofi_spectra_id) VALUES(SELECT * FROM tmptable_1);
-- DROP TEMPORARY TABLE IF EXISTS tmptable_1;
