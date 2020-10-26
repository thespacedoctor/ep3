update sofi_imaging set DETRON = 12 where DETRON is null and  esoPhaseIII  = 1;
update efosc_spectra set PRODCATG = NULL where currentFilename like "%_si.fits";
update efosc_spectra set detron = 11.6 where detron is null and data_rel is not null;
update efosc_spectra set FLUXERR = 15.2 where FLUXERR is null and data_rel is not null;
update efosc_spectra set FLUXCAL = "ABSOLUTE" where FLUXCAL is null and data_rel is not null;
update efosc_spectra set TELAPSE = EXPTIME where TELAPSE is null and data_rel is not null;
update efosc_spectra set TEXPTIME = EXPTIME where TEXPTIME is null and data_rel is not null;
update efosc_spectra set TITLE = OBJECT where TITLE is null and data_rel is not null;
update efosc_spectra set DISPELEM = "Gr#13" where DISPELEM is null and data_rel is not null and currentFilename like "%Gr13%";
update efosc_spectra set MJD_END = MJD_OBS+exptime/(60*60*24) where MJD_END is null and data_rel is not null;
update efosc_spectra set tmid = (mjd_end+mjd_obs)/2 where tmid is null and data_rel is not null and mjd_end is not null;
update efosc_imaging set transientBucketId = -9999, object = "STD-VMA2", esoPhaseIII = 0, data_rel = null where filename like "%vma2%";
update sofi_imaging set filetype_key_calibration = 8, filetype_key_reduction_stage = 2, esoPhaseIII = 0, data_rel = null where object = "sj9108" and filetype_key_calibration = 13 and filetype_key_reduction_stage = 3;
update sofi_imaging set filetype_key_calibration = 8, filetype_key_reduction_stage = 2, esoPhaseIII = 0, data_rel = null where object = "ILL_S264-D" and filetype_key_calibration = 13 and filetype_key_reduction_stage = 3;
update sofi_imaging set filetype_key_calibration = 8, filetype_key_reduction_stage = 2, esoPhaseIII = 0, data_rel = null where object = "STD" and filetype_key_calibration = 13 and filetype_key_reduction_stage = 3;
update sofi_spectra set filetype_key_calibration = 8, filetype_key_reduction_stage = 2, esoPhaseIII = 0, data_rel = null where object = "LTT7987" and filetype_key_calibration = 13 and filetype_key_reduction_stage = 3;
update sofi_imaging set esoPhaseIII = 0, data_rel = null where object = "sj9108";
update sofi_imaging set esoPhaseIII = 0, data_rel = null where object = "ILL_S264-D";
update sofi_imaging set esoPhaseIII = 0, data_rel = null where object = "STD";
update sofi_imaging set transientBucketId = 1475 where object = "SN2012dy" and transientBucketId = 0;
update efosc_spectra set spec_sye = 0.01 where data_rel = "ssdr2";
update sofi_spectra set spec_sye = 0.01 where data_rel = "ssdr2";


update efosc_spectra set transientBucketId = 45354, Object = "SN2013fc" where object like "%PESSTOESO154%" and (filename like "%2013fc%" or filename like "%PSNJ02450896-5544273%");
update efosc_imaging set transientBucketId = 45354, Object = "SN2013fc" where object like "%PESSTOESO154%" and (filename like "%2013fc%" or filename like "%PSNJ02450896-5544273%");
update sofi_imaging set transientBucketId = 45354, Object = "SN2013fc" where object like "%PESSTOESO154%" and (filename like "%2013fc%" or filename like "%PSNJ02450896-5544273%");
update sofi_spectra set transientBucketId = 45354, Object = "SN2013fc" where object like "%PESSTOESO154%" and (filename like "%2013fc%" or filename like "%PSNJ02450896-5544273%");
update efosc_imaging set do_not_release = 1 where photsys is null and data_rel is not null and filter = "Free";

update efosc_spectra set contnorm = "F" where contnorm is null and esoPhaseIII = 1;
update sofi_spectra set contnorm = "F" where contnorm is null and esoPhaseIII = 1;
update efosc_spectra set EXT_OBJ = "F" where EXT_OBJ is null and esoPhaseIII = 1;
update sofi_spectra set EXT_OBJ = "F" where EXT_OBJ is null and esoPhaseIII = 1;
update efosc_spectra set TOT_FLUX = "F" where TOT_FLUX is null and esoPhaseIII = 1;
update sofi_spectra set TOT_FLUX = "F" where TOT_FLUX is null and esoPhaseIII = 1;


## objects that are too close to other objects
update efosc_spectra set ra = 41.28729, decl = -55.74092 where object = "SN2013fc" and data_rel is not null;
update sofi_spectra set ra = 41.28729, decl = -55.74092 where object = "SN2013fc" and data_rel is not null;


update transientBucket set spectralType = "Ia" where spectralType = "SN Ia";
update transientBucket set spectralType = "Ia-p" where spectralType = "SN Ia-pec";
update transientBucket set spectralType = "II" where spectralType = "SN II";
update transientBucket set spectralType = "II-pec" where spectralType = "SN II-pec";
update transientBucket set spectralType = "Ibc" where spectralType = "SN Ib/c";
update transientBucket set spectralType = "IIP" where spectralType = "SN IIP";
update transientBucket set spectralType = "Ic" where spectralType = "SN Ic";
update transientBucket set spectralType = "Ic-p" where spectralType = "SN Ic-pec";
update transientBucket set spectralType = "IIL" where spectralType = "SN IIL";
update transientBucket set spectralType = "IIn" where spectralType = "SN IIn";
update transientBucket set spectralType = "Ib" where spectralType = "SN Ib";
update transientBucket set spectralType = "Ib-p" where spectralType = "SN Ib-pec";
update transientBucket set spectralType = "I" where spectralType = "SN I";
update transientBucket set spectralType = "IIb" where spectralType = "SN IIb";
update transientBucket set spectralType = "Ibn" where spectralType = "SN Ibn";
update transientBucket set spectralType = "Ic-BL" where spectralType = "SN Ic-BL";
update transientBucket set spectralType = "Ia" where spectralType = "SN Ia-91bg-like";
update transientBucket set spectralType = "Ia" where spectralType = "Ia-91bg";
update transientBucket set spectralType = "Ia" where spectralType = "SN Ia-91T-like";
update transientBucket set spectralType = "IIP" where spectralType = "SN Type IIP";
update transientBucket set spectralType = "AGN" where spectralType = "agn";
update transientBucket set spectralType = "Imposter" where spectralType = "imposter";
update transientBucketSummaries set recentClassification = "Ia" where recentClassification = "SN Ia";
update transientBucketSummaries set recentClassification = "Ia-p" where recentClassification = "SN Ia-pec";
update transientBucketSummaries set recentClassification = "II" where recentClassification = "SN II";
update transientBucketSummaries set recentClassification = "II-pec" where recentClassification = "SN II-pec";
update transientBucketSummaries set recentClassification = "Ibc" where recentClassification = "SN Ib/c";
update transientBucketSummaries set recentClassification = "IIP" where recentClassification = "SN IIP";
update transientBucketSummaries set recentClassification = "Ic" where recentClassification = "SN Ic";
update transientBucketSummaries set recentClassification = "Ic-p" where recentClassification = "SN Ic-pec";
update transientBucketSummaries set recentClassification = "IIL" where recentClassification = "SN IIL";
update transientBucketSummaries set recentClassification = "IIn" where recentClassification = "SN IIn";
update transientBucketSummaries set recentClassification = "Ib" where recentClassification = "SN Ib";
update transientBucketSummaries set recentClassification = "Ib-p" where recentClassification = "SN Ib-pec";
update transientBucketSummaries set recentClassification = "I" where recentClassification = "SN I";
update transientBucketSummaries set recentClassification = "IIb" where recentClassification = "SN IIb";
update transientBucketSummaries set recentClassification = "Ibn" where recentClassification = "SN Ibn";
update transientBucketSummaries set recentClassification = "Ic-BL" where recentClassification = "SN Ic-BL";
update transientBucketSummaries set recentClassification = "Ia" where recentClassification = "SN Ia-91bg-like";
update transientBucketSummaries set recentClassification = "Ia" where recentClassification = "Ia-91bg";
update transientBucketSummaries set recentClassification = "Ia" where recentClassification = "SN Ia-91T-like";
update transientBucketSummaries set recentClassification = "IIP" where recentClassification = "SN Type IIP";
update transientBucketSummaries set recentClassification = "AGN" where recentClassification = "agn";
update transientBucketSummaries set recentClassification = "Imposter" where recentClassification = "imposter";



UPDATE pesstoObjects p,
    transientBucketSummaries s 
SET 
    marshallWorkflowLocation = 'archive'
WHERE
    marshallWorkflowLocation = 'inbox'
        AND s.transientBucketId = p.transientBucketId
        AND currentMagnitude > 20.5;
