-- ## UPDATE THE FLUERR IN EFOSC SPECTRA
UPDATE efosc_spectra SET FLUXERR = 15.2 WHERE (SUBSTRING(filename,-9,4) = "_2df" or SUBSTRING(filename,-7,2) = "_e" or SUBSTRING(filename,-8,3) = "_sb") and FLUXERR != 15.2;
UPDATE sofi_spectra SET FLUXERR = 34.7 WHERE (SUBSTRING(filename,-9,4) = "_2df" or SUBSTRING(filename,-7,2) = "_f" or SUBSTRING(filename,-8,3) = "_sb" or SUBSTRING(filename,-8,3) = "_sc") and FLUXERR != 34.7 and data_rel = "ssdr1";
UPDATE sofi_spectra SET FLUXERR = 22.0 WHERE (SUBSTRING(filename,-9,4) = "_2df" or SUBSTRING(filename,-7,2) = "_f" or SUBSTRING(filename,-8,3) = "_sb" or SUBSTRING(filename,-8,3) = "_sc") and FLUXERR != 22.0 and data_rel = "ssdr2";


-- ## UPDATE THE PHOTZP AND PHOTZPER FOR PHOTOMETRIC NIGHTS FOR EFOSC IMAGING DATA
UPDATE efosc_imaging i,
    efosc_average_zero_points z 
SET 
    i.PHOTZPER = z.zeropoint_error
WHERE
    i.filename NOT LIKE 'EFOSC.%'
        AND z.filter = i.eso_ins_filt1_name
        AND i.primaryId IN (SELECT 
            *
        FROM
            (SELECT 
                e.primaryId
            FROM
                efosc_imaging e
            WHERE
                (e.PHOTZPER = 9999 OR e.PHOTZPER IS NULL)
                    AND SUBSTRING(SUBSTRING(e.filename, LOCATE(CONCAT('_', YEAR(e.DATE_OBS)), e.filename), 9), 2, 8) not in ('', "2013_SN_") and DATE(SUBSTRING(SUBSTRING(e.filename, LOCATE(CONCAT('_', YEAR(e.DATE_OBS)), e.filename), 9), 2, 8)) IN (SELECT 
                        night_date
                    FROM
                        ntt_photometric_night_log
                    WHERE
                        flag = 1)) AS subSelect);

-- NOT SURE WHAT THIS COMMAND WAS SUPPOSED TO DO BUT IT DOESN'T WORK! -- 20170309

-- UPDATE efosc_imaging SET PHOTZP = (
--     SELECT zeropoint from efosc_average_zero_points WHERE filename not like "EFOSC.%" and filter = eso_ins_filt1_name and PHOTZPER = zeropoint_error
-- ) WHERE primaryId in (
--     SELECT * from (
--         SELECT e.primaryId from
--             efosc_imaging as e,
--             ntt_photometric_night_log as n
--         WHERE
--             (e.PHOTZPER = (SELECT zeropoint_error from efosc_average_zero_points WHERE filename not like "EFOSC.%" and filter = eso_ins_filt1_name) or e.PHOTZPER = 9999) and
--             date(SUBSTRING(SUBSTRING(e.filename,LOCATE(CONCAT('_', YEAR(DATE_OBS)), e.filename),9),2,8)) in (
--                 SELECT
--                     n.night_date
--                 from
--                     ntt_photometric_night_log
--                 where
--                     n.flag = 1
--                 )
--         ) as subSelect
-- );


UPDATE efosc_imaging 
SET 
    PHOTZP = (SELECT 
            zeropoint
        FROM
            efosc_average_zero_points
        WHERE
            filename NOT LIKE 'EFOSC.%'
                AND filter = eso_ins_filt1_name)
WHERE PHOTZPER = 9999;


UPDATE efosc_imaging SET PHOTZPER = 2.0 where filename not like "EFOSC.%" and PHOTZPER is NULL;
update efosc_imaging set PHOTZPER = 9999 where PHOTZPER = 2.0 and filename not like "EFOSC.%" and esoPhaseIII = 1;
update efosc_imaging a, (select distinct PHOTSYS, filter from efosc_imaging where esoPhaseIII = 1 and PHOTSYS is not null and PHOTSYS != "") b set a.PHOTSYS = b.PHOTSYS where a.filter=b.filter and a.esoPhaseIII = 1;


