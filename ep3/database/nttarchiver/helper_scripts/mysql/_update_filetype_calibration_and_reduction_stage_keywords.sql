-- ADD FILETYPE KEYS FOR 6ey_calibration and filetype_key_reduction_stage
DELIMITER //
DROP PROCEDURE IF EXISTS `set_calibration_and_reduction_stage_keys_from_filename`;
CREATE PROCEDURE `set_calibration_and_reduction_stage_keys_from_filename`(IN tableName VARCHAR(255),IN prefix VARCHAR(20),IN calibrationKey TINYINT,IN reductionKey TINYINT,IN currentCalibrationKey TINYINT ,IN currentReductionKey TINYINT)
COMMENT 'A procedure to set calibration and reduction stage keys for the ntt data'
BEGIN
    -- SET VARIABLE
    SET @tableName = tableName;
    SET @prefix = prefix;
    SET @calibrationKey = calibrationKey;
    SET @reductionKey = reductionKey;
    SET @currentCalibrationKey = currentCalibrationKey;
    SET @currentReductionKey = currentReductionKey;
    SET @sqlQuery1 = CONCAT('SELECT * FROM (SELECT primaryId from ',@tableName,' WHERE fileName LIKE "%',@prefix,'%" and (filetype_key_calibration = ',@currentCalibrationKey,' and filetype_key_reduction_stage = ',@currentReductionKey,')) as alias');
    SET @sqlText = CONCAT('UPDATE ',@tableName,' SET filetype_key_calibration = ',@calibrationKey,', filetype_key_reduction_stage = ',@reductionKey,' WHERE primaryId in (',@sqlQuery1,')');
    -- select @sqlText AS ' ';

    PREPARE stmt FROM @sqlText;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END//
DELIMITER ;

DELIMITER //
DROP PROCEDURE IF EXISTS `set_calibration_and_reduction_stage_keys_from_keywords`;
CREATE PROCEDURE `set_calibration_and_reduction_stage_keys_from_keywords`(IN tableName VARCHAR(255),IN keyword VARCHAR(255),IN value VARCHAR(255),IN calibrationKey TINYINT,IN reductionKey TINYINT,IN currentCalibrationKey TINYINT,IN currentReductionKey TINYINT)
COMMENT 'A procedure to set calibration and reduction stage keys for the ntt data'
BEGIN
    -- SET VARIABLE
    SET @tableName = tableName;
    SET @keyword = keyword;
    SET @value = value;
    SET @calibrationKey = calibrationKey;
    SET @reductionKey = reductionKey;
    SET @currentCalibrationKey = currentCalibrationKey;
    SET @currentReductionKey = currentReductionKey;
    SET @sqlQuery1 = CONCAT('SELECT * from (SELECT primaryId FROM ',@tableName,' WHERE lower(',@keyword,') like "%',@value,'%" and (filetype_key_calibration = ',@currentCalibrationKey,' and filetype_key_reduction_stage = ',@currentReductionKey,')) as alias');
    SET @sqlText = CONCAT('UPDATE ',@tableName,' SET filetype_key_calibration = ',@calibrationKey,', filetype_key_reduction_stage = ',@reductionKey,' WHERE primaryId in (',@sqlQuery1,')');
    -- select @sqlText AS ' ';
    PREPARE stmt FROM @sqlText;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END//
DELIMITER ;


-- CALL THE PROCEDURE
DELIMITER //
DROP PROCEDURE IF EXISTS `loop_over_ntt_tables_and_set_keywords`;
CREATE PROCEDURE `loop_over_ntt_tables_and_set_keywords`()
COMMENT 'A procedure to loop over ntt tables and set keywords'
BEGIN
    -- SET VARIABLE
    SET @array = 'efosc_imaging,sofi_imaging,efosc_spectra,sofi_spectra,';
    WHILE (LOCATE(',', @array) > 0)
    DO
        SET @item = SUBSTRING(@array, 1, LOCATE(',',@array)-1);
        SET @array = SUBSTRING(@array, LOCATE(',', @array) + 1);

        ## CRAP
        -- CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"LSQ12dlf_20130102_R642_56463_2",0,0,1,3);
        -- CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"LSQ12dlf_20130102_R642_56463_1",0,0,1,3);
        -- CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"OGLE-2012-SN-040_20121221_%_56475_%",0,0,1,3);

        ## BENNITI LP OBJECTS
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"2012au",1,5,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"2011ja",1,5,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"NGC5775OT",1,5,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"sn2010jl",1,5,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"2012aw",1,5,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"2012a_",1,5,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"2011hs",1,5,0,0);

        ## STD STARS
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"vma2",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"CSS121006-004911+052",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"ltt3864",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"BD174708003_",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"PG1047",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"RU152",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"pg0231",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"MARKA_",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"PG1323",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"pg2336",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"RU_149",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"s82_",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"LTT3218",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"L745a",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"eg131",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"CD32",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"hr4468",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"gd71",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"EG274",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip000682",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip014112",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip000080",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip000682",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip012167",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip12852",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"tHip013239",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip016344",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip023984",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip029097",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip036187",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip044487",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip45705",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip091690",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"hip9285",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip010502",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip010545",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip012167",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"HIP012425",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip12852",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"HIP013239",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip014112",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip015199",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip016344",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip021368",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip023984",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip029097",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip45705",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip036187",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"TSTD",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"TSTD",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip044487",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip45705",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"TSTD",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"TSTD",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"GD153",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"TSTD",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip084988",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip091690",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"hip101106",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip106951",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"TSTD",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"TSTD",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Hip110512",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"TSTD",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Feige110",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"eg21",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"ltt7379",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"sj9108",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"ILL_S264-D",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"STD",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"LTT7987",8,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"T_Phe",8,2,0,0);

        ## CRAP STD-STAR FIELD FILES
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"Free",8,4,8,2);


        ## CALIBRATIONS
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"sens_",7,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"atmo",6,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"illum_",9,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"mask",4,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"mask",4,2,8,2);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"fmask",4,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"bad_pixel_mask",4,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"bias_",2,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"flat_",3,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"nflat_",3,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"masterflat_",3,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"arc_",5,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"OTHER",10,3,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"acq_",10,3,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"weight",11,3,0,0);
        -- CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"sky",12,2,0,0);

        IF SUBSTRING(@item,1,5) = "efosc" THEN
            CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_OCS_DET1_IMGNAME","EFOSC_AcqRotSlit",10,4,10,3);
            CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_OCS_DET1_IMGNAME","EFOSC_AcqRotSlit",10,4,0,0);
        END IF;
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"OBJECT","FOCUS",1,4,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"OBJECT","FOCUS",1,4,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_OBS_NAME","slit-position",1,4,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_OBS_NAME","specpol",1,4,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_OBS_NAME","slit position",1,4,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_OBS_NAME","interactive",1,4,0,0);

        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_TPL_NAME","bias",2,1,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_DPR_TYPE","bias",2,1,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_TPL_NAME","flat",3,1,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_DPR_TYPE","flat",3,1,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_TPL_NAME","arc calibration",3,1,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_TPL_NAME","other",10,1,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_DPR_TYPE","other",10,1,0,0);

        ## STD FILES
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"weight",11,2,8,2);
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_OBS_NAME","std",8,0,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_DPR_TYPE","std",8,0,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"SOFI.",8,1,8,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"EFOSC.",8,1,8,0);
        ## SKY FILES
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_DPR_TYPE","sky",12,2,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"SOFI.",12,1,12,2);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"EFOSC.",12,1,12,2);
        ## SCIENCE IMAGE FILES
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_TPL_NAME","imag",13,6,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_DPR_TECH","imag",13,6,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_TPL_NAME","spectr",13,6,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_DPR_TECH","spectr",13,6,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_DPR_TYPE","OBJECT",13,6,0,0);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"SOFI.",13,1,13,6);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"EFOSC.",13,1,13,6);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"EFOSC.",1,1,1,4);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"SOFI.",13,1,13,6);
        CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"EFOSC.",13,1,13,6);

        ## FIND INTERMEDIATE FILES
        if @item = "efosc_spectra" THEN
            CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"_2df.fits",13,3,13,6);
            CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"_e.fits",13,3,13,6);
            CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"_sb.fits",13,3,13,6);
        END IF;

        if @item = "sofi_spectra" THEN
            CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"_f.fits",13,3,13,6);
            CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"_2df.fits",13,3,13,6);
            -- CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"_e.fits",13,3,13,6);
            CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"_sc.fits",13,3,13,6);
            CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"_sb.fits",13,3,13,6);
        END IF;

        if @item = "sofi_imaging" THEN
            CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,"merge",13,3,13,6);
        END IF;


        if @item = "efosc_imaging" THEN
            CALL `set_calibration_and_reduction_stage_keys_from_filename`(@item,".fits",13,3,13,6);
        END IF;

        SET @sqlText = CONCAT('UPDATE ',@item,' SET filetype_key_reduction_stage = 2 and filetype_key_calibration = 1 where filename like "\_%";');
        -- select @sqlText AS ' ';
        PREPARE stmt FROM @sqlText;
        EXECUTE stmt;

        CALL `set_calibration_and_reduction_stage_keys_from_keywords`(@item,"ESO_TPL_ID","_acq_",10,3,13,3);

        ## CRAP AQU FILES
        IF SUBSTRING(@item,1,5) = "efosc" THEN
            SET @sqlText = CONCAT('UPDATE ',@item,' SET filetype_key_reduction_stage = 4 where filetype_key_reduction_stage = 3 and filetype_key_calibration = 10 and ESO_INS_SLIT1_NAME != "free";');
            PREPARE stmt FROM @sqlText;
            EXECUTE stmt;
            SET @sqlText = CONCAT('UPDATE ',@item,' SET filetype_key_reduction_stage = 4 where filetype_key_reduction_stage = 3 and filetype_key_calibration = 10 and ESO_INS_FILT1_NAME = "free";');
            PREPARE stmt FROM @sqlText;
            EXECUTE stmt;
            SET @sqlText = CONCAT('UPDATE ',@item,' SET filetype_key_reduction_stage = 4 where filetype_key_reduction_stage = 3 and filetype_key_calibration = 10 and FILTER = "free";');
            PREPARE stmt FROM @sqlText;
            EXECUTE stmt;
        END IF;
        update efosc_imaging set filetype_key_calibration = 1, filetype_key_reduction_stage = 2, esoPhaseIII = 0 where psf_fwhm = 9999 and filename not like "EFOSC%" and filetype_key_reduction_stage != 3;



        ## ADD PRODCATG KEYWORD TO ACQU IMAGES
        SET @sqlText = CONCAT('UPDATE ',@item,' SET PRODCATG = "SCIENCE.IMAGE" where filetype_key_reduction_stage = 3 and filetype_key_calibration = 10 and PRODCATG != "SCIENCE.IMAGE";');
        PREPARE stmt FROM @sqlText;
        EXECUTE stmt;

        ## REMOVE PRODCATG KEYWORD FROM WEIGHT IMAGES
        SET @sqlText = CONCAT('UPDATE ',@item,' SET PRODCATG = Null where filetype_key_reduction_stage = 3 and filetype_key_calibration = 11 and PRODCATG is not Null;');
        PREPARE stmt FROM @sqlText;
        EXECUTE stmt;

        SET @sqlText = CONCAT('UPDATE ',@item,' SET filetype_key_reduction_stage = 4, filetype_key_calibration = 1 where filename like "\_%";');
        PREPARE stmt FROM @sqlText;
        EXECUTE stmt;

        ### FILL THE objectInFilename COLUMN
        -- SET @sqlText = 'UPDATE ',@item,' SET objectInFilename = SUBSTRING(string,start_index,length)  where filename like "OTHER_%" and filetype_key_reduction_stage = 3 and filetype_key_calibration = 10;'


        -- SET @sqlText = CONCAT('select count(*) from ',@item,' where filetype_key_reduction_stage = 0 or filetype_key_calibration = 0;');
        -- PREPARE stmt FROM @sqlText;
        -- EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END WHILE;
END//

-- CALL THE PROCEDURE
CALL `loop_over_ntt_tables_and_set_keywords`();
DROP PROCEDURE IF EXISTS `loop_over_ntt_tables_and_set_keywords`;
DROP PROCEDURE IF EXISTS `set_calibration_and_reduction_stage_keys_from_filename`;
DROP PROCEDURE IF EXISTS `set_calibration_and_reduction_stage_keys_from_keywords`;




## ADDED July 28, 2016 ...
# EFOSC IMAGE SCIENCE FRAME CORRECTION
update  efosc_imaging set filetype_key_calibration = 13 where object not in (select object from ntt_standards) and isInTransientBucket = 1 and filetype_key_calibration = 8;
update  efosc_imaging set filetype_key_calibration = 8 where object in (select object from ntt_standards) and isInTransientBucket = 1 and filetype_key_calibration = 13;

# UPDATE SOFI ACQUISITION FRAMES
update sofi_imaging set filetype_key_calibration = 10 where  filetype_key_calibration = 8 and object = "SKY";
update sofi_imaging set filetype_key_calibration = 13 where object = "OTHER" and isInTransientBucket = 1 and filetype_key_calibration = 10  and object not in (select object from ntt_standards) ;



# SOFI IMAGE SCIENCE FRAME CORRECTION
update  sofi_imaging set filetype_key_calibration = 13 where object not in (select object from ntt_standards) and isInTransientBucket = 1 and filetype_key_calibration = 8;
update  sofi_imaging set filetype_key_calibration = 8 where object in (select object from ntt_standards) and isInTransientBucket = 1 and filetype_key_calibration = 13;
# ODD SOFI CALIBRATION FRAMES
update sofi_imaging set filetype_key_calibration = 8 where date_obs > "2014-05-02" and filetype_key_reduction_stage = 1 and filetype_key_calibration = 13  and object not in (select object from ntt_standards) and object = "OTHER";


# EFOSC SPECTRA SCIENCE FRAME CORRECTION
update  efosc_spectra set filetype_key_calibration = 13 where object not in (select object from ntt_standards) and isInTransientBucket = 1 and filetype_key_calibration = 8;
update  efosc_spectra set filetype_key_calibration = 13 where filename="EFOSC.2015-03-21T09:41:39.408.fits";
update  efosc_spectra set filetype_key_calibration = 8 where object in (select object from ntt_standards) and filetype_key_calibration = 13;

# SOFI SPECTRA SCIENCE FRAME CORRECTION
update  sofi_spectra set filetype_key_calibration = 13 where object not in (select object from ntt_standards) and isInTransientBucket = 1 and filetype_key_calibration = 8;
update  sofi_spectra set filetype_key_calibration = 8 where object in (select object from ntt_standards) and filetype_key_calibration = 13;

# SOFI SPECTRA LAMP CALIBRATATIONS
update  sofi_spectra set filetype_key_calibration = 3 where object = "LAMP" and filetype_key_calibration = 8;


update sofi_imaging e, transientBucket t set e.transientBucketId = t.transientBucketId, e.isInTransientBucket = 1, e.dryx_comment = "file marshall coordinate mismatch - object prob not in image?" where filetype_key_calibration = 8  and isInTransientBucket = 0 and e.transientBucketId = 0 and object not in (select object from ntt_standards) and e.object = t.name;
update efosc_spectra e, transientBucket t set e.transientBucketId = t.transientBucketId, e.isInTransientBucket = 1, e.dryx_comment = "file marshall coordinate mismatch - object prob not in image?" where filetype_key_calibration = 8  and isInTransientBucket = 0 and e.transientBucketId = 0 and object not in (select object from ntt_standards) and e.object = t.name;
update efosc_imaging e, transientBucket t set e.transientBucketId = t.transientBucketId, e.isInTransientBucket = 1, e.dryx_comment = "file marshall coordinate mismatch - object prob not in image?" where filetype_key_calibration = 8  and isInTransientBucket = 0 and e.transientBucketId = 0 and object not in (select object from ntt_standards) and e.object = t.name;
update sofi_spectra e, transientBucket t set e.transientBucketId = t.transientBucketId, e.isInTransientBucket = 1, e.dryx_comment = "file marshall coordinate mismatch - object prob not in image?" where filetype_key_calibration = 8  and isInTransientBucket = 0 and e.transientBucketId = 0 and object not in (select object from ntt_standards) and e.object = t.name;


