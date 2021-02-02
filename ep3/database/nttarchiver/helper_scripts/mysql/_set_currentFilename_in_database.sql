DELIMITER //
DROP PROCEDURE IF EXISTS `set_currentFilenames_in_database`;
CREATE PROCEDURE `set_currentFilenames_in_database`()
COMMENT 'A procedure to set currentFilenames in database'
BEGIN
    -- SET VARIABLE
    SET @array = 'efosc_imaging,sofi_imaging,efosc_spectra,sofi_spectra,corrupted_files,';
    WHILE (LOCATE(',', @array) > 0)    WHILE (LOCATE(',', @array) > 0)

    DO
        SET @item = SUBSTRING(@array, 1, LOCATE(',',@array)-1);
        SET @array = SUBSTRING(@array, LOCATE(',', @array) + 1);

        ## ADD A NEWFILENAME FOR _e FILES FOR EFOSC
        SET @sqlText = CONCAT('UPDATE ',@item,' SET currentFilename = filename, currentFilepath = filepath where currentFilename is NULL and updatedFilename is null;');
        PREPARE stmt FROM @sqlText;
        EXECUTE stmt;

        IF @item = "efosc_spectra" THEN
            SET @sqlText = CONCAT('UPDATE ',@item,' SET updatedFilename = SUBSTRING(replace(filename,"_e.fits",".fits"),2), updatedFilepath = CONCAT(archivePath,SUBSTRING(replace(filename,"_e.fits",".fits"),2)) where filename like "%\_e.fits" and currentFilename = filename and updatedFilename is null;');
            select @sqlText AS ' ';
            PREPARE stmt FROM @sqlText;
            EXECUTE stmt;
            SET @sqlText = CONCAT('UPDATE ',@item,' SET updatedFilename = SUBSTRING(replace(filename,"_2df.fits","_si.fits"),2), updatedFilepath = CONCAT(archivePath,SUBSTRING(replace(filename,"_2df.fits","_si.fits"),2)) where filename like "%\_2df.fits" and currentFilename = filename and updatedFilename is null;');
            PREPARE stmt FROM @sqlText;
            EXECUTE stmt;
        ELSEIF @item = "efosc_imaging" THEN
            SET @sqlText = CONCAT('UPDATE ',@item,' SET updatedFilename = filename, updatedFilepath = CONCAT(archivePath,filename) where (filetype_key_reduction_stage = 3 and filetype_key_calibration = 13) or (filetype_key_reduction_stage = 2 and filetype_key_calibration = 8) and currentFilename = filename and updatedFilename is null;');
            PREPARE stmt FROM @sqlText;
            EXECUTE stmt;
        ELSEIF @item = "sofi_spectra" THEN
            SET @sqlText = CONCAT('UPDATE ',@item,' SET updatedFilename = filename, updatedFilepath = CONCAT(archivePath,filename) where filename like "%_merge_%" and (filename like "%_sc.fits" or filename like "%\_f.fits") and currentFilename = filename and updatedFilename is null;');
            PREPARE stmt FROM @sqlText;
            EXECUTE stmt;
            SET @sqlText = CONCAT('UPDATE ',@item,' SET updatedFilename = replace(filename,"_2df.fits","_si.fits"), updatedFilepath = CONCAT(archivePath,replace(filename,"_2df.fits","_si.fits")) where filename like "%\_2df.fits" and currentFilename = filename and updatedFilename is null;');
            PREPARE stmt FROM @sqlText;
            EXECUTE stmt;
        ELSEIF @item = "sofi_imaging" THEN
            SET @sqlText = CONCAT('UPDATE ',@item,' SET updatedFilename = filename, updatedFilepath = CONCAT(archivePath,filename)  where filename like "%merge%" and currentFilename = filename and updatedFilename is null;');
            PREPARE stmt FROM @sqlText;
            EXECUTE stmt;
            SET @sqlText = CONCAT('UPDATE ',@item,' SET updatedFilename = filename, updatedFilepath = CONCAT(archivePath,filename)  where filename like "%weight%" and currentFilename = filename and updatedFilename is null;');
            PREPARE stmt FROM @sqlText;
            EXECUTE stmt;
        END IF;

        ## ADD A NEWFILENAME FOR ACQUISITION IMAGES FILES
        SET @sqlText = CONCAT('UPDATE ',@item,' SET updatedFilename = replace(filename,"OTHER",CONCAT("acq_",OBJECT)), updatedFilepath = CONCAT(archivePath,replace(filename,"OTHER",CONCAT("acq_",OBJECT))) where filename like "OTHER\_%" and filetype_key_reduction_stage = 3 and filetype_key_calibration = 10 and currentFilename = filename and updatedFilename is null;');
        PREPARE stmt FROM @sqlText;
        EXECUTE stmt;

        DEALLOCATE PREPARE stmt;
    END WHILE;

END//

-- CALL THE PROCEDURE
CALL `set_currentFilenames_in_database`();
DROP PROCEDURE IF EXISTS `set_currentFilenames_in_database`;
