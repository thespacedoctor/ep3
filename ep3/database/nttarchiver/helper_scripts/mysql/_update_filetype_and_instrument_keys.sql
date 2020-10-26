-- UPDATE THE IMAGING/SPECTRA & INSTRUMENTS KEYS
DELIMITER //
DROP PROCEDURE IF EXISTS `set_imaging_and_spectra_keys`;
CREATE PROCEDURE `set_imaging_and_spectra_keys`(IN tableName VARCHAR(255), IN typeKey TINYINT)
COMMENT 'A procedure to set imaging and spectra keys for all the newly ingested ntt data'
BEGIN
    -- SET VARIABLES
    SET @tableName = tableName;
    SET @typeKey = typeKey;
    IF @tableName like "efosc%" THEN
        SET @instrumentKey = 1;
    ELSE
        SET @instrumentKey = 2;
    END IF;

    SET @sqlText = CONCAT('UPDATE ',@tableName,' SET filetype_key_image_or_spectrum = ',@typeKey,', filetype_key_instrument = ',@instrumentKey,' WHERE (filetype_key_image_or_spectrum = 0 or filetype_key_instrument = 0);');
    PREPARE stmt FROM @sqlText;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END//
DELIMITER ;

CALL `set_imaging_and_spectra_keys`('efosc_imaging',1);
CALL `set_imaging_and_spectra_keys`('efosc_spectra',2);
CALL `set_imaging_and_spectra_keys`('sofi_imaging',1);
CALL `set_imaging_and_spectra_keys`('sofi_spectra',2);
DROP PROCEDURE IF EXISTS `set_imaging_and_spectra_keys`;
