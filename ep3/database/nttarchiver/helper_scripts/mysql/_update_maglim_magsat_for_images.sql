DELIMITER //
DROP PROCEDURE IF EXISTS `update_maglim_magsat`;
CREATE PROCEDURE `update_maglim_magsat`()
COMMENT 'A procedure to update maglim and magsat for NTT images'
BEGIN
    -- SET VARIABLE
    -- SELECT @mbkg := mbkg FROM efosc_imaging where filetype_key_calibration = 13;
    -- SELECT @gain := gain FROM efosc_imaging where filetype_key_calibration = 13;
    -- SELECT @texptime := texptime FROM efosc_imaging where filetype_key_calibration = 13;
    -- SELECT @effron := effron FROM efosc_imaging where filetype_key_calibration = 13;
    -- SELECT @photzp := photzp FROM efosc_imaging where filetype_key_calibration = 13;

    SET @n_pix := '(PI()*POW((`PSF_FWHM`/0.24),2))';
    SET @one := CONCAT(@n_pix,'*(mbkg*gain)');
    SET @two := CONCAT(@n_pix,'*pow(effron,2)');
    SET @three := CONCAT('sqrt(',@one,'+',@two,')');
    SET @four := CONCAT('(5/(gain*texptime))*',@three);
    SET @five := CONCAT('photzp-2.5*log(10,',@four,')');

    SET @sqlText = CONCAT('UPDATE ignore efosc_imaging SET abmaglim =',@five,' where filetype_key_calibration = 13 or esoPhaseIII = 1;');
    PREPARE stmt FROM @sqlText;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @four := CONCAT('(5/gain)*',@three);
    SET @five := CONCAT('photzp-2.5*log(10,',@four,')');

    SET @sqlText = CONCAT('UPDATE ignore sofi_imaging SET abmaglim =',@five,' where filetype_key_calibration = 13 or esoPhaseIII = 1 and filename not like "%weight%";');
    PREPARE stmt FROM @sqlText;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @one := 'POW((`PSF_FWHM`/0.24),2)';
    SET @two := CONCAT('(PI()/4*ln(2))*(60000-mbkg)*',@one);
    SET @three := CONCAT('photzp-2.5*log(10,',@two,')');

    SET @sqlText = CONCAT('UPDATE ignore efosc_imaging SET abmagsat =',@three,' where filetype_key_calibration = 13 or esoPhaseIII = 1;');
    PREPARE stmt FROM @sqlText;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @one := 'POW((`PSF_FWHM`/0.24),2)';
    SET @two := CONCAT('(PI()/4*ln(2))*(32000-mbkg)*',@one);
    SET @three := CONCAT('photzp-2.5*log(10,',@two,')');

    SET @sqlText = CONCAT('UPDATE ignore sofi_imaging SET abmagsat =',@three,' where filetype_key_calibration = 13 or esoPhaseIII = 1;');
    PREPARE stmt FROM @sqlText;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END//

-- CALL THE PROCEDURE
CALL `update_maglim_magsat`();
DROP PROCEDURE IF EXISTS `update_maglim_magsat`;



