update view_efosc_spectra_raw set archivePath = CONCAT(year(date_obs),"/",LOWER(SUBSTRING(monthname(date_obs),1,3)),"/efosc/spectra/raw/") where archivePath is NULL;
update view_efosc_imaging_raw set archivePath = CONCAT(year(date_obs),"/",LOWER(SUBSTRING(monthname(date_obs),1,3)),"/efosc/imaging/raw/") where archivePath is NULL;
update view_sofi_spectra_raw set archivePath = CONCAT(year(date_obs),"/",LOWER(SUBSTRING(monthname(date_obs),1,3)),"/sofi/spectra/raw/") where archivePath is NULL;
update view_sofi_imaging_raw set archivePath = CONCAT(year(date_obs),"/",LOWER(SUBSTRING(monthname(date_obs),1,3)),"/sofi/imaging/raw/") where archivePath is NULL;

update view_efosc_spectra_reduced set archivePath = CONCAT(year(date_obs),"/",LOWER(SUBSTRING(monthname(date_obs),1,3)),"/efosc/spectra/reduced/") where archivePath is NULL;
update view_efosc_imaging_reduced set archivePath = CONCAT(year(date_obs),"/",LOWER(SUBSTRING(monthname(date_obs),1,3)),"/efosc/imaging/reduced/") where archivePath is NULL;
update view_sofi_spectra_reduced set archivePath = CONCAT(year(date_obs),"/",LOWER(SUBSTRING(monthname(date_obs),1,3)),"/sofi/spectra/reduced/") where archivePath is NULL;
update view_sofi_imaging_reduced set archivePath = CONCAT(year(date_obs),"/",LOWER(SUBSTRING(monthname(date_obs),1,3)),"/sofi/imaging/reduced/") where archivePath is NULL;

update view_efosc_spectra_intermediate set archivePath = CONCAT(year(date_obs),"/",LOWER(SUBSTRING(monthname(date_obs),1,3)),"/efosc/spectra/intermediate/") where archivePath is NULL;
update view_efosc_imaging_intermediate set archivePath = CONCAT(year(date_obs),"/",LOWER(SUBSTRING(monthname(date_obs),1,3)),"/efosc/imaging/intermediate/") where archivePath is NULL;
update view_sofi_spectra_intermediate set archivePath = CONCAT(year(date_obs),"/",LOWER(SUBSTRING(monthname(date_obs),1,3)),"/sofi/spectra/intermediate/") where archivePath is NULL;
update view_sofi_imaging_intermediate set archivePath = CONCAT(year(date_obs),"/",LOWER(SUBSTRING(monthname(date_obs),1,3)),"/sofi/imaging/intermediate/") where archivePath is NULL;

update view_sofi_spectra_benetti set archivePath = CONCAT(year(date_obs),"/",LOWER(SUBSTRING(monthname(date_obs),1,3)),"benetti/sofi/spectra/") where archivePath is NULL;
update view_sofi_imaging_benetti set archivePath = CONCAT(year(date_obs),"/",LOWER(SUBSTRING(monthname(date_obs),1,3)),"benetti/sofi/imaging/") where archivePath is NULL;
update view_efosc_spectra_benetti set archivePath = CONCAT(year(date_obs),"/",LOWER(SUBSTRING(monthname(date_obs),1,3)),"benetti/efosc/spectra/") where archivePath is NULL;
update view_efosc_imaging_intermediate set archivePath = CONCAT(year(date_obs),"/",LOWER(SUBSTRING(monthname(date_obs),1,3)),"benetti/efosc/imaging/") where archivePath is NULL;

update corrupted_files set archivePath = "trash/" where archivePath is NULL;
