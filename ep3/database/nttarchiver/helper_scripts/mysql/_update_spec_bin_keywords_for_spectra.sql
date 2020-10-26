update efosc_spectra set spec_bin = 0.43 where FORMAT(spec_bin,2) = 1.88 and ESO_INS_GRIS1_NAME = "Gr#16";
update efosc_spectra set spec_bin = 0.55 where FORMAT(spec_bin,2) = 1.52 and ESO_INS_GRIS1_NAME = "Gr#13";
update efosc_spectra set spec_bin = 0.41 where FORMAT(spec_bin,2) = 1.27 and ESO_INS_GRIS1_NAME = "Gr#11";

update efosc_spectra set spec_bin = ( WAVELMAX - WAVELMIN ) / ( NELEM - 1) where nelem is not null;
update sofi_spectra set spec_bin = ( WAVELMAX - WAVELMIN ) / ( NELEM - 1) where nelem is not null;
