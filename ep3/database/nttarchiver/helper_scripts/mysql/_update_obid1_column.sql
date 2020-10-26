update view_efosc_imaging_reduced set obid1=obid where obid1 is NULL;
update view_sofi_imaging_reduced set obid1=obid where obid1 is NULL;
update view_efosc_spectra_reduced set obid1=obid where obid1 is NULL;
## sofi_spectra has no obid1 column
