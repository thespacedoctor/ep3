update sofi_imaging set gain = ncombine * (2./3.) * 5.4 where esoPhaseIII = 1;
update sofi_imaging set DETRON = 12. where esoPhaseIII = 1 and DETRON != 12.;
update sofi_spectra set DETRON = 12. where esoPhaseIII = 1 and DETRON != 12.;
update sofi_imaging set effron = 12.*sqrt(PI()/2)/sqrt(ncombine*ndit) where esoPhaseIII = 1;

