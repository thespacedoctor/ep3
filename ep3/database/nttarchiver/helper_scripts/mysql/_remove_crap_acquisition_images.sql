update efosc_imaging set esoPhaseIII = 1, DATA_REL = NULL, filetype_key_reduction_stage = 4 where ABMAGLIM = 999.99 and currentFilename like "acq%";
