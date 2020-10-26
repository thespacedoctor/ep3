update efosc_imaging set object = replace(object,'.dat','') where esoPhaseIII = 1 and object like "%.dat";
