update efosc_imaging set dryx_comment = "spectroscopic standard - do not release" where object in ("eg131", "eg21", "eg274", "feige110", "gd153", "gd71", "l745a", "ltt3218", "ltt3218", "ltt3864", "ltt7379", "s82_0237-0054", "s82_0304-0110", "s82_0315+0103", "vma2", "s82_0222+0054");    
update efosc_imaging set dryx_comment = "photometric standard field - do not release" where object in ("MARKA", "pg0231", "PG1047", "PG1323", "pg2336", "RU152", "RU_149");