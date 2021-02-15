mysqldump -umarshall -pmar5ha11 pessto_phase_iii --no-data --routines > ~/git_repos/_packages_/python/ep3/ep3/tests/input/pessto_phase_iii_schema.sql
mysqldump -umarshall -pmar5ha11 pessto_phase_iii ntt_standards filetype_key_calibration filetype_key_reduction_stage transientbucketsummaries astronotes_transients atel_coordinates atel_names marshall_transient_akas fits_header_keywords >> ~/git_repos/_packages_/python/ep3/ep3/tests/input/pessto_phase_iii_schema.sql
perl -p -i.bak -e "s/DEFINER=\`\w.*?\`@\`.*?\`//g" ~/git_repos/_packages_/python/ep3/ep3/tests/input/pessto_phase_iii_schema.sql
perl -p -i.bak -e "s/ALTER DATABASE .*?CHARACTER.*?;//g" ~/git_repos/_packages_/python/ep3/ep3/tests/input/pessto_phase_iii_schema.sql
perl -p -i.bak -e "s/AUTO_INCREMENT=\d*//g" ~/git_repos/_packages_/python/ep3/ep3/tests/input/pessto_phase_iii_schema.sql
rm -rf  ~/git_repos/_packages_/python/ep3/ep3/tests/input/pessto_phase_iii_schema.sql.bak
