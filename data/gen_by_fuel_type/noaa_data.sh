# declare -a arr=('DE' 'IL' 'IN' 'KY' 'MD' 'MI' 'NJ' 'NC' 'OH' 'PA' 'TN' 'VA' 'WV' 'DC')
# declare -a de=('72408813707' '72409313764' '72418013781' '99728199999' '99737299999' '99768999999' '99769499999' '99816999999')





dolt sql -r csv -q "
    SELECT *
    FROM dolt_history_air_temp
    WHERE station = '99731499999' AND commit_date <= '2016-01-01 00:00:00' 
" > DC/air-temp.csv


dolt sql -r csv -q "
    SELECT *
    FROM dolt_history_dew_point_temp
    WHERE station = '99731499999' AND commit_date <= '2016-01-01 00:00:00' 
" > DC/dew_point_temp.csv



dolt sql -r csv -q "
    SELECT *
    FROM dolt_history_sea_level_pressure
    WHERE station = '99731499999' AND commit_date <= '2016-01-01 00:00:00' 
" > DC/sea_level_pressure.csv



dolt sql -r csv -q "
    SELECT *
    FROM dolt_history_sky_ceiling_height
    WHERE station = '99731499999' AND commit_date <= '2016-01-01 00:00:00' 
" > DC/sky_ceiling_height.csv

dolt sql -r csv -q "
    SELECT *
    FROM dolt_history_visibility
    WHERE station = '99731499999' AND commit_date <= '2016-01-01 00:00:00' 
" > DC/visibility.csv

dolt sql -r csv -q "
    SELECT *
    FROM dolt_history_wind_speed
    WHERE station = '99731499999' AND commit_date <= '2016-01-01 00:00:00' 
" > DC/wind_speed.csv

