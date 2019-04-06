#!/bin/csh

set label = "ksTestTail test[scenario rcm gcm location]"
set obs = dataFiles/prec.obs.nc
set cur = dataFiles/prec.cur.nc
set fut = dataFiles/prec.fut.nc
set png = testKStestTail.png
set var = prec
set txt = testKStestTail.csv
python ksTestTail.py $label $obs $cur $fut $png $var $txt
