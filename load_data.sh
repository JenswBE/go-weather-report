#!/usr/bin/env bash

DB_FILE="analysis.sqlite3"

echo
echo '=> Dropping existing DB ...'
rm ${DB_FILE:?}

echo
echo '=> Loading rain data ...'
sqlite3 ${DB_FILE:?} <<EOF
create table rain(timestamp TEXT, value REAL, quality_code INTEGER, absolute_value REAL, absolute_quality_code INTEGER);
EOF
for CSV_FILE in data/rain/*.csv; do
    sqlite3 --csv --separator ';' ${DB_FILE:?} ".import \"${CSV_FILE:?}\" rain"
done

echo
echo '=> Loading sunset/sunrise data ...'
sqlite3 ${DB_FILE:?} <<EOF
create table sun(date TEXT,sunrise_start TEXT,sunrise_end TEXT,sunset_start TEXT,sunset_end TEXT,duration_minutes INTEGER);
EOF
for CSV_FILE in data/sunrise_sunset/*.csv; do
    sqlite3 --csv --separator ',' ${DB_FILE:?} ".import \"${CSV_FILE:?}\" sun"
done

echo
echo '=> Loading temperature data ...'
sqlite3 ${DB_FILE:?} <<EOF
create table temperature(timestamp TEXT, value REAL, quality_code INTEGER, absolute_value REAL, absolute_quality_code INTEGER);
EOF
for CSV_FILE in data/temperature/*.csv; do
    sqlite3 --csv --separator ';' ${DB_FILE:?} ".import \"${CSV_FILE:?}\" temperature"
done

echo
echo '=> Cleaning data ...'
sqlite3 ${DB_FILE:?} <<EOF
-- Remove comments
DELETE FROM rain WHERE timestamp LIKE "#%";
DELETE FROM sun WHERE date NOT LIKE "2%";
DELETE FROM temperature WHERE timestamp LIKE "#%";
EOF

echo
echo '=> Checking data ...'
echo -n 'Tables: '
sqlite3 ${DB_FILE:?} '.tables'
echo
sqlite3 -header ${DB_FILE:?} 'SELECT * FROM rain LIMIT 10;'
echo
sqlite3 -header ${DB_FILE:?} 'SELECT * FROM sun LIMIT 10;'
echo
sqlite3 -header ${DB_FILE:?} 'SELECT * FROM temperature LIMIT 10;'