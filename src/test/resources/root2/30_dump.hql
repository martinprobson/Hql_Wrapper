INSERT OVERWRITE LOCAL DIRECTORY '${OUTPUT_DIR}'
  ROW FORMAT delimited fields terminated by ','
  SELECT * FROM test_results;