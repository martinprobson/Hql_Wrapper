CREATE TABLE employees (
        emp_no INT,
        first_name STRING,
        last_name STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH '${EMP_DATA}' OVERWRITE INTO TABLE employees;
SELECT COUNT(*) FROM employees;
