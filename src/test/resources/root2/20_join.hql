set hive.auto.convert.join=false;
set hive.auto.convert.join.noconditionaltask=false;
create table test_results as
    SELECT a.emp_no,a.last_name,b.dept_no
    FROM employees a JOIN dept_emp b ON a.emp_no = b.emp_no;

