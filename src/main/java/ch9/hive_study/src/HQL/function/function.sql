show tables;
select * from emp;
SELECT emp_name,udf_upper(emp_name) FROM emp;
ADD JAR hdfs:///user/hive/udf/hive_study-2.0.jar;

CREATE TEMPORARY FUNCTION mean AS 'hive.study.Mean';

SELECT avg(salary) as AVG,mean(salary) as mean FROM emp;

ADD JAR hdfs:///user/hive/udf/hive_study-3.0.jar;

CREATE TEMPORARY FUNCTION split_string
AS 'hive.study.SplitStringUDTF';

SELECT split_string('apple,orange,banana') AS value;


select explode(array(1,2,3)) as num;
SELECT d.dept_id, d.dept_name, COUNT(e.emp_id) AS emp_count,
       AVG(e.salary) AS avg_salary,
       MIN(e.hire_date) AS min_hire_date, MAX(e.hire_date) AS max_hire_date,
       DATEDIFF(MAX(e.hire_date),MIN(e.hire_date)) AS hire_span,
       CASE WHEN d.create_date > TO_DATE('2020-01-01')
                THEN 'Yes' ELSE 'No' END AS is_created_after_2020
FROM emp e JOIN dept d ON e.dept_id = d.dept_id;



ADD JAR hdfs:///user/hive/udf/hive_study.jar;

CREATE FUNCTION udf_upper AS 'hive.study.UpperCaseUDF' USING JAR 'hdfs:///user/hive/udf/hive_study.jar';

