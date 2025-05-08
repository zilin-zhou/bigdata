drop table emp;
CREATE EXTERNAL TABLE emp(
   id INT,
   name STRING,
   salary DOUBLE,
   dept_id INT,
   txn_date DATE
)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA INPATH '/user/hive/mydata/emp.txt'
    OVERWRITE INTO TABLE emp;
drop table dept;
CREATE EXTERNAL TABLE dept(
                            id INT,
                            name STRING
)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA INPATH '/user/hive/mydata/dept.txt'
OVERWRITE INTO TABLE dept;
select * from emp;
select * from dept;
SELECT name, dept_id
FROM emp
WHERE salary > 4000;
SELECT id, name, salary, dept_id
FROM emp
WHERE salary >= 4000 AND (dept_id = 10 OR dept_id = 20);
SELECT name, salary
FROM emp
ORDER BY salary DESC ;
SELECT name, salary
FROM emp
SORT BY salary;
SELECT *
FROM emp
    DISTRIBUTE BY dept_id
    SORT BY salary DESC;
SELECT *
FROM emp e INNER JOIN dept d
ON e.dept_id = d.id;
SELECT *
FROM dept d LEFT JOIN emp e
ON e.dept_id = d.id;
SELECT *
FROM emp e LEFT JOIN dept d
ON e.dept_id = d.id;
SELECT *
FROM emp e RIGHT JOIN dept d
ON e.dept_id = d.id;
SELECT *
FROM dept d RIGHT JOIN emp e
ON e.dept_id = d.id;
SELECT *
FROM dept d FULL JOIN emp e
ON e.dept_id = d.id;
SELECT *
FROM emp e FULL JOIN dept d
ON e.dept_id = d.id;

SELECT *
FROM emp e LEFT SEMI JOIN dept d
ON e.dept_id = d.id;
SELECT *
FROM emp e CROSS JOIN dept d
ON e.dept_id = d.id;
    SELECT *
    FROM emp e CROSS JOIN dept d;
    SELECT *
    FROM emp e JOIN dept d;
CREATE TABLE emp_bucketed
AS
SELECT *
FROM emp
CLUSTER BY dept_id;
select * from emp_bucketed;
SELECT id, name, salary, dept_id,
sum(salary)
OVER (PARTITION BY dept_id) AS total_salary
FROM emp;
SELECT id, name, salary, dept_id,rank() OVER (
                   PARTITION BY dept_id
                   ORDER BY salary DESC
                   ) AS rank
FROM emp;
select explode(array(1,2,3)) as num;
select parse_url_tuple(
               'https://www.example.com/path?query=hello#ref',
               'HOST',
               'PATH',
               'QUERY',
               'REF')
           as (host,path,query,ref);

select * from emp;
set mapreduce.job.reduces=4;
select id, name, salary, dept_id
from emp
cluster by dept_id;
SELECT dept_id, COUNT(id) AS emp_count, AVG(salary) AS avg_salary
FROM emp
GROUP BY dept_id
GROUPING SETS (dept_id, ( ));
SELECT dept_id, txn_date, COUNT(id) AS emp_count, AVG(salary) AS avg_salary
FROM emp
GROUP BY dept_id, txn_date
WITH CUBE;
SELECT dept_id, txn_date, COUNT(id) AS emp_count, AVG(salary) AS avg_salary
FROM emp
GROUP BY dept_id, txn_date
WITH ROLLUP;
SELECT
    dept_id,
    AVG(
            CASE WHEN dept_id IN (10, 20)
                 THEN salary * 1.1
            ELSE salary
            END) AS avg_salary
FROM
    emp
GROUP BY
    dept_id;
SELECT
    CASE WHEN salary >= 5000 THEN 'High'
         WHEN salary >= 4000 THEN 'Medium'
         ELSE 'Low'
        END AS salary_level,
    COUNT(*) AS employee_count
FROM
    emp
GROUP BY
    CASE WHEN salary >= 5000 THEN 'High'
         WHEN salary >= 4000 THEN 'Medium'
         ELSE 'Low'
        END;

