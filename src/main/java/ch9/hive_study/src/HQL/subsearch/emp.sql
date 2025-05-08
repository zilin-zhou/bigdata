SELECT
    name,
    salary
FROM
    (SELECT name, salary, AVG(salary)OVER() AS avg_salary FROM emp) AS sub
WHERE
        salary > avg_salary;

SELECT
    name,
    salary
FROM
    emp
WHERE
        salary > (SELECT AVG(salary) FROM emp WHERE dept_id = emp.dept_id);

SELECT
    name,
    salary,
    (SELECT AVG(salary) FROM emp) AS avg_salary
FROM
    emp;