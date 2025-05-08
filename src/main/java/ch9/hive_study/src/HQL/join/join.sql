SELECT e.name, e.salary, d.name AS dept_name
FROM emp e INNER JOIN dept d
ON e.dept_id = d.id;

SELECT e.name, e.salary, d.name AS dept_name
FROM emp e LEFT JOIN dept d
ON e.dept_id = d.id;

SELECT e.name, e.salary, d.name AS dept_name
FROM emp e RIGHT JOIN dept d
ON e.dept_id = d.id;

SELECT e.name, e.salary, d.name AS dept_name
FROM dept d LEFT JOIN emp e
ON d.id = e.dept_id;

SELECT e.name, e.salary, d.name AS dept_name
FROM dept d LEFT JOIN emp e
ON d.id = e.dept_id;

SELECT *
FROM emp e LEFT SEMI JOIN dept d
ON e.dept_id = d.id;

SELECT e.name, e.salary, d.name AS dept_name
FROM emp e CROSS JOIN dept d
ON e.dept_id = d.id;

SELECT e.name, e.salary
FROM emp e
         LEFT SEMI JOIN (
    SELECT dept_id, AVG(salary) AS avg_salary
    FROM emp
    GROUP BY dept_id
) t
ON e.dept_id = t.dept_id AND e.salary > t.avg_salary;
SELECT d.dept_id, d.dept_name, COUNT(e.emp_id) AS emp_count,
       AVG(e.salary) AS avg_salary,
       MIN(e.hire_date) AS min_hire_date, MAX(e.hire_date) AS max_hire_date,
       DATEDIFF(MAX(e.hire_date),MIN(e.hire_date)) AS hire_span,
       CASE WHEN d.create_date > TO_DATE('2020-01-01')
                THEN 'Yes' ELSE 'No' END AS is_created_after_2020
FROM emp e JOIN dept d ON e.dept_id = d.dept_id;