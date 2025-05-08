drop table emp;
CREATE EXTERNAL TABLE emp(
                             emp_id INT,
                             emp_name STRING,
                             dept_id INT,
                             hire_date DATE,
                             salary DOUBLE
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA INPATH '/user/hive/mydata/function/emp_1.txt'
    OVERWRITE INTO TABLE emp;


drop table dept;
CREATE EXTERNAL TABLE dept(
                              dept_id INT,
                              dept_name STRING,
                              create_date DATE
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA INPATH '/user/hive/mydata/function/dept_1.txt'
    OVERWRITE INTO TABLE dept;