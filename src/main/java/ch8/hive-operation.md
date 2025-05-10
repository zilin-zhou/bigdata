1、创建数据库 student\_data.

```
create database student_data;
```

2、创建表.

```
CREATE TABLE student(
    gender STRING,
    race STRING,
    parental_level STRING,
    lunch STRING,
    course STRING,
    math INT,
    reading INT,
    writing INT
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


```

3、加载表.

**3.1 location //本地加载到数据库**

```
LOAD DATA LOCAL INPATH '/opt/student-type.csv' 
INTO TABLE student;

//查询表的前几行
select * from student limit 10; 

//查询有多少条数据
select count(*) from student;

```

**3.2 从HDFS加载数据**

```
1、上传文件到HDFS文件系统
2、加载表（追加）
LOAD DATA INPATH  '/opt/data/student.csv' 
INTO TABLE student;

覆盖
LOAD DATA LOCAL INPATH '/opt/student-type.csv' 
OVERWRITE INTO TABLE student;

```

4、插入数据

```

INSERT INTO student 
VALUES
('female','group A',"new record",'standard','none',60,60,60),
('female','group B',"new record",'standard','none',70,70,70);

//查询追加结果
SELECT * FROM student WHERE parental_level="new record";

//将student表中parental_level=’new record’的学生的信息，经过三科成绩加5处理后，追加到student表中
INSERT INTO TABLE student
    SELECT gender,race,parental_level,lunch, 
    course,math+5, reading+5, writing+5
    FROM student 
    WHERE parental_level="new record";

// 计算
SELECT AVG(math) FROM student;

// 实验三命令
//建表
create table students(name string, clazz string, sid string,
gender string, birthday Date, phone bigint, loc string, score int)
row format delimited
fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

任务一：select gender,count(*) from students group by gender;
任务二：select distinct substr(name,0,1) from students;
任务三：select max(score),min(score) from students;
任务四：select avg(score) from students;

```
