--1、创建瓜子二手车的内部表，将数据导入
CREATE DATABASE IF NOT EXISTS cardb
    COMMENT 'car'
    LOCATION '/user/hive/cardb'
    WITH DBPROPERTIES ('creator'='hadoop', 'created_at'='2023-06-02');

--1(1) 瓜子二手车
drop table guazi_cars;
CREATE TABLE guazi_cars (
                            title STRING,
                            city STRING,
                            vehicle_id STRING,
                            owner_price FLOAT,
                            original_price FLOAT,
                            registration_time STRING,
                            mileage FLOAT,
                            registration_location STRING,
                            displacement FLOAT,
                            transmission STRING,
                            owner STRING,
                            collection_time STRING,
                            label STRING
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

CREATE TABLE guazi_cars_str (
                            title STRING,
                            city STRING,
                            vehicle_id STRING,
                            owner_price STRING,
                            original_price STRING,
                            registration_time STRING,
                            mileage STRING,
                            registration_location STRING,
                            displacement STRING,
                            transmission STRING,
                            owner STRING,
                            collection_time STRING,
                            label STRING
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

--1(2)加载数据到表guazi_cars
LOAD DATA INPATH '/user/hive/car/guazi_cars.txt'
    OVERWRITE INTO TABLE guazi_cars;

LOAD DATA INPATH '/user/hive/car/guazi_cars.txt'
    OVERWRITE INTO TABLE guazi_cars_str;
--1(3)查看导入的数据
SELECT * from guazi_cars_str limit 10;

--宝马X3 2016款 xDrive20i M运动型(进口),,HC-94598442,金融专享价:￥30.69万,,Mar-18,5.74万公里,2.0T,自动,,李先生,52:36.6,宝马X3 2016款 xDrive20i M运动型(进口)

--2、二手车价格预测表：创建一个名为car_data的表，包含11个字段
DROP TABLE car_data_old;
CREATE TABLE car_data_old (
                          Name STRING,
                          Location STRING,
                          Year INT,
                          Kilometers_Driven INT,
                          Fuel_Type STRING,
                          Transmission STRING,
                          Owner_Type STRING,
                          Mileage STRING,
                          Engine STRING,
                          Power STRING,
                          Seats INT,
                          New_Price STRING,
                          Price FLOAT
)
    ROW FORMAT DELIMITED --指定行格式为分隔符
        FIELDS TERMINATED BY ',' --指定字段分隔符为逗号
    STORED AS TEXTFILE; --指定存储格式为文本文件

--创建一个名为car_data的表，包含11个字段
CREATE TABLE car_data (
                          Name STRING,
                          Location STRING,
                          Year INT,
                          Kilometers_Driven INT,
                          Fuel_Type STRING,
                          Transmission STRING,
                          Owner_Type STRING,
                          Mileage FLOAT,
                          Engine INT,
                          Power FLOAT,
                          Seats INT,
                          New_Price FLOAT,
                          Price FLOAT
)
    ROW FORMAT DELIMITED --指定行格式为分隔符
        FIELDS TERMINATED BY ',' --指定字段分隔符为逗号
    STORED AS TEXTFILE; --指定存储格式为文本文件
 --将car_data表中的数据按照数据类型导入到car_data_type中
 INSERT INTO car_data
 SELECT Name,Location,Year,Kilometers_Driven,Fuel_Type,Transmission,Owner_Type,
        cast(split(Mileage,' ') [0] as float),
        cast(split(Engine,' ') [0] as INT),
        cast(split(Power,' ') [0] as float),
        Seats,
        cast(split(New_Price,' ') [0] as float),
        Price FROM car_data_old;

--2、将二手车价格加载到数据表car_data中
LOAD DATA INPATH '/user/hive/car/car_price_all_notitle.csv'
    OVERWRITE INTO TABLE car_data_old;
--2.1 查看数据表结构
SELECT * FROM car_data_old LIMIT 5;

SELECT * FROM car_data LIMIT 5;

--2.1 查看数据集的大小
SELECT COUNT(*) AS count FROM car_data_old;

--2.2 查看数据集中各个字段的最小值、最大值、平均值、标准差
SELECT MIN(Year) AS min_year,
       MAX(Year) AS max_year,
       AVG(Year) AS avg_year,
       STDDEV(Year) AS std_year
FROM car_data_old;
SELECT MIN(Kilometers_Driven) AS min_km,
       MAX(Kilometers_Driven) AS max_km,
       AVG(Kilometers_Driven) AS avg_km,
       STDDEV(Kilometers_Driven) AS std_km
FROM car_data_old;

SELECT MIN(cast(split(Mileage,' ') [0] as float)) AS min_mileage,
       MAX(cast(split(Mileage,' ') [0] as float)) AS max_mileage,
       AVG(cast(split(Mileage,' ') [0] as float)) AS avg_mileage,
       STDDEV(cast(split(Mileage,' ') [0] as float)) AS std_mileage
FROM car_data_old;

SELECT MIN(cast(split(Engine,' ') [0] as float)) AS min_engine,
       MAX(cast(split(Engine,' ') [0] as float)) AS max_engine,
       AVG(cast(split(Engine,' ') [0] as float)) AS avg_engine,
       STDDEV(cast(split(Engine,' ') [0] as float)) AS std_engine
FROM car_data_old;

SELECT MIN(cast(split(Power,' ') [0] as float)) AS min_power,
       MAX(cast(split(Power,' ') [0] as float)) AS max_power,
       AVG(cast(split(Power,' ') [0] as float)) AS avg_power,
       STDDEV(cast(split(Power,' ') [0] as float)) AS std_power
FROM car_data_old;

SELECT MIN(cast(split(New_Price,' ') [0] as float)) AS min_seats,
       MAX(cast(split(New_Price,' ') [0] as float)) AS max_seats,
       AVG(cast(split(New_Price,' ') [0] as float)) AS avg_seats,
       STDDEV(cast(split(New_Price,' ') [0] as float)) AS std_seats
FROM car_data_old;

SELECT MIN(Price) AS min_price,
       MAX(Price) AS max_price,
       AVG(Price) AS avg_price,
       STDDEV(Price) AS std_price
FROM car_data_old;

--查看数据集中各个字段的中位数
SELECT  PERCENTILE(Year,0.5) AS median_year
FROM car_data_old;

SELECT PERCENTILE(Kilometers_Driven,0.5) AS median_km FROM car_data_old;

SELECT PERCENTILE(cast(split(Mileage,' ') [0] as BIGINT),0.5) AS median_mileage FROM car_data_old;

SELECT PERCENTILE(cast(split(Engine,' ') [0] as BIGINT),0.5) AS median_engine FROM car_data_old;

SELECT PERCENTILE(cast(split(Power,' ') [0] as BIGINT),0.5) AS median_power FROM car_data_old;

SELECT PERCENTILE(Seats,0.5) AS median_seats FROM car_data_old;

SELECT PERCENTILE(cast(split(New_Price,' ') [0] as BIGINT),0.5) AS median_new_price FROM car_data_old;

SELECT PERCENTILE(cast(Price as BIGINT),0.5) AS median_price FROM car_data_old;

--查看数据集中各个分类字段的频数和比例
SELECT Name,COUNT(*) AS count,COUNT(*)/COUNT(*) OVER() *100.0 as percentage
FROM car_data
GROUP BY Name
ORDER BY count DESC;

SELECT Location,COUNT(*) AS count,COUNT(*)/COUNT(*) OVER() *100.0 as percentage
FROM car_data
GROUP BY Location
ORDER BY count DESC;

SELECT Fuel_Type,COUNT(*) AS count,COUNT(*)/COUNT(*) OVER() *100.0 as percentage
FROM car_data
GROUP BY Fuel_Type
ORDER BY count DESC;

SELECT Transmission,COUNT(*) AS count,COUNT(*)/COUNT(*) OVER() *100.0 as percentage
FROM car_data
GROUP BY Transmission
ORDER BY count DESC;

SELECT Owner_Type,COUNT(*) AS count,COUNT(*)/COUNT(*) OVER() *100.0 as percentage
FROM car_data
GROUP BY Owner_Type
ORDER BY count DESC;
/*可以使用Hive的正则表达式函数`regexp_extract`来提取字符串中的价格数字。具体的HQL语句如下：

```sql
SELECT regexp_extract('金融专享价:¥7.07万', '¥([0-9.]+)万', 1);
```

解释一下上述HQL语句：
- `regexp_extract`函数用于提取字符串中符合正则表达式的部分。
- 第一个参数是要提取的字符串，这里是'金融专享价:¥7.07万'。
- 第二个参数是正则表达式，这里是'¥([0-9.]+)万'，表示匹配以¥开头，以万结尾，中间是数字和小数点的部分。
- 第三个参数是要提取的子表达式的索引，这里是1，表示提取正则表达式中第一个用括号括起来的子表达式，即价格数字。

执行上述HQL语句后，将会返回提取出来的价格数字，即'7.07'。
*/
SELECT regexp_extract('金融专享价:¥7.07万', '¥([0-9.]+)万', 1);

SELECT percentile_approx(Price, 0.25) as price_25,
       percentile_approx(Price, 0.5) as price_50,
       percentile_approx(Price, 0.75) as price_75,
       percentile_approx(Power, 0.25) as power_25,
       percentile_approx(Power, 0.5) as power_50,
       percentile_approx(Power, 0.75) as power_75
FROM car_data;
CREATE TABLE car_cluster AS
SELECT *,
       CASE WHEN Price < 3.5 AND Power < 74 THEN 1
--根据上一步计算出的百分位数填入具体的值
            WHEN Price < 5.5 AND Power < 97 THEN 2
            WHEN Price < 9.95 AND Power < 138 THEN 3
            WHEN Price > 9.95 AND Power > 138 THEN 4
            ELSE 5 END AS cluster_label
FROM car_data;