-- 创建清洗后的数据表
CREATE TABLE cleaned_car_data AS
SELECT
    Name,
    Location,
    Year,
    Kilometers_Driven,
    Fuel_Type,
    Transmission,
    Owner_Type,
    Mileage,
    Engine,
    Power,
    Seats,
    New_Price,
    Price
FROM
    car_data
WHERE
    Name IS NOT NULL
  AND Location IS NOT NULL
  AND Year IS NOT NULL
  AND Kilometers_Driven IS NOT NULL
  AND Fuel_Type IS NOT NULL
  AND Transmission IS NOT NULL
  AND Owner_Type IS NOT NULL
  AND Mileage IS NOT NULL
  AND Engine IS NOT NULL
  AND Power IS NOT NULL
  AND Seats IS NOT NULL
  AND Price IS NOT NULL;

SELECT COUNT(*) - COUNT(Name) AS missing_name,
       COUNT(*) - COUNT(Location) AS missing_location,
       COUNT(*) - COUNT(Year) AS missing_year,
       COUNT(*) - COUNT(Kilometers_Driven)
           AS missing_kilometers_driven,
       COUNT(*) - COUNT(Fuel_Type) AS missing_fuel_type,
       COUNT(*) - COUNT(Transmission) AS missing_transmission,
       COUNT(*) - COUNT(Owner_Type) AS missing_owner_type,
       COUNT(*) - COUNT(Mileage) AS missing_mileage,
       COUNT(*) - COUNT(Engine) AS missing_engine,
       COUNT(*) - COUNT(Power) AS missing_power,
       COUNT(*) - COUNT(Seats) AS missing_seats,
       COUNT(*) - COUNT(New_Price) AS missing_new_price,
       COUNT(*) - COUNT(Price) AS missing_price
FROM cleaned_car_data;






SELECT MIN(Year) AS min_year,
       MAX(Year) AS max_year,
       AVG(Year) AS avg_year,
       STDDEV(Year) AS std_year,
       MIN(Kilometers_Driven) AS min_kilometers_driven,
       MAX(Kilometers_Driven) AS max_kilometers_driven,
       AVG(Kilometers_Driven) AS avg_kilometers_driven,
       STDDEV(Kilometers_Driven) AS std_kilometers_driven,
       MIN(Mileage) AS min_mileage,
       MAX(Mileage) AS max_mileage,
       AVG(Mileage) AS avg_mileage,
       STDDEV(Mileage) AS std_mileage,
       MIN(Engine) AS min_engine,
       MAX(Engine) AS max_engine,
       AVG(Engine) AS avg_engine,
       STDDEV(Engine) AS std_engine,
       MIN(Power) AS min_power,
       MAX(Power) AS max_power,
       AVG(Power) AS avg_power,
       STDDEV(Power) AS std_power,
       MIN(Seats) AS min_seats,
       MAX(Seats) AS max_seats,
       AVG(Seats) AS avg_seats,
       STDDEV(Seats) AS std_seats,
       MIN(New_Price) AS min_new_price,
       MAX(New_Price) AS max_new_price,
       AVG(New_Price) AS avg_new_price,
       STDDEV(New_Price) AS std_new_price,
       MIN(Price) AS min_price,
       MAX(Price) AS max_price,
       AVG(Price) AS avg_price,
       STDDEV(Price) AS std_price
FROM cleaned_car_data;
SELECT Name, Location, Year, Kilometers_Driven, Fuel_Type, Transmission, Owner_Type
FROM cleaned_car_data
GROUP BY Name, Location, Year, Kilometers_Driven, Fuel_Type, Transmission, Owner_Type
HAVING COUNT(*) > 1;

-- 特征处理和转换
DROP TABLE processed_car_data;
CREATE TABLE processed_car_data (
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
                                  Price DECIMAL(10, 2),
                                  Brand STRING,
                                  Model STRING,
                                  Mileage_Value DECIMAL(10, 2),
                                  Engine_Value DECIMAL(10, 2),
                                  Power_Value DECIMAL(10, 2)
)
    STORED AS ORC
    TBLPROPERTIES (
        'transactional'='true'
        );

INSERT INTO updated_car_data
SELECT
    Name,
    Location,
    Year,
    Kilometers_Driven,
    Fuel_Type,
    Transmission,
    Owner_Type,
    Mileage,
    Engine,
    Power,
    Seats,
    New_Price,
    Price,
    SUBSTRING_INDEX(Name, ' ', 1) AS Brand,
    REGEXP_REPLACE(Name, '[^0-9.]', '') AS Model,
    CAST(REGEXP_REPLACE(Mileage, '[^0-9.]', '') AS DECIMAL(10, 2)) AS Mileage_Value,
    CAST(REGEXP_REPLACE(Engine, '[^0-9.]', '') AS DECIMAL(10, 2)) AS Engine_Value,
    CAST(REGEXP_REPLACE(Power, '[^0-9.]', '') AS DECIMAL(10, 2)) AS Power_Value
FROM
    cleaned_car_data
        ;


UPDATE processed_car_data
SET Kilometers_Driven = (SELECT AVG(Kilometers_Driven) FROM car_data)
WHERE Kilometers_Driven < 0 OR Kilometers_Driven > 500000;


SELECT Location, COUNT(*) AS count, ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM car_data), 2) AS percentage
FROM car_data
GROUP BY Location
ORDER BY count DESC;
SELECT Year, COUNT(*) AS count,ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM car_data), 2) AS percentage
FROM car_data
GROUP BY Year
ORDER BY count DESC;
SELECT Location, ROUND(AVG(Price), 2) AS avg_price, ROUND(STDDEV(Price), 2) AS std_price
FROM car_data
GROUP BY Location
ORDER BY avg_price DESC;
SELECT Year, ROUND(AVG(Price), 2) AS avg_price, ROUND(STDDEV(Price), 2) AS std_price
FROM car_data
GROUP BY Year
ORDER BY avg_price DESC;

--价格最高的10辆车
SELECT Name, Location, Year, Price
FROM car_data
ORDER BY Price DESC LIMIT 10;

--里程最低的10辆车
SELECT Name, Location, Year, Kilometers_Driven
FROM car_data
ORDER BY Kilometers_Driven ASC LIMIT 10;

--销量最高的10个城市
SELECT Location, COUNT(*) AS count
FROM car_data
GROUP BY Location
ORDER BY count DESC LIMIT 10;

SELECT Kilometers_Driven, Price
FROM car_data
WHERE Kilometers_Driven IS NOT NULL AND Price IS NOT NULL;
SELECT Seats, Price
FROM car_data
WHERE Seats IS NOT NULL AND Price IS NOT NULL;
select * from car_data limit 3;
select * from cleaned_car_data limit 3;
select * from updated_car_data limit 3;
CREATE TABLE car_train AS
SELECT * FROM updated_car_data WHERE rand() < 0.8;
CREATE TABLE car_test AS
SELECT * FROM updated_car_data WHERE rand() >= 0.8;

-- 筛选出特定品牌的二手车数据子集
SELECT
    Brand,
    AVG(Price) AS AvgPrice,
    MAX(Year) AS MaxYear
FROM
    updated_car_data
WHERE
        Brand IN ('Maruti', 'Hyundai', 'Honda') -- 选择特定品牌
GROUP BY
    Brand
HAVING
        AVG(Price) > 500000 -- 价格均值大于500,000
   AND MAX(Year) > 2015 -- 最大年份大于2015
;

ADD JAR hdfs:///opt/module/hivemall-all-0.6.2-incubating-SNAPSHOT.jar;

-- 创建预测函数
CREATE FUNCTION logress_price AS
    'hivemall.regression.LogressUDTF'
    USING JAR 'hdfs:///opt/module/hivemall-all-0.6.2-incubating-SNAPSHOT.jar';

set hivevar:hivemall_jar=hdfs:///opt/module/hivemall-all-0.6.2-incubating-SNAPSHOT.jar;
--source /tmp/define-all-as-permanent.hive;

show functions "hivemall.*";

-- 使用预测函数进行预测
SELECT
    Name,
    Location,
    Year,
    Kilometers_Driven
--    ,predict_price(feature_1, feature_2, ...)
FROM
    car_data;


SHOW FUNCTIONS LIKE '%hivemall%';

SELECT MIN(Year) AS min_year, MAX(Year) AS max_year, AVG(Year) AS avg_year, STDDEV(Year) AS std_year FROM car_data;
SELECT MIN(Kilometers_Driven) AS min_km, MAX(Kilometers_Driven) AS max_km, AVG(Kilometers_Driven) AS avg_km, STDDEV(Kilometers_Driven) AS std_km FROM car_data;

SELECT MIN(Mileage) AS min_mileage, MAX(Mileage) AS max_mileage, AVG(Mileage) AS avg_mileage, STDDEV(Mileage) AS std_mileage FROM car_data;

SELECT MIN(Engine) AS min_engine, MAX(Engine) AS max_engine, AVG(Engine) AS avg_engine, STDDEV(Engine) AS std_engine FROM car_data;

SELECT MIN(Power) AS min_power, MAX(Power) AS max_power, AVG(Power) AS avg_power, STDDEV(Power) AS std_power FROM car_data;

SELECT MIN(Seats) AS min_seats, MAX(Seats) AS max_seats, AVG(Seats) AS avg_seats, STDDEV(Seats) AS std_seats FROM car_data;


SELECT MIN(New_Price) AS min_new_price, MAX(New_Price) AS max_new_price, AVG(New_Price) AS avg_new_price, STDDEV(New_Price) AS std_new_price FROM car_data;

SELECT MIN(Price) AS min_price, MAX(Price) AS max_price, AVG(Price) AS avg_price, STDDEV(Price) AS std_price FROM car_data;


SELECT Year,
       ROUND(AVG(Price), 2) AS avg_price,
       ROUND(STDDEV(Price), 2) AS std_price
FROM car_data
GROUP BY Year
ORDER BY avg_price DESC;

SELECT PERCENTILE(Year,0.5) AS median_year ,
 PERCENTILE(Kilometers_Driven,0.5) AS median_km,
 PERCENTILE(Engine,0.5) AS median_engine ,
 PERCENTILE(Seats,0.5) AS median_seats
 FROM   car_data;

SELECT PERCENTILE(Year,0.5) AS median_year FROM car_data;

SELECT
    percentile_approx(Power,0.5) AS median_power ,
    percentile_approx(New_Price,0.5) AS median_new_price,
    percentile_approx(Mileage,0.5) AS median_mileage ,
    percentile_approx(Price,0.5) As median_price
FROM car_data;

SELECT seats,count(seats) as car_count
FROM car_data
GROUP BY seats
HAVING seats>0;

SELECT Fuel_Type,AVG(price) as price_avg,count(Fuel_Type) as car_count
,round(count(Fuel_Type)/(SELECT COUNT(*) FROM car_data),2) as percentage
FROM car_data
GROUP BY Fuel_Type;

SELECT Year, COUNT(*) AS count,
       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM car_data), 2) AS percentage
FROM car_data
GROUP BY Year
ORDER BY count DESC;


SELECT Location, COUNT(*) AS count,
       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM car_data), 2) AS percentage
FROM car_data
GROUP BY Location
ORDER BY count DESC;


SELECT Location,
       ROUND(AVG(Price), 2) AS avg_price,
       ROUND(STDDEV(Price), 2) AS std_price
FROM car_data
GROUP BY Location
ORDER BY avg_price DESC;