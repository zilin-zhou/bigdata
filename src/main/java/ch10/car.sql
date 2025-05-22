show databases ;
create database cars;



use cars;
CREATE TABLE car_data(
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
)ROW FORMAT DELIMITED --指定行格式为分隔符
    FIELDS TERMINATED BY ',' --指定字段分隔符为逗号
    STORED AS TEXTFILE; --指定存储格式为文本文件

show tables ;

LOAD DATA INPATH '/tables/car_price_all_notitle.csv' OVERWRITE INTO TABLE car_data;

//计算各个特征字段的频数、百分比、均值、标准差等。如：查看不同地区的二手车数量和占比
SELECT Location, COUNT(*) AS count,
       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM car_data), 2) AS percentage
FROM car_data GROUP BY Location ORDER BY count DESC;

//查看不同年份的二手车数量和占比。
SELECT Year, COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM car_data), 2) AS percentage
FROM car_data
GROUP BY Year
ORDER BY count DESC;

-- 3、统计不同燃料类型的二手车数量和车辆平均销售价格。
SELECT Fuel_Type,AVG(price) as price_avg,count(Fuel_Type) as car_count
FROM car_data
GROUP BY Fuel_Type;

-- 4、查看数据集中各个字段的最小值、最大值、平均值、标准差。
SELECT MIN(Year) AS min_year, MAX(Year) AS max_year, AVG(Year) AS avg_year, STDDEV(Year) AS std_year FROM car_data;
SELECT MIN(Kilometers_Driven) AS min_km, MAX(Kilometers_Driven) AS max_km, AVG(Kilometers_Driven) AS avg_km, STDDEV(Kilometers_Driven) AS std_km FROM car_data;
SELECT MIN(Mileage) AS min_mileage, MAX(Mileage) AS max_mileage, AVG(Mileage) AS avg_mileage, STDDEV(Mileage) AS std_mileage FROM car_data;
SELECT MIN(Engine) AS min_engine, MAX(Engine) AS max_engine, AVG(Engine) AS avg_engine, STDDEV(Engine) AS std_engine FROM car_data;
SELECT MIN(Power) AS min_power, MAX(Power) AS max_power, AVG(Power) AS avg_power, STDDEV(Power) AS std_power FROM car_data;
SELECT MIN(Seats) AS min_seats, MAX(Seats) AS max_seats, AVG(Seats) AS avg_seats, STDDEV(Seats) AS std_seats FROM car_data;
SELECT MIN(New_Price) AS min_new_price, MAX(New_Price) AS max_new_price, AVG(New_Price) AS avg_new_price, STDDEV(New_Price) AS std_new_price FROM car_data;
SELECT MIN(Price) AS min_price, MAX(Price) AS max_price, AVG(Price) AS avg_price, STDDEV(Price) AS std_price FROM car_data;

-- 5、查看数据集中各个字段的中位数。
SELECT PERCENTILE(Year,0.5) AS median_year FROM car_data;
SELECT PERCENTILE(Kilometers_Driven,0.5) AS median_km FROM car_data;
--SELECT PERCENTILE(Engine,0.5) AS median_engine FROM car_data;
SELECT PERCENTILE(Seats,0.5) AS median_seats FROM car_data;
-- SELECT PERCENTILE_APPROX(Mileage,0.5) AS median_mileage FROM car_data;
-- SELECT PERCENTILE_APPROX(Power,0.5) AS median_power FROM car_data;
-- SELECT PERCENTILE_APPROX(New_Price,0.5) AS median_new_price FROM car_data;
SELECT PERCENTILE_APPROX(Price,0.5) As median_price FROM car_data;

-- 6、相关系数分析。
--
--         (1)创建相关性分析结果表。
CREATE TABLE feature_correlation (
                                     feature1 STRING,
                                     feature2 STRING,
                                     correlation FLOAT
);

--         (2)二手车数据集中各个字段与汽车生产年份之间的相关系数。

-- REGEXP_EXTRACT(column, '([0-9.]+)', 1)：提取字符串中的数字部分（支持带小数点）。
--
-- CAST(... AS DOUBLE)：将提取结果转换为数值类型。
--
-- 加入 WHERE 过滤 NULL：避免因 NULL 值影响相关性计算。

SELECT
    CORR(Year, Kilometers_Driven) AS corr_year_km,
    CORR(Year, CAST(REGEXP_EXTRACT(Mileage, '([0-9.]+)', 1) AS DOUBLE)) AS corr_year_mileage,
    CORR(Year, CAST(REGEXP_EXTRACT(Engine, '([0-9.]+)', 1) AS DOUBLE)) AS corr_year_engine,
    CORR(Year, CAST(REGEXP_EXTRACT(Power, '([0-9.]+)', 1) AS DOUBLE)) AS corr_year_power,
    CORR(Year, CAST(Seats AS INT)) AS corr_year_seats,
    CORR(Year, CAST(REGEXP_EXTRACT(New_Price, '([0-9.]+)', 1) AS DOUBLE)) AS corr_year_new_price,
    CORR(Year, CAST(Price AS DOUBLE)) AS corr_year_price
FROM car_data
WHERE Mileage IS NOT NULL AND Engine IS NOT NULL AND Power IS NOT NULL AND New_Price IS NOT NULL AND Price IS NOT NULL;


-- 7、特征关系可视化分析。

--         (1)绘制散点图：价格与行驶公里数的关系
SELECT Kilometers_Driven, Price
FROM car_data
WHERE Kilometers_Driven IS NOT NULL AND Price IS NOT NULL;

--         (2)绘制箱线图：不同座位数的价格分布
SELECT Seats, Price
FROM car_data
WHERE Seats IS NOT NULL AND Price IS NOT NULL;
