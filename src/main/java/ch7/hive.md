以下操作都在node01上操作

hive  下载地址

```
https://dlcdn.apache.org/hive/hive-4.0.1/apache-hive-4.0.1-bin.tar.gz
```


1、安装mysql数据库。如果你们有mysql可以跳过此步骤，在哪安装的数据库都行。

```
yum -y install mysql
```

启动服务

```
systemctl start mysqld.service
```

进入mysql 修改密码

```mysql
mysql -uroo -p
ALTER USER 'root'@'localhost' IDENTIFIED BY 'new password';
```

创建数据库用来存储元数据

```
create database hive //创建hive数据库
create user zilin@'%' identified by '123456'; //创建新用户
grant all privileges on hive.* to zilin;
flush privileges;
```

2、rz命令上传hive的压缩包，目录大家可以随便选择

3、解压安装，也可以用3.x版本的也行，需要了自己下载

```tar
tar -zxvf apache-hive-4.0.1-bin.tar.gz
```

添加环境变量

```
vim /etc/profile
export HIVE_HOME=/etc/apps/apache-hive-4.0.1-bin
export PATH=$PATH:$HIVE_HOME/bin

source /etc/profile  //环境变量生效
```

4、修改hive-site.xml配置文件

```
vim /apache-hive-4.0.1-bin/conf/hive-site.xml     // 这个配置文件可能没有，直接vim创建就行
```

配置文件如下，需要把挂在的端口和对应的数据库名称、用户、密码、端口修改为自己的：

```
<?xml version="1.0"?>

<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
 <!-- jdbc 连接的 URL -->
<property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://node01:3306/hive?useSSL=false</value>
</property>
 <!-- jdbc 连接的 Driver-->
 <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>com.mysql.cj.jdbc.Driver</value>
</property>
<!-- jdbc 连接的 username-->
 <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>zilin</value>
 </property>
 <!-- jdbc 连接的 password -->
 <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>123456</value>
</property>

<!-- Hive 默认在 HDFS 的工作目录 -->

<property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/opt/data/hivedata</value>
</property>

<!-- 指定存储元数据要连接的地址 -->

<property>
    <name>hive.metastore.uris</name>
    <value>thrift://node01:9083</value>
 </property>
<!--指定hiveserver2连接的host-->
<property>
	<name>hive.server2.thrift.bind.host</name>
	<value>node01</value>
</property>
<property>
	<name>hive.server2.thrift.port</name>
	<value>10000</value>
</property>
</configuration>
```

6、添加 jdbc jar包

将  mysql-connector-java-8.0.26.jar 包rz上传到hive安装目录的lib/文件夹内

7、初始化hive

```
schematool -dbType mysql -initSchema
```

8、启动metastore 和 hiveserver 服务

```
hive --service metastore
hive --service hiveserver2
```

9、如果使用的3.x版本  运行hive命令就进去 hive cli。如果使用4.x 他会进入beeline，在beeline下我们使用jdbc连接到hive。

这里要启动你的集群。具体启动方式不在赘述。

jdbc连接数据库

```
beeline -u jdbc:hive2://node01:10000 -n root
```

连接成功hive安装完毕。
