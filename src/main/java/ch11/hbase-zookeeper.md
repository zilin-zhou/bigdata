**一、下载hbase 和 zookeeper**

```
//hbase 下载地址
https://dlcdn.apache.org/hbase/2.6.2/hbase-2.6.2-src.tar.gz
// zookeeper 下载地址
https://dlcdn.apache.org/zookeeper/zookeeper-3.8.4/apache-zookeeper-3.8.4-bin.tar.gz

// 没有魔法可能下载很慢，可以去学习通下载。
```

**二、上传到虚拟机中进行解压**

```
cd /opt/apps

rz apache-zookeeper-3.8.4-bin.tar.gz   //上传zookeeper
rz hbase-2.6.2-src.tar.gz              //上传Hbase

//解压
tar -zxvf apache-zookeeper-3.8.4-bin.tar.gz
tar -zxvf hbase-2.6.2-src.tar.gz

//配置环境变量， 三台主机的环境变量都要修改
vim /etc/profile   

//追加以下内容到变量文件后面
#Hbase Zookeeper
export ZOOKEEPER_HOME=/opt/apps/apache-zookeeper-3.8.4-bin
export HBASE_HOME=/opt/apps/hbase-2.6.2
export PATH=$PATH:$ZOOKEEPER_HOME/bin:$HBASE_HOME/bin

//生效环境变量
source /etc/profile
```

**三、配置zookeeper**

```


// 修改 zookeeper 配置文件
cd /opt/apps/apache-zookeeper-3.8.4-bin/conf/

cp zoo_sample.cfg zoo.cfg

vim zoo.cfg
//追加下面内容
server.1=node01:2888:3888
server.2=node02:2888:3888
server.3=node03:2888:3888
dataDir=/opt/data/zookeeper   //这个时zookeeper数据文件目录，需要自己创建。哪里创建都行。三台主机尽量保持一样。

//复制到主机node02 和node03
scp /opt/apps/apache-zookeeper-3.8.4-bin node02:/opt/apps/
scp /opt/apps/apache-zookeeper-3.8.4-bin node03:/opt/apps/

// 在上面的目录dataDir写入myid执行文件。
echo 1 > /opt/data/zookeeper/myid     //node01执行
echo 2 > /opt/data/zookeeper/myid     //node02执行
echo 3 > /opt/data/zookeeper/myid     //node03执行

//启动zookeeper
zkServer.sh start 
// 查看状态
zkServer.sh status 

2个Mode：follwoer
1个Mode：leader
```

**四、配置hbase**

```
vim /opt/apps/hbase-2.6.2/conf/regionserversvim /opt/apps/hbase-2.6.2/conf/regionservers//修改配置文件
vim /opt/apps/hbase-2.6.2/conf/hbase-site.xml

//追加下面内容
  <property>
  <name>hbase.rootdir</name>
  <value>hdfs://node01:9000/hbase</value>
</property>
<property>
  <name>hbase.zookeeper.quorum</name>
  <value>node01:2181,node02:2181,node03:2181</value>
</property>

把文件中本来就有的属性 hbase.cluster.distributed 改为true

//配置 环境变量文件
vim /opt/apps/hbase-2.6.2/conf/hbase-env.sh

export HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP="true"
export JAVA_HOME=/opt/apps/jdk1.8.0_381
export HBASE_MANAGES_ZK=false

// 修改regionservers文件
vim /opt/apps/hbase-2.6.2/conf/regionservers

//删掉里面内容，加入
node01
node02
node03

//复制到主机node02 和node03
scp /opt/apps/hbase-2.6.2 node02:/opt/apps/
scp /opt/apps/hbase-2.6.2 node03:/opt/apps/

//启动hbase
start-hbase.sh
hbase shell
```
