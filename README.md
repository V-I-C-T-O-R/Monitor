`Monitor for hive,hdfs,mysql.....`
--------------------------------------
Monitor data for stable of hive,mysql,hdfs etc
###监控范围:
####1.HDFS特定位置数据监控
####2.Hive表数据监控
####3.Mysql数据分区情况监控
####4.Azkaban执行流监控
###功能包含:
####1.查询指定位置的Hive表、Hdfs位置、Mysql分区和Azkaban执行流的状态
####2.通过邮件的方式发送给发送人(采用sendmail方式),包含失败日志
####3.通过微信接口发送微信提醒(内部功能，如需实现需要自己实现)
####4.Azkaban任务API手动重启
###使用说明:
#####直接执行项目打包后的jar文件,并指定天数即可，如：java -jar datamonitor.jar 3。天数的含义为监控前几天的资源状态情况
####`须知:`
1.log4j2显示模式可以自己调节，默认为控制台打印
2.鉴于隐私性，各种配置文件已经私有化，需要按实际情况配置
