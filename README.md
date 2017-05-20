# 从百度采集IP项目 #

- 采集IP信息，保存为text,再入mongoDB

----------


- 采集IP信息，入库MYSQL，并作分析
- 多进程（已完成）
- scrapy框架


## 相关SQL语句 ##
- 查询总记录数

    `SELECT COUNT(1) FROM ip_info;`

- 按照IP段进行查询

    `SELECT SUBSTRING_INDEX(ip,'.',2) ips, COUNT(1) counts FROM ip_info 
GROUP BY SUBSTRING_INDEX(ip,'.',2);`

# 修改bug #
1. 问题如下（2017-05-14）
![findall_bug](http://i.imgur.com/UjBH0PY.png)
应该匹配第一个结果结果值.


# scrapy_task数据库说明 #

ip_info表：
    
	CREATE TABLE `ip_info` (
	  `id` int(15) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增列',
	  `ip` varchar(15) NOT NULL COMMENT 'IP',
	  `addr` varchar(50) DEFAULT NULL COMMENT '归属地',
	  `carrieroperator` varchar(20) DEFAULT NULL COMMENT '运营商',
	  `jointime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '采集时间',
	  PRIMARY KEY (`id`)
	) ENGINE=InnoDB AUTO_INCREMENT=914751 DEFAULT CHARSET=utf8 COMMENT='IP信息表';


ip_city表:

	CREATE TABLE `ip_city` (
	  `id` int(15) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增列',
	  `city` varchar(50) DEFAULT NULL COMMENT '归属地',
	  PRIMARY KEY (`id`)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='归属地信息';

ip_carrier表：
	
	CREATE TABLE `ip_carrier` (
	  `id` int(15) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增列',
	  `carrieroperator` varchar(20) DEFAULT NULL COMMENT '运营商',
	  PRIMARY KEY (`id`)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='IP运营商信息';

ip_log_info表：

	CREATE TABLE `ip_log_info` (
	  `id` int(15) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增列',
		`ip_range` varchar(15) NOT NULL COMMENT 'IP段(AB)',
	  `city_count` int(10) DEFAULT NULL COMMENT '归属地个数',
		`city_finished` char(2) DEFAULT '否' COMMENT '归属地是否完成',
		`carrier_count` int(10) DEFAULT NULL COMMENT '运营商个数',
		`carrier_finished` char(2) DEFAULT '否' COMMENT '运营商是否完成',
	  PRIMARY KEY (`id`)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='IP采集信息统计进度表';
