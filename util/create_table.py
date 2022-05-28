from util.db_util import get_connection



sql_create_future_basic = """
CREATE TABLE `future_basic`(`id` int(11) NOT NULL AUTO_INCREMENT,`code` varchar(16) DEFAULT NULL,`underlying` varchar(16) DEFAULT NULL,`name` varchar(32) DEFAULT NULL,`exchange` varchar(16) DEFAULT NULL,`start_date` datetime DEFAULT NULL,`end_date` datetime DEFAULT NULL,PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=7103 DEFAULT CHARSET=utf8mb4;"""

sql_create_future_daily = """
CREATE TABLE `future_daily` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(16) DEFAULT NULL,
  `open` float DEFAULT NULL,
  `close` float DEFAULT NULL,
  `low` float DEFAULT NULL,
  `high` float DEFAULT NULL,
  `pre_settle` float DEFAULT NULL,
  `settle` float DEFAULT NULL,
  `volume` float DEFAULT NULL,
  `money` float DEFAULT NULL,
  `open_interest` float DEFAULT NULL,
  `underlying` varchar(8) DEFAULT NULL,
  `trade_date` datetime NOT NULL,
  `pre_close` float DEFAULT NULL,
  `stop_loss` float DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=13485 DEFAULT CHARSET=utf8mb4;"""
sql_create_future_m = """
CREATE TABLE `future_m` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(16) DEFAULT NULL,
  `open` float DEFAULT NULL,
  `close` float DEFAULT NULL,
  `low` float DEFAULT NULL,
  `high` float DEFAULT NULL,
  `pre_settle` float DEFAULT NULL,
  `settle` float DEFAULT NULL,
  `volume` float DEFAULT NULL,
  `money` float DEFAULT NULL,
  `open_interest` float DEFAULT NULL,
  `underlying` varchar(8) DEFAULT NULL,
  `trade_date` datetime NOT NULL,
  `pre_close` float DEFAULT NULL,
  `stop_loss` float DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"""


sql_create_future_mapping = """
CREATE TABLE `future_mapping` (
  `id` int(16) NOT NULL AUTO_INCREMENT,
  `underlying` varchar(8) NOT NULL,
  `dominate_code` varchar(16) NOT NULL,
  `trade_date` datetime NOT NULL,
  `mapping_code` varchar(16) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `future_mapping_UN` (`mapping_code`,`trade_date`)
) ENGINE=InnoDB AUTO_INCREMENT=17011 DEFAULT CHARSET=utf8mb4;"""

sql_create_future_underlying = """
CREATE TABLE `future_underlying` (
  `id` int(4) NOT NULL AUTO_INCREMENT,
  `underlying` varchar(8) NOT NULL,
  `underlying_name` varchar(32) DEFAULT NULL,
  `exchange` varchar(16) DEFAULT NULL,
  `exchange_name` varchar(32) DEFAULT NULL,
  `dominate_code` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `future_underlying_UN` (`underlying`)
) ENGINE=InnoDB AUTO_INCREMENT=71 DEFAULT CHARSET=utf8mb4;"""


def drop_all(conn):
    sql_drop_future_basic = """DROP TABLE IF EXISTS `future_basic`"""
    sql_drop_future_daily = """DROP TABLE IF EXISTS `future_daily`;"""
    sql_drop_future_underlying = """DROP TABLE IF EXISTS `future_underlying`;"""
    sql_drop_future_m = """DROP TABLE IF EXISTS `future_m`;"""
    sql_drop_future_mapping = """DROP TABLE IF EXISTS `future_mapping`;"""
    conn.execute(sql_drop_future_basic)
    conn.execute(sql_drop_future_daily)
    conn.execute(sql_drop_future_underlying)
    conn.execute(sql_drop_future_m)
    conn.execute(sql_drop_future_mapping)



conn = get_connection()
drop_all(conn)
conn.execute(sql_create_future_basic)
conn.execute(sql_create_future_daily)
conn.execute(sql_create_future_mapping)
conn.execute(sql_create_future_underlying)
conn.execute(sql_create_future_m)
conn.close()
