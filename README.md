# 基于聚宽的期货数据平台

## 目录结构

+ `strategy` 所有策略代码
  + `stop_loss.py` 基于止损值的策略
+ `mq` 消息队列实现类 生产者-消费者模式
  + `consumer.py` 消费者
  + `producer.py` 生产者
+ `model` 抽象出的业务类
  + `stock.py` 股票类
  + `stop_loss.py` 止损值指标类
+ `config/dev.ini` 数据库配置文件
+ `test` 非正式代码
  + com.py 消费者测试类
  + pub.py 生产者测试类
  + consumer.py 待修改的消费者类
  + producer.py 待修改的生产者类
  + trade.py 模拟实时分钟数据计算每分钟的止损值
+ `util` 工具类
  + `create_table.py` 创建表
  + `init_table.py` 初始化表数据
  + `db_util.py` 数据库工具类
  + `jquant_util.py` 聚宽认证装饰器
  + `redis_util.py` 初始化redis连接池
  + `mysingleton.py` 单例装饰器方法
  + `order.py` wh9聚宽接口，包括账号关联和按照参数下单
+ `service.py` 服务端实时从聚宽获取分钟级数据, 并缓存到`redis`, 通过`redis`构建消息队列
+ `client.py` 客户端接收服务端推送的消息(分钟级期货数据), 通过策略使用数据生成买入卖出信号

## redis测试服务器

将机房的bigdata1服务器作为redis的测试服务器, 连接信息如下: 

Host: 122.207.108.56

Port: 12479

**如何修改redis配置文件**

```bash
sudo /etc/init.d/redis stop
sudo vim /etc/redis/redis.conf
sudo /etc/init.d/redis start
```

## How to run

```bash
python ./client.py
python ./service.py
```

## To do

+ [ ] 将获取历史数据的功能部署为网络接口 (通过fastapi等web框架实现)
+ [ ] 通过定时任务更新基础表的数据
+ [ ] 寻找方法将期货品种保证金, 最低手数, 交易单位字段添加到future_underlying表中
+ [ ] 增加回测功能 (计算合约保证金, 手续费)
