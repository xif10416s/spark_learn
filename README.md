#  spark 测试
##  2.2 版本
*  scala\core\base
    *   scala基础语法：反射，匹配 match,集合，函数，泛型
    *   breeze:矩阵运算，作图
*  spark\core\
    *   mock\simpleprocess :  spark 基础流程简单模拟代码
    *   rdd基础api测试
*  spark\graph
    *   图计算sample代码
*  spark\ml   -- 新库，基于dataframe
    *   spark机器学习sample代码   
*  spark\mllib -- 旧库，基于RDD
    *   spark机器学习sample
*  spark\sql
    *   catalyst: 简单代码，模拟了dataset的处理流程
    *   datasets: 基础api测试
*  performance
    *   全局代码生成性能测试
*  stream
    *   batch：批处理方式测试
    *   structured: table方式测试
    
    
## 2.4.2 版本
*   delta :
    *   spark 数据湖测试
*   spark/ml/ts
    *   git自定义时序模型
*   spark/ml/mmlspark
    *   微软机器学习库Azure
*   spark/core/mock/communication  -- spark 通信模块
    *   network -- 模拟实现netty封装 , 省略了一些netty 通讯参数的配置
        *   只模拟了RPC调用场景
        *   TODO流处理，数据拉取
    *   core -- spark 在network实现上的封装
    *   test -- 测试启动类目录
        *   test.network 模拟 NettyRpcEnv使用TransportContext，创建netty server , netty client 相互通讯
            *   客户端连接服务器发送一条消息，服务端接收消息，并响应客户端 
            *   没有Dispatch相关逻辑，忽略了数据传输的格式，直接最简单的消息
            *   基础的调用流程，通讯流程
        *   test.core 测试core的模拟代码，模拟nettyRpcEnv的使用
            *   模拟driver注册到master的过程
            *   TestMaster: 模拟Master启动NettyServer,实现了ThreadSafeRpcEndpoint的receive接口，处理driver发送的注册消息
            *   TestDriver：模拟Driver端，通过ClientEndpointMock给master发送注册消息，并接收master发送的注册成功信息
                *   driver 发送的注册消息 和 master发送的注册完成消息都时单向的消息。
    *   TODO 其他endpoint逻辑
*   spark/sql
    *   sources/v2 -- 数据源接口v1扩展（https://www.iteblog.com/archives/2579.html）  
        *   reader
            *   支持功能接口
                *   SupportsPushDownFilters -- 支持谓词下推过滤
                *   SupportsPushDownRequiredColumns -- 列裁剪
                *   SupportsReportPartitioning -- 上报数据分区
                *   SupportsReportStatistics -- 上报统计数据
                *   SupportsScanColumnarBatch -- 批量列扫描
*   deeplearn/bigdl  深度学习bigdl库
    *   Intel开源了基于Apache Spark的分布式深度学习框架BigDL
    *   examples/lenet  基于MNIST数据集的数字识别
    
