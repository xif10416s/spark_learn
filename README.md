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
    *   test -- 测试启动类目录
        *   test.network 模拟 NettyRpcEnv使用TransportContext，创建netty server , netty client 相互通讯
            *   客户端连接服务器发送一条消息，服务端接收消息，并响应客户端 
            *   没有Dispatch相关逻辑，忽略了数据传输的格式，直接最简单的消息
            *   基础的调用流程，通讯流程
    