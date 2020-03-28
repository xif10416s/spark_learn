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
*   spark/ml --- demo 算法

      |类名      | 算法名 |   适用场景  | 数据格式 |
      |-|-|-|-|
      |AFTSurvivalRegressionExample | 加速失效时间（AFT）模型 | 根据特征参数预测存活时间，如根据身体特征预测存活时间 |  1.218 （存活时间）, 1.0 （结局：1 死亡，2 缺失数据，尚存货）, Vectors.dense(1.560, -0.605) 【特征列表】|
      |ALSExample | 最小二乘法 | 推荐系统 | userId: Int, movieId: Int, rating: Float, timestamp: Long |
      |BinarizerExample | Binarizer | 根据阈值进行二值化,大于阈值的为1.0,小于等于阈值的为0.0 |  |
      |BisectingKMeansExample | 二分K均值算法 | 分类，对误差平方和最大的簇进行再一次的划分 | 0 1:0.0 2:0.0 3:0.0 |
      |BucketizerExample | 根据指定的分位点进行分桶 |   将人的体重进行离散化，将人群分为：【瘦，标准，胖】 |  |
      |ChiSqSelectorExample | 卡方检验选择特征 |  特征选择 | 第一位是分类，后面是标签：值，要求必须是数字  |
      |ChiSquareTestExample | 假设检验方法 | 统计样本的实际观测值与理论推断值之间的偏离程度，用于两变量间的关联分析、频属分析的拟合优度检验等 |  |
      |CorrelationExample | TODO | 计算矩阵两两相关系数 |  |
      |CountVectorizerExample | TODO | 算法是将文本向量转换成稀疏表示打数值向量（字符频率向量），把频率高的单词排在前面 |  |
      |DCTExample | 离散余弦变换(Discrete Cosine Transform | 主要用于将数据或图像的压缩，能够将空域的信号转换到频域上，具有良好的去相关性的性能 |  |
      |DecisionTreeClassificationExample | 决策树分类 | 监督学习，分类场景,目标变量为分类型数值 | 第一列分类0 or 1, 其他特征值 |
      |DecisionTreeExample | 参数化配置，综合了分类和回归 |  |  |
      |DecisionTreeRegressionExample | 决策树回归预测 | 目标变量为连续型变量 |  |
      |ElementwiseProductExample | 元素智能乘积 | 每一个输入向量乘以一个给定的“权重”向量,对数据集的每一列进行缩放 |  |
      |FeatureHasherExample | 特征哈希 | 将任意特征转换为向量或矩阵中的索引 |  |
      |FPGrowthExample | 关联规则 | 构造一个树结构来压缩数据记录，使得挖掘频繁项集只需要扫描两次数据记录，而且该算法不需要生成候选集合 |  |
      |GaussianMixtureExample | GMM混合高斯模型 | 7 |  |
      |GBTExample | Gradient-Boosted Trees | 基于Stochastic Gradient Boosting(随机梯度提升)，用于分类预测 |  |
      |GeneralizedLinearRegressionExample | 广义线性模型 | 7 |  |
      |IndexToStringExample | 索引转文本 | 7 |  |
      |InteractionExample | TODO | 7 |  |
      |IsotonicRegressionExample | 保序回归算法 | TODO |  |
      |KMeansExample | 聚类算法 | 分类预测 | 分类，特征列表 |
      |LDAExample | Latent Dirichlet Allocation | LDA是文本语义分析中应用广泛的一个模型，比如找相似文档等应用场景 |  |
      |LinearRegressionExample | 线性回归例子 | 根据特征预测，身高预测体重，面积预测房价 |  |
      |LinearRegressionWithElasticNetExample | $1 | 只要数据线性相关，用LinearRegression拟合的不是很好，需要正则化，可以考虑使用岭回归(L2), 如何输入特征的维度很高,而且是稀疏线性关系的话， 岭回归就不太合适,考虑使用Lasso回归。 |  |
      |LinearSVCExample | $1 | 7 |  |
      |草莓 | $1 | 7 |  |
      |草莓 | $1 | 7 |  |
      |草莓 | $1 | 7 |  |
      |草莓 | $1 | 7 |  |
      |草莓 | $1 | 7 |  |
      |草莓 | $1 | 7 |  |
      
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
*   spark.stream -- spark 流处理
    *   batch  -- DStream 微批处理方式
        *    kakfa -- kafka对接
             *    DirectKafkaExactOnceWordCount -- kafka直连，通过数据库实现只一次语义实现

