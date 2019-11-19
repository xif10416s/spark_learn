package org.fxi.test.spark.sql.sources.v2.reader

import org.apache.spark.sql.SparkSession

/**
  * == Physical Plan ==
  * *(1) Project [id#0L, create_time#1, update_time#2, deleted#3, delete_time#4, version#5, login_name#6, user_name#7, user_type#8, password#9, phone#10, email#11, avatar#12, gender#13, post#14, status#15, remark#16, login_fail_num#17, locked_time#18, logon_at#19, message_push_enabled#20, wechat#21, wechat_open_id#22]
  * +- *(1) Filter NOT (id#0L = 28)   <== 不支持 算子下推，所以在数据扫描之后过滤
  * // id > 20 支持算子下推，所以再扫描时，sql里面过滤
  * +- *(1) ScanV2 DefaultSource[id#0L, create_time#1, update_time#2, deleted#3, delete_time#4, wechat_open_id#22] (Filters: [isnotnull(id#0L), (id#0L > 20)], Options: [url=*********(redacted),paths=[],key=id,driver=org.postgresql.Driver,split=3,dbtable=sys_user,en...)
  * // 四个数据分区，按id切割
  * SELECT id FROM sys_user WHERE id >= 16 AND id <22 AND "id" IS NOT NULL AND "id" > 20
  * SELECT id FROM sys_user WHERE id >= 22 AND id <28 AND "id" IS NOT NULL AND "id" > 20
  * SELECT id FROM sys_user WHERE id >= 28 AND id <30 AND "id" IS NOT NULL AND "id" > 20
  * SELECT id FROM sys_user WHERE id >= 10 AND id <16 AND "id" IS NOT NULL AND "id" > 20
  */
object DefaultSourceTester {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("DefaultSourceTester")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .format("org.fxi.test.spark.sql.sources.v2.reader")
      .option("url", "jdbc:postgresql://192.168.100.21:5432/inc-fin-dev?user=inclusive-finance&password=SflKH4TLU*lvdR9Nn")
      .option("dbtable", "sys_user")
      .option("driver", "org.postgresql.Driver")
      .option("start","10")
      .option("end","30")
      .option("split","3")
      .option("key","id")
      .load().filter("id > 20 and id != 28")

    df.explain(true)
    df.count()
  }

}
