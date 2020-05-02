package com.atguigu.hotitems

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import java.sql.Timestamp

object HotItemsSQL {
  def main(args: Array[String]): Unit = {
    // 1. 创建执行环境
    val conf: Configuration = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val settings = EnvironmentSettings.
      newInstance().useBlinkPlanner().inStreamingMode().build()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env, settings)

    // 2. 读取数据
    val inputPath = "HotItemAnalysis/src/main/resources/UserBehavior.csv"
    val stream = env.readTextFile(inputPath)
      .map(line => {
        val items: Array[String] = line.split(",")
        UserBehavior(
          items(0).trim.toLong, items(1).trim.toLong, items(2).trim.toInt, items(3).trim, items(4).trim.toLong)
      })
      .filter(_.behavior == "pv")
      // 由于是递增时间戳，使用递增timestamp即可
      .assignAscendingTimestamps(_.timestamp * 1000L)

    tEnv.createTemporaryView("user_behavior_tbl", stream,
      'itemId, 'timestamp.rowtime as 'ts)

    val result = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM (
        |    SELECT *,
        |        ROW_NUMBER() OVER (PARTITION BY window_end ORDER BY cnt DESC) as row_num
        |    FROM (
        |      SELECT itemId, count(itemId) as cnt, TUMBLE_START(ts, INTERVAL '1' HOUR) as window_end
        |      FROM user_behavior_tbl
        |      GROUP BY TUMBLE(ts, INTERVAL '1' HOUR), itemId
        |    ) top_tbl
        |) WHERE row_num <= 5
        |""".stripMargin
    )
    result.toRetractStream[(Long, Long, Timestamp, Long)].print("top 5 item")

    env.execute("Hot Item SQL")
  }
}
