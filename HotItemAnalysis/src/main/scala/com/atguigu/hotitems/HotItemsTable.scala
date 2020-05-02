package com.atguigu.hotitems

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table, Tumble}
import java.sql.Timestamp

object HotItemsTable {
  def main(args: Array[String]): Unit = {
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

    val itemTBL = tEnv.fromDataStream(stream, 'timestamp.rowtime, 'itemId)

    val cntTBL: DataStream[(Long, Long, Timestamp)] = itemTBL
      .window(Tumble over 60.minutes on 'timestamp as 'w)
      .groupBy('itemId, 'w)
      .aggregate('itemId.count as 'cnt)
      .select('itemId, 'cnt, 'w.end as 'window_end)
      .toAppendStream[(Long, Long, Timestamp)]

    tEnv.createTemporaryView("cnt_tbl", cntTBL, 'itemId, 'cnt, 'window_end)

    val topN: Table = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM (
        |    SELECT *,
        |        ROW_NUMBER() OVER (PARTITION BY window_end ORDER BY cnt DESC) as row_num
        |    FROM cnt_tbl)
        |WHERE row_num <= 5
        |""".stripMargin)

    topN.toRetractStream[(Long, Long, Timestamp, Long)].print("top 5 items")
    env.execute("Hot Items Table")
  }
}
