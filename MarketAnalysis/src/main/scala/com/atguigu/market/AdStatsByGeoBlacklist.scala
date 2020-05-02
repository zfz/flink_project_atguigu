package com.atguigu.market

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 过滤恶意用户行为
 */
object AdStatsByGeoBlacklist {

  // 侧输出流tag，得到当前的黑名单
  val blackListOutputTag = new OutputTag[BlacklistWarning]("blacklist")

  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val conf: Configuration = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = this.getClass.getClassLoader.getResource("AdClickLog.csv")
    // 读取数据并转换成AdClickEvent
    val adEventStream: DataStream[AdClickEvent] = env.readTextFile(resource.getPath)
      .map(line => {
        val items = line.split(",")
        AdClickEvent(items(0).trim.toLong, items(1).trim.toLong, items(2).trim, items(3).trim, items(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 过滤用户行为
    val filterBlacklistStream: DataStream[AdClickEvent] = adEventStream
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlacklistUser(100))

    // 根据省份分组，开窗聚合
    val adCntByProvinceStream: DataStream[AdCountByProvince] = filterBlacklistStream
      .keyBy(_.province) // 如果需要聚合躲组，把需要聚合的Keys包装成元组
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdWindowResult())

    adCntByProvinceStream.print("ad count by prince stream")
    filterBlacklistStream.getSideOutput(blackListOutputTag).print("blacklist")
    env.execute("Add Stats By Geo")

  }

  /**
   * 记录状态，每天0点清空黑名单
   * 定义在Object中，可以使用侧输出流tag
   * @param maxCount 每个用户一天点击该广告超过n说明刷单，每个userid-adId含有一个FilterBlacklistUser用于判断
   */
  class FilterBlacklistUser(maxCount: Int) extends KeyedProcessFunction[(Long,Long), AdClickEvent, AdClickEvent] {
    // 该userId点击adId的次数
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("count-state", classOf[Long])
    )

    // 表示是否登记过黑名单
    lazy val hasSent: ValueState[Boolean] = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("has-over-state", classOf[Boolean])
    )

    // 保存定时器触发的时间戳
    lazy val resetTime: ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("reset-time-state", classOf[Long])
    )

    override def processElement(value: AdClickEvent,
                                ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context,
                                out: Collector[AdClickEvent]): Unit = {
      // 取出count状态
      val curCount = countState.value()

      // 如果是第一次处理，注册定时器，每天00:00触发
      if (curCount == 0) {
        val ts = (ctx.timerService().currentProcessingTime() / (1000*3600*24) + 1) * (1000*3600*24)
        resetTime.update(ts)
        ctx.timerService().registerProcessingTimeTimer(ts)
      }

      // 如果点击次数达到上限，则加入黑名单
      if (curCount > maxCount) {
        // 判断是否发送过
        if (!hasSent.value()) {
          hasSent.update(true)
          // 输出到测输出流
          ctx.output(blackListOutputTag,
            BlacklistWarning(value.userId, value.adId, s"Click over ${maxCount} times today"))
          return
        }
      }
      // 点击次数没达到上限，加1
      countState.update(countState.value() + 1)
      out.collect(value)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext,
                         out: Collector[AdClickEvent]): Unit = {
      // 定时器触发时清空状态
      if (timestamp == resetTime.value()) {
        hasSent.clear()
        countState.clear()
      }
    }
  }
}
