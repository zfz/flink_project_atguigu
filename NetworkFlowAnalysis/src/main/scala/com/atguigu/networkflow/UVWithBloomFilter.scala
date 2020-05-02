package com.atguigu.networkflow

import java.lang

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UVWithBloomFilter {
  def main(args: Array[String]): Unit = {
    // 1. 创建执行环境
    val conf: Configuration = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. 读取数据
    val inputPath = "HotItemAnalysis/src/main/resources/UserBehavior.csv"
    val dataStream = env.readTextFile(inputPath)
      .map(line => {
        val items = line.split(",")
        UserBehavior(
          items(0).trim.toLong, items(1).trim.toLong, items(2).trim.toInt, items(3).trim, items(4).trim.toLong)
      })
      // 由于是递增时间戳，使用递增timestamp即可
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")

    val UVCntStream = dataStream
      .map(data => ("dummyKey", data.userId))
      // 类似timeWindowAll
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      // 每来一条数据就触发一次窗口操作，因为用了Redis缓存，不要在窗口关闭的时候再触发
      .trigger(new MyTrigger())
      .process(new UVCountWithBloomFilter())

    UVCntStream.print("UV Cnt Stream")
    env.execute("UV With Bloom Filter")
  }
}

/**
 * 自定义窗口触发器
 * T 当前数据类型("dummyKey", userId)
 * W 窗口类型
 */
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  // 每个数据到达窗口后触发该方法
  override def onElement(event: (String, Long),
                         timestamp: Long,
                         window: TimeWindow,
                         ctx: Trigger.TriggerContext): TriggerResult = {
    // 每来一条数据触发窗口操作，并清空窗口所有状态
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  // 窗口的EventTime定时器触发时调用onEventTime
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  // onElement来清空
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}

/**
 * Bloom过滤器其实是一个哈希函数
 * Size 过滤器大小，也可以用Int，32位，代表20亿正数，2G内存
 */
class BloomFilter(size: Long) extends Serializable {
  // 位图的总大小 2^27 = 134,217,728，占用2^4*2^3*2^10*2^10 = 16MB
  lazy val cap = if (size > 0) size else 1 << 27

  // 定义hash函数，seed一般是质数，默认给67
  def hash(value: String, seed: Int = 67): Long = {
    var res = 0L
    // 累加字符
    for (i <- 0 until value.length) {
      res = res * seed + value.charAt(i)
    }
    // 只需要Long类型数的后27位值，取值范围在0 ~ 27个1之间
    res & (cap-1)
  }
}

/**
 * 基于Window后的操作，用ProcessWindowFunction
 * I 输入数据类型("dummyKey", userId)
 * O 输出数据类型
 * K Key数据类型
 * W 窗口类型
 */
class UVCountWithBloomFilter() extends ProcessWindowFunction[(String, Long), UVCount, String, TimeWindow] {
  lazy val jedis = new Jedis("localhost", 6379)
  lazy val bloomFilter = new BloomFilter(1<<29)

  override def process(key: String,
                       ctx: Context,  // 上下文可以拿到窗口信息
                       elements: Iterable[(String, Long)],
                       out: Collector[UVCount]): Unit = {
    // 位图在Redis的存储方式: 每个窗口一个位图，key是windowEnd，value是bitmap
    val storeKey = ctx.window.getEnd.toString

    // 用布隆过滤器判断是否存在
    // 每来一条数据就触发窗口操作，elements里其实只有一条数据
    val userId = elements.head._2.toString
    val offSet = bloomFilter.hash(userId)

    // 从位图中查找是否存在
    val isExist: lang.Boolean = jedis.getbit(storeKey, offSet)

    if (!isExist) {
      // 位图的位置设置为1
      jedis.setbit(storeKey, offSet, true)

      // 由于自定义Trigger清空了Window状态，需要把每个Window的UV数量也存入Redis：Cnt表，key是windowEnd，value是UV数
      val windowUVCnt: lang.Long = jedis.hincrBy("count", storeKey, 1)
      // out.collect(UVCount(storeKey.toLong, windowUVCnt))
    } else {
      // 用户已经存在，无操作只输出
      // out.collect(UVCount(storeKey.toLong, jedis.hget("count", storeKey).toLong))
    }

  }
}