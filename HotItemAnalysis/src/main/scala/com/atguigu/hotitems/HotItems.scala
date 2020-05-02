package com.atguigu.hotitems

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 基本需求
 *  – 统计近1小时内的热门商品，每5分钟更新一次
 *  – 热门度用浏览次数（“pv”）来衡量
 * 解决思路
 *  – 在所有用户行为数据中，过滤出浏览（“pv”）行为进行统计
 *  – 构建滑动窗口，窗口长度为1小时，滑动距离为5分钟
 **/
object HotItems {
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
        val items: Array[String] = line.split(",")
        UserBehavior(
          items(0).trim.toLong, items(1).trim.toLong, items(2).trim.toInt, items(3).trim, items(4).trim.toLong)
      })
      // 由于是递增时间戳，使用递增timestamp即可
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 3. Transform过滤PV行为
    val itemViewCountStream = dataStream
      .filter(_.behavior == "pv")
      // 注意KEY的类型由keyBy决定，如果是string类型，key使用tuple，使用的是Long类型，key使用Long
      // .keyBy("itemId")
      .keyBy(_.itemId)
      // 进行开窗操作，1小时，5分钟滑动
      .timeWindow(Time.hours(1), Time.minutes(5))
      // 窗口聚合
      .aggregate(new CountAgg(), new WindowResult())
      // 按照窗口分组
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))

    // 4. Sink控制台输出
    itemViewCountStream.print("top n hot items")

    env.execute("Hot Item Analysis")
  }
}

/**
 * 自定义预聚合函数，每次来一个数据，进行累加
 * IN 输入类型
 * ACC 中间累加变量
 * OUT 输出类型
 **/
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

/**
 * 自定义窗口处理函数[IN, OUT, KEY, W]，输出结果
 * 注意KEY的类型由keyBy决定，如果是string类型，key使用tuple，使用的是Long类型，key使用Long
 **/
class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd(), input.iterator.next()))
  }
}

/**
 * 自定义预聚合函数，计算平均数
 * IN 输入类型
 * ACC 中间累加变量（保存总数和数量）
 * OUT 输出类型
 **/
class AvgAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {
  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1+in.timestamp, acc._2+1)

  override def getResult(acc: (Long, Int)): Double = acc._1 / acc._2

  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1+acc1._1, acc._2+acc1._2)
}

/**
 * 自定义排序输出处理函数，K, I, O
 **/
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount,
                              ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    // 把每条数据存入状态列表
    itemState.add(value)
    // 注册一个定时器
    // 注意：在该窗口结束后+1ms开始计算统计排序的值，每个值有该窗口结束的时间
    // 相同的定时器，不会触发多次，内部是一个优先级队列，使用timestamp作为比较参数
    // 定时器的触发基于watermark的到来
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 定时器触发时，对所有数据排序，并输出结果
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    // 将所有state中的数据取出，放到List Buffer中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()
    // 支持集合的遍历操作
    import scala.collection.JavaConversions._
    for (item <- itemState.get()) {
      allItems += item
    }

    // 按照count大小排序，并取前N个
    val topN = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 清空状态
    itemState.clear()

    // 排名结果格式化输出
    val result: StringBuilder = new StringBuilder()
    // timestamp-1得到windowEnd
    result.append("时间：").append(new Timestamp(timestamp-1)).append("\n")
    // 输出每一个商品的信息
    for (i <- topN.indices) {
      val cur = topN(i)
      result.append("No").append(i+1).append(":")
        .append(" 商品id=").append(cur.itemId)
        .append(" 浏览量=").append(cur.count)
        .append("\n")
    }
    result.append("======================")
    // 控制输出频率，测试显示使用
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
