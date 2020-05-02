package com.atguigu.networkflow

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object NetworkFlow {
  def main(args: Array[String]): Unit = {
    // 1. 创建执行环境
    val conf: Configuration = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. 读取数据
    val inputPath = "NetworkFlowAnalysis/src/main/resources/apachelog.txt"
    val dataStream = env.readTextFile(inputPath)
      .map(line => {
        val items = line.split(" ")
        // 定义时间转换
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val ts = simpleDateFormat.parse(items(3).trim).getTime()
        LogEvent(items(0).trim, items(1).trim, ts, items(5).trim, items(6).trim)
      })
      // 由于日志的时间是乱序的，需要使用watermark，需要按照经验设置延时大小
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[LogEvent](Time.seconds(1)) {
          override def extractTimestamp(element: LogEvent): Long = element.eventTime
      })

    // 3. 开辟窗口
    val windowedStream: WindowedStream[LogEvent, String, TimeWindow] = dataStream
      // 对url不满足条件的进行过滤
      .filter( data =>{
        val pattern = "^((?!\\.(css|js|ico|png)$).)*$".r
        (pattern findFirstIn(data.url)).nonEmpty
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      // 允许获取迟到60s的数据
      .allowedLateness(Time.seconds(60))


    // 4. 进行聚合操作
    val aggStream: DataStream[URLViewCount] = windowedStream
      .aggregate(new CountAgg(), new WindowResult())

    // 5. 进行分组操作排序
      aggStream
      .keyBy(_.windowEnd)
      .process(new TopNUrl(3)).print("top n url")

    env.execute("Network Flow Analysis")
  }
}

/**
 * 自定义预聚合函数，每次来一个数据，进行累加
 * IN 输入类型
 * ACC 中间累加变量
 * OUT 输出类型，交给WindowFunction
 **/
class CountAgg() extends AggregateFunction[LogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: LogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

/**
 * 自定义窗口处理函数[IN, OUT, KEY, W]
 * 输入类型来自AggregateFunction
 * 注意KEY的类型由keyBy决定，如果是string类型，key使用tuple，使用的是Long类型，key使用Long
 **/
class WindowResult() extends WindowFunction[Long, URLViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[URLViewCount]): Unit = {
    out.collect(URLViewCount(key, window.getEnd(), input.iterator.next()))
  }
}

/**
 * 自定义排序输出处理函数，K, I, O
 **/
class TopNUrl(topSize: Int) extends KeyedProcessFunction[Long, URLViewCount, String] {
  lazy val urlState: ListState[URLViewCount] = getRuntimeContext.getListState(
    new ListStateDescriptor[URLViewCount]("url-state", classOf[URLViewCount]))
  override def processElement(value: URLViewCount,
                              ctx: KeyedProcessFunction[Long, URLViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    urlState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, URLViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    // 从状态中拿到所有数据
    val urlViews: ListBuffer[URLViewCount] = new ListBuffer()

    val iter = urlState.get().iterator()
    while (iter.hasNext) {
      urlViews += iter.next()
    }
    urlState.clear()

    val topNItems = urlViews.sortWith(_.count > _.count).take(topSize)

    val result: StringBuilder = new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp-1)).append("\n")
    for (i <- topNItems.indices) {
      val cur = topNItems(i)
      result.append("No").append(i+1).append(":")
        .append(" URL=").append(cur.url)
        .append(" 浏览量=").append(cur.count)
        .append("\n")
    }
    result.append("======================")
    // 控制输出频率，测试显示使用
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}