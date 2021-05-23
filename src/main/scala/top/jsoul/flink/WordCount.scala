package top.jsoul.flink

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


object WordCount {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironment(4)
    val stream = env.socketTextStream("192.168.37.131", 9999)
    //3. 对接收到的数据转换成单词元组
    val wordDataStream: DataStream[(String, Int)] = stream.flatMap(_.split(" ")).map(line => (line, 1))

    //4. 使用 keyBy 进行分流（分组）
    //在批处理中针对于dataset， 如果分组需要使用groupby
    //在流处理中针对于datastream， 如果分组（分流）使用keyBy
    val groupedDataStream: KeyedStream[(String, Int), Tuple] = wordDataStream.keyBy(0)

    //5. 使用 timeWinodw 指定窗口的长度（每5秒计算一次）
    //spark-》reduceBykeyAndWindow
    val windowDataStream: WindowedStream[(String, Int), Tuple, TimeWindow] = groupedDataStream.timeWindow(Time.seconds(5))

    //6. 使用sum执行累加
    val sumDataStream = windowDataStream.sum(1)
    sumDataStream.print()
    env.execute()

  }
}

