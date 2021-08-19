package spendreport

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.{Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WordCountJob extends App {
  import org.apache.flink.streaming.api.scala._

  case class Element(myKeyField: String, myTimestampField: Long, someValue: Int = 1)

  def onEventTrigger[T]() = new Trigger[T, TimeWindow] {
    override def onElement(element: T, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE
    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = ()
  }

  def onEventWatermark[T](): WatermarkStrategy[T] = new WatermarkStrategy[T] {
    override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[T] =
      new WatermarkGenerator[T] {
        override def onEvent(t: T, l: Long, watermarkOutput: WatermarkOutput): Unit = watermarkOutput
          .emitWatermark(new Watermark(l))

        override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = ()
      }
  }

  val elements: List[Element] = List(
    Element(myKeyField = "a", myTimestampField = 1L),
    Element(myKeyField = "a", myTimestampField = 2L),
    Element("b", 3L),
    Element("a", 4L)
  )

  val fun = new ProcessFunction[Element, Element] {
    override def processElement(value: Element, ctx: ProcessFunction[Element, Element]#Context, out: Collector[Element]): Unit = {
      println(s"current watermark: ${ctx.timerService().currentWatermark()}")
      println(s"current timestamp: ${ctx.timestamp()}")
      out.collect(value)
    }
  }

//  new SourceFunction[] {}
  val env = StreamExecutionEnvironment.createLocalEnvironment()
    .setRuntimeMode(RuntimeExecutionMode.STREAMING)


  val source: DataStream[Element] = env
    .fromCollection(elements)
    .assignAscendingTimestamps(_.myTimestampField)
    .assignTimestampsAndWatermarks(onEventWatermark[Element]())


  val counts = source
    .process(fun)
    .keyBy(_.myKeyField)
//    .window(TumblingEventTimeWindows.of(Time.seconds(2)))
//    .trigger(EventTimeTrigger.create())
//    .trigger(onEventTrigger[Element]())
    .sum("someValue")
//    .process(fun)

  val sink = counts.print()

  env.execute()
}
