package spendreport

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.functions.RichFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.operators.{BoundedOneInput, KeyedProcessOperator, Output}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object JdtJob extends App {
  import org.apache.flink.streaming.api.scala._

  case class Element(myKeyField: String, myTimestampField: Long, someValue: Int = 1)

  case class AggregatedElement(myKeyField: String, sum: Int) {
    def +(maybeOther: Option[AggregatedElement]): AggregatedElement = maybeOther.map { other =>
      other.copy(sum = this.sum + other.sum)
    }.getOrElse(this)
  }

  class MyOperator(f: MyFun) extends KeyedProcessOperator[String, Element, AggregatedElement](f) with BoundedOneInput {
    override def open(): Unit = {
      super.open()
      f.output = output
    }

    override def endInput(): Unit = {
      println("dupa")
      ()
//      f.endInput()
    }
  }

  class MyFun extends KeyedProcessFunction[String, Element, AggregatedElement] with RichFunction with BoundedOneInput {

    var output: Output[StreamRecord[AggregatedElement]] = _

    private var state: ValueState[AggregatedElement] = _

    val ti = createTypeInformation[AggregatedElement]

    val stateDescriptor = new ValueStateDescriptor[AggregatedElement]("myState", ti)

    override def processElement(value: Element, ctx: KeyedProcessFunction[String, Element, AggregatedElement]#Context, out: Collector[AggregatedElement]): Unit = {
      val newState = AggregatedElement(value.myKeyField, value.someValue).+(Option(state.value()))
      state.update(newState)
    }

    override def endInput(): Unit = {
      if (state != null) {
        Option(state.value()).foreach { v =>
          output.collect(new StreamRecord[AggregatedElement](v))
        }
      }
    }

    override def open(parameters: Configuration): Unit = {
      state = getRuntimeContext.getState(stateDescriptor)
    }
  }

  val sortedElements: List[Element] = List(
    Element(myKeyField = "a", myTimestampField = 1L),
    Element(myKeyField = "a", myTimestampField = 2L),
    Element("a", 3L),
    Element("b", 5L)
  )


  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
    .setRuntimeMode(RuntimeExecutionMode.STREAMING)
//    .setRuntimeMode(RuntimeExecutionMode.BATCH)

  val tableEnv: StreamTableEnvironment = TableHelpers.getStreamTableEnv(env)

  val source: DataStream[Element] = env
    .fromCollection(sortedElements)

  import org.apache.flink.table.api.Expressions.row

  val tableSource: Table = tableEnv.fromValues(row("a", long2Long(1)))

  val source2 = new DataStream[Row](tableEnv.toAppendStream(tableSource, createTypeInformation[Row]))
      .map { row =>
        Element(myKeyField = row.getFieldAs(0), myTimestampField = row.getFieldAs(1))
      }

  val counts = source2
    .keyBy(_.myKeyField)
    .transform("my-agg", new MyOperator(new MyFun))

  val sink: DataStreamSink[AggregatedElement] = counts.print()

  env.execute()
}
