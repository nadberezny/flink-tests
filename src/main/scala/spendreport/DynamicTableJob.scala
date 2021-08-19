package spendreport

import org.apache.flink.table.api.DataTypes.{INT, VARCHAR}
import org.apache.flink.table.api.{Schema, Table}
import org.apache.flink.types.Row

object Element {
  val schema: Schema = Schema.newBuilder()
    .column("name", VARCHAR(8))
    .column("someValue", INT())
    .build()
}

case class Element(name: String, someValue: Int)

object DynamicTableJob extends App {
  import org.apache.flink.streaming.api.scala._
  import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
  import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

  val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

  val source = env.fromElements("a", "b", "a")

  val stream: DataStream[Element] = source
    .map(el => Element(el, 1))

  val myInputTable: Table = tableEnv.fromDataStream(stream, Element.schema)
  tableEnv.createTemporaryView("elements", myInputTable)

  val myOutputTable: Table = tableEnv.sqlQuery("select name, MyF(someValue) from elements group by name")

//  val resultStream: DataStream[Row] = tableEnv.toDataStream(myOutputTable)
  val resultStream: DataStream[Row] = tableEnv.toChangelogStream(myOutputTable)

  resultStream.print()

  env.execute()
}