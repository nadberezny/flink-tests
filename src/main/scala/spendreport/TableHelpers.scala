package spendreport

import org.apache.flink.api.dag.{Pipeline, Transformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.{EnvironmentSettings, TableConfig}
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog, GenericInMemoryCatalog}
import org.apache.flink.table.delegation.PlannerFactory
import org.apache.flink.table.factories.ComponentFactoryService
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.planner.delegation.BatchExecutor

import java.util
import scala.collection.mutable

/*
  Tu jest obejście jakiegoś dziwnego checku w StreamTableEnvironmentImpl.create:
  		if (!settings.isStreamingMode()) {
			throw new TableException(
				"StreamTableEnvironment can not run in batch mode for now, please use TableEnvironment.");
		}
  ==> na liście dyskusyjnej poradzili żeby po prostu skopiować tę metodę bez tego checka:
  http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/Conversion-of-Table-Blink-batch-to-DataStream-tc34080.html#a34090

 */
object TableHelpers {

  private val cached: mutable.Map[StreamExecutionEnvironment, StreamTableEnvironment] = new mutable.HashMap()

  def getStreamTableEnv(executionEnvironment: StreamExecutionEnvironment): StreamTableEnvironment = synchronized {
    cached.getOrElseUpdate(executionEnvironment, {
      val settings = EnvironmentSettings.newInstance()
        .inBatchMode()
        .useBlinkPlanner()
        .build()

      val catalogManager = CatalogManager.newBuilder()
        .defaultCatalog(settings.getBuiltInCatalogName,
          new GenericInMemoryCatalog(settings.getBuiltInCatalogName, settings.getBuiltInDatabaseName))
        .classLoader(getClass.getClassLoader)
        //???
        .config(new Configuration())
        .executionConfig(executionEnvironment.getConfig)
        .build()

      val moduleManager = new ModuleManager


      val executor = new BatchExecutor(executionEnvironment.getJavaEnv) {


        override def createPipeline(transformations: util.List[Transformation[_]],
                                    tableConfig: TableConfig, jobName: String): Pipeline = {
          val original = super.createPipeline(transformations, tableConfig, jobName).asInstanceOf[StreamGraph]
          val back = getExecutionEnvironment.getStateBackend
          original.setStateBackend(back)
          original.setTimeCharacteristic(TimeCharacteristic.EventTime)
          original
        }

      }

      val tableConfig = new TableConfig
      tableConfig.getConfiguration.setString(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE, "PIPELINED")
      val functionCatalog = new FunctionCatalog(tableConfig, catalogManager, moduleManager)

      val plannerProperties = settings.toPlannerProperties
      val planner = ComponentFactoryService.find(classOf[PlannerFactory], plannerProperties)
        .create(plannerProperties, executor, tableConfig, functionCatalog, catalogManager)
      new StreamTableEnvironmentImpl(catalogManager, moduleManager,
        functionCatalog, tableConfig, executionEnvironment.getJavaEnv, planner, executor, settings.isStreamingMode, getClass.getClassLoader)
    })
  }
}
