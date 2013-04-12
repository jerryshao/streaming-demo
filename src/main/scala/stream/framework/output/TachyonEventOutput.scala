package stream.framework.output

import java.nio.ByteBuffer
import java.nio.ByteOrder

import spark.RDD
import spark.streaming.DStream

import tachyon.client.TachyonClient
import tachyon.thrift.FileAlreadyExistException
import tachyon.client.RawColumn
import tachyon.client.TachyonFile
import tachyon.client.OpType

import stream.framework.output.column._

import org.apache.hadoop.io.Text

class TachyonEventOutput extends AbstractEventOutput {

  private var tableName: String = null
  
  @transient val tachyonClient = {
    val master = System.getenv("TACHYON_MASTER")

      if (master == null)
        TachyonClient.getClient("localhost:19998")
      else
        TachyonClient.getClient(master)  
  }
  
  val warehousePath = {
    val path = System.getenv("TACHYON_WAREHOUSE_PATH")
    if (path == null)
      "/user/tachyon"
    else
      path
  }
  
  // create data warehouse path in tachyon
  try {
    tachyonClient.mkdir(warehousePath)
  } catch {
    case e: FileAlreadyExistException => Unit
  }
  
  // create table in tachyon
  lazy val tablePath = warehousePath + "/" + tableName + "_tachyon"
  @transient lazy val table = {
    if (tachyonClient.exist(tablePath)) {
      tachyonClient.getRawTable(tablePath)
    } else {
      val tableId = tachyonClient.createRawTable(tablePath, 3)
      tachyonClient.getRawTable(tableId)
    }
      
  }
  
  override def output(stream: DStream[_]) {
    stream match {
      case s: DStream[(String, Long)] => strLongOutput(s)
      case s: DStream[(String, Seq[String])] => Unit
      case _ => Unit
    }
  }
  
  override def setOutputName(name: String) {	
    tableName = name
  }
  
  private def strLongOutput(stream: DStream[(String, Long)]) {
    var partition = 0;
    
    stream.foreach(r => {
      val rows = r.collect()
      
      val metaCol = table.getRawColumn(0)
      metaCol.createPartition(partition)
      val f = metaCol.getPartition(partition)
      f.open(OpType.WRITE_CACHE)
      val buffer = ByteBuffer.allocate(8)
      buffer.order(ByteOrder.nativeOrder())
      buffer.putLong(rows.length.toLong)
      buffer.flip()
      f.append(buffer)
      f.close()
      
      val longColBuilder = new LongColumnBuilder
      longColBuilder.initialize(100)
      
      val strColBuilder = new StringColumnBuilder
      strColBuilder.initialize(100)
      
      rows.foreach(r => {
        println(">>>>>rows: " + r._1 + " >>> " + r._2)
        strColBuilder.append(new Text(r._1))
        longColBuilder.append(r._2)
        }
      )
        
      val col1 = table.getRawColumn(1)
      col1.createPartition(partition)
      val f1 = col1.getPartition(partition)
      f1.open(OpType.WRITE_CACHE)
      val buf1 = strColBuilder.build
      f1.append(buf1)
      f1.close()
      
      val col2 = table.getRawColumn(2)
      col2.createPartition(partition)
      val f2 = col2.getPartition(partition)
      f2.open(OpType.WRITE_CACHE)
      val buf2 = longColBuilder.build
      f2.append(buf2)
      f2.close()
      
      partition += 1
    })
  }
  
//  case(partitionIndex, iter) =>
//      op.initializeOnSlave()
//      val serde = new ColumnarSerDe
//      serde.initialize(op.hconf, op.localHiveOp.getConf.getTableInfo.getProperties())
//
//      // Serialize each row into the builder object.
//      // ColumnarSerDe will return a TablePartitionBuilder.
//      var builder: Writable = null
//      iter.foreach { row =>
//        builder = serde.serialize(row.asInstanceOf[AnyRef], op.objectInspector)
//      }
//
//      if (builder != null) {
//        statsAcc += Tuple2(partitionIndex, builder.asInstanceOf[TablePartitionBuilder].stats)
//        Iterator(builder.asInstanceOf[TablePartitionBuilder].build)
//      } else {
//        // Empty partition.
//        statsAcc += Tuple2(partitionIndex, new TablePartitionStats(Array(), 0))
//        Iterator(new TablePartition(0, Array()))
//      }
  
}