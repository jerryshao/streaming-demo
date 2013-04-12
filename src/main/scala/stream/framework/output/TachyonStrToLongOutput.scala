package stream.framework.output

import stream.framework.output.column._

import java.nio.ByteBuffer
import java.nio.ByteOrder

import spark.streaming.DStream

import tachyon.client.OpType

import org.apache.hadoop.io.Text

class TachyonStrToLongOutput extends AbstractTachyonEventOutput {
  
  @transient lazy val table = getTachyonTable(4)
  
  var partition = 0
  
  override def output(stream: DStream[_]) {
    stream.foreach(r => {
      val rows = r.collect()
      
      //first create metaColumn
      createMetaCol(rows.length)
      
      val currTime = System.currentTimeMillis() / 1000
      
      //create column builder for 3 data column
      val colBuilders = Array(new LongColumnBuilder, 
          new StringColumnBuilder, new LongColumnBuilder)
      colBuilders.foreach(_.initialize(rows.length))
      
      // build the column
      rows.foreach(r => {
        val newRow = r.asInstanceOf[(String, Long)]
        
        colBuilders(0).asInstanceOf[LongColumnBuilder].append(currTime)
        colBuilders(1).asInstanceOf[StringColumnBuilder].append(new Text(newRow._1))
        colBuilders(2).asInstanceOf[LongColumnBuilder].append(newRow._2)
      })
      
      //write to partition
      colBuilders.zipWithIndex.foreach(c => {
        val col = table.getRawColumn(c._2 + 1)
        col.createPartition(partition)
        
        val file = col.getPartition(partition)
        file.open(OpType.WRITE_CACHE)
        
        val buf = c._1.build
        file.append(buf)
        file.close()
      })
      
      partition += 1
    })
    
  }
  
  private def createMetaCol(rowNum: Long) {
    val metaCol = table.getRawColumn(0)
    metaCol.createPartition(partition)
    
    val file = metaCol.getPartition(partition)
    file.open(OpType.WRITE_CACHE)
    
    val buf = ByteBuffer.allocate(8)
    buf.order(ByteOrder.nativeOrder).putLong(rowNum).flip()
    
    file.append(buf)
    file.close()
  }
}
