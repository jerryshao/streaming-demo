package stream.framework.output

import spark.streaming.DStream

abstract class AbstractEventOutput {

  /**
   * abstract method of output DStream, derivatives should implement this.
   */
  def output(stream: DStream[_])
}

class StdEventOutput extends AbstractEventOutput with Serializable {
  
  override def output(stream: DStream[_]) {
    stream.print()
  }
}