package stream.framework

import spark.streaming.DStream
import spark.streaming.StreamingContext
import spark.streaming.Seconds

import stream.framework.util.AppConfig
import stream.framework.parser.AbstractEventParser
import stream.framework.output.AbstractEventOutput
import stream.framework.output.StdEventOutput
import stream.framework.operator._
import stream.framework.util.CountProperty
import stream.framework.util.AggregateProperty
import stream.framework.util.Configuration

object FrameworkEnv {

  class AppEnv(val appConfig: AppConfig) extends Serializable {
    
    // notice. appConfig, parser, output should be serializable, it will
    // be deserialized by spark streaming checkpoint, if add @transient,
    // after deserialize, these fields will be null
    val parser = try {
      Class.forName(appConfig.parserClass).newInstance().asInstanceOf[AbstractEventParser]
    } catch {
      case e: Exception => println(e.getStackTraceString); exit(1)
    }
    
    val output = try {
      Class.forName(appConfig.outputClass).newInstance().asInstanceOf[AbstractEventOutput]
    } catch {
      case _ => new StdEventOutput
    }
    
    val operators = appConfig.properties.map(p => {
      p match {
        case s: CountProperty => 
          new CountOperator(s.key, s.hierarchy, s.window)
        case s: AggregateProperty => 
          new AggregateOperator(s.key, s.value, s.hierarchy, s.window)
        case _ => new NoneOperator
      }
    })
    
    def process(stream: DStream[String]) {
      val eventStream = stream.map(s => parser.parseEvent(s, appConfig.schemas))
      operators.foreach(p => {
        val resultStream = p.process(eventStream)
        output.output(resultStream)
        })
    }
  }
  
  def main(args: Array[String]) {
    if (args.length < 4) {
      println("FrameworkEnv master zkQuorum group conf/properties.xml")
      exit(1)
    }
    
    //parse the conf file
    Configuration.parseConfig(args(3))

    //for each app, create its app env
    val appEnvs = Configuration.appConfMap.map(e => (e._1, new AppEnv(e._2)))
    
    //create kafka input stream
    val Array(master, zkQuorum, group, _) = args
    val ssc =  new StreamingContext(master, "kafkaStream", Seconds(5))
    ssc.checkpoint("checkpoint")
    
    /****************TODO. this should be modified later*******************/
    //cause all the topics are in one DStream, first we should filter out
    // what topics to what application
    // because kafka stream currently do not support decode method
    // other than string decode, so currently workaround solution is:
    // all the input message should follow this format: "category|||message",
    // "|||" is a delimiter, category is topic name, message is content
    val lines = ssc.kafkaStream[String](zkQuorum, group, 
         Configuration.appConfMap.map(e => (e._1, 1)))
    val streams = appEnvs.map(e => (e._1, lines.filter(s => s.startsWith(e._1))))
    	 .map(e => (e._1, e._2.map(s => s.substring(s.indexOf("|||") + 3))))
    
    streams.foreach(e => appEnvs(e._1).process(e._2))
    
    
    ssc.start()
  }
}