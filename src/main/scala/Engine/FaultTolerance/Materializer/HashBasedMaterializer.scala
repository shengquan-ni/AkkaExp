package Engine.FaultTolerance.Materializer

import Engine.Common.AmberTag.LayerTag
import Engine.Common.AmberTuple.Tuple
import Engine.Common.TupleProcessor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.{BufferedWriter, File, FileWriter}
import java.net.URI

class HashBasedMaterializer(val outputPath:String,val index:Int, val hashFunc:Tuple => Int, val numBuckets:Int, val remoteHDFS:String = null) extends TupleProcessor {

  var writer:Array[BufferedWriter] = _

  override def accept(tuple: Tuple): Unit = {
    val index = (hashFunc(tuple) % numBuckets + numBuckets) % numBuckets
    writer(index).write(tuple.mkString("","|","\n"))
  }

  override def onUpstreamChanged(from: LayerTag): Unit = {

  }

  override def noMore(): Unit = {
    for(i <- 0 until numBuckets) {
      writer(i).close()
    }
    if(remoteHDFS != null){
      try{
        //val fs = FileSystem.get(new URI(remoteHDFS),new Configuration())
        for(i <- 0 until numBuckets) {
          println("start to write "+ "/amber-akka-tmp/"+outputPath+"/"+i+"/"+index+".tmp")
          //fs.copyFromLocalFile(new Path("/home/12198/"+outputPath+"/"+index+"/"+i+".tmp"),new Path("/amber-akka-tmp/"+outputPath+"/"+i+"/"+index+".tmp"))
          Runtime.getRuntime.exec("hadoop fs -cp file://"+"home/12198/"+outputPath+"/"+index+"/"+i+".tmp"+" hdfs://10.138.0.2:8020"+"/amber-akka-tmp/"+outputPath+"/"+i+"/"+index+".tmp")
          println("finished write "+ "/amber-akka-tmp/"+outputPath+"/"+i+"/"+index+".tmp")
        }
        //fs.close()
      }catch{
        case e:Exception => println(e)
      }
    }
  }

  override def initialize(): Unit = {
    writer = new Array[BufferedWriter](numBuckets)
    for(i <- 0 until numBuckets){
      val file = new File("/home/12198/"+outputPath+"/"+index+"/"+i+".tmp")
      //val file = new File("D:\\"+outputPath+"\\"+0+"\\"+0+".tmp")
      file.getParentFile.mkdirs() // If the directory containing the file and/or its parent(s) does not exist
      file.createNewFile()
      writer(i) = new BufferedWriter(new FileWriter(file))
    }
  }

  override def hasNext: Boolean = false

  override def next(): Tuple = ???

  override def dispose(): Unit = {
    writer.foreach(_.close())
  }

  override def onUpstreamExhausted(from: LayerTag): Unit = {

  }
}
