package Engine.Common

object Config {

  /**
   * The following is used to connect a socket at beginning to know own IP
   */
  val thirdPartyIP: String = "128.195.52.129"
  val thirdPartyPort:Int = 9870

  /**
   * Put HDFS connection details below.
   */
  val remoteHDFSPath = "hdfs://128.195.52.129:9871"
  val remoteHDFSIP = "128.195.52.129"
  var dataset = 20;

  /**
   * Automate number of workers per operator and data to be processed
   */
  var numWorkerPerNode = 12
  var dataVolumePerNode = 0
}
