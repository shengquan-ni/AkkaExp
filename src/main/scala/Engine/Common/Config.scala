package Engine.Common

object Config {

  /**
   * The following is used to connect a socket at beginning to know own IP
   */
  // val thirdPartyIP: String = "128.195.52.129"
  // val thirdPartyPort:Int = 9870
  val thirdPartyIP: String = "172.31.33.27"
  val thirdPartyPort:Int = 22
  /**
   * Put HDFS connection details below.
   */
//  val remoteHDFSPath = "hdfs://128.195.52.129:9871"
//  val remoteHDFSIP = "128.195.52.129"
  val remoteHDFSPath = "hdfs://ec2-18-216-84-85.us-east-2.compute.amazonaws.com:9870"
  val remoteHDFSIP = "ec2-18-216-84-85.us-east-2.compute.amazonaws.com"
  var dataset = 20

  /**
   * Automate number of workers per operator and data to be processed
   */
  // var numWorkerPerNode = 12
  var numWorkerPerNode = 4
  var dataVolumePerNode = 0
}
