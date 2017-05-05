package cn.qbbill.akka

/**
  * Created by 钱斌 on 2017/3/2.
  */
/*trait Message extends Serializable*/

case object HeartBeat//内部消息,单例即可,用来做一个信号


case class HeartBeatToMaster(workId: String)

case class RegisterInfo(workerId: String, memory: Int, cores: Int)

case class MasterInfo(host: String, port: Int)

case object CheckTimeOut


