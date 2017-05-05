package cn.qbbill.akka

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

/**
  * 实现worker自动注册到master中,master保存worker信息并反馈注册成功;worker定时发送心跳连接,
  * master定期检测worker心跳,发现worker心跳超时时在保存的worker信息中自动剔除超时worker
  * Created by 钱斌 on 2017/2/26.
  */
class Master(val host: String, val port: Int) extends Actor {


  val clients = new scala.collection.mutable.HashMap[String, WorkerInfo]()
  val workers = new scala.collection.mutable.HashSet[WorkerInfo]()
  val CHECK_INTERVAL = 10000

  override def preStart(): Unit = {
    println("初始化中,actor上下文对象:" + context)
    import context.dispatcher
    context.system.scheduler.schedule(0 millis, CHECK_INTERVAL millis, self, CheckTimeOut) //定时给自己发送消息,通知自己检测worker最后心跳时间
  }

  override def receive: Receive = {
    case RegisterInfo(id, memory, cores) => {
      //把worker的注册请求保存起来,供将来任务分配
      if (!clients.contains(id)) {
        val workInfo = new WorkerInfo(id, memory, cores)
        clients(id) = workInfo
        workers += workInfo
      }
      sender ! MasterInfo(host, port)
    }
    case HeartBeatToMaster(workerId) => {
      //根据worker的消息更新记录的worker最后心跳时间,用来判断worker心跳是否超时
      val currentTime = System.currentTimeMillis()
      val workerInfo = clients(workerId)
      workerInfo.lastHeartBeatTIme = currentTime
    }
    case CheckTimeOut => {
      val currentTime = System.currentTimeMillis()
      val remove = workers.filter(x => currentTime - x.lastHeartBeatTIme > CHECK_INTERVAL) //找出最后心跳时间超出检测间隔时间的worker,即为失去连接的worker
      for (deadWorker <- remove) {
        clients -= deadWorker.id //移除失去连接的worker,这样分配工作时就不会把工作分配给这些失联的worker
        workers -= deadWorker
      }
      println("worker size" + workers.size)
    }
  }
}

object Master {
  def main(args: Array[String]) {
    val host = args(0)
    val port = args(1).toInt
    val conStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    val config = ConfigFactory.parseString(conStr)
    val actorSystem = ActorSystem("MasterSystem", config)
    //actorOf创建actor
    val master = actorSystem.actorOf(Props(new Master(host, port)), "master")
    actorSystem.awaitTermination()
  }
}

class WorkerInfo(val id: String, val memory: Int, val cores: Int) {
  var lastHeartBeatTIme: Long = _ //记录最后一次心跳连接时间
}
