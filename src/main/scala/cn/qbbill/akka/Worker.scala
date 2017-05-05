package cn.qbbill.akka

import java.util.UUID

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Created by 钱斌 on 2017/2/27.
  */
class Worker(val host: String, val port: Int) extends Actor {

  var master: ActorSelection = _
  val workerId = UUID.randomUUID().toString() //worker唯一的标志

  override def preStart(): Unit = {
    println("worker preStart")
    //actorSelection根据路径查找actor
    master = context.actorSelection("akka.tcp://MasterSystem@127.0.0.1:9999/user/master") //user固定
    master ! RegisterInfo(workerId, 2048, 4)
  }

  override def receive: Receive = {
    case MasterInfo(host, port) => {
      import context.dispatcher
      context.system.scheduler.schedule(0 millis, 5000 millis, self, HeartBeat) //这里无法获得master的actorRef,所以先发给自己
    }
    case HeartBeat => {
      //自己接收到消息,向master发送心跳
      master ! HeartBeatToMaster(workerId)
    }
  }
}

object Worker {
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
    val actorSystem = ActorSystem("WorkerSystem", config)
    val worker = actorSystem.actorOf(Props(new Worker(host, port)), "worker")
    actorSystem.awaitTermination()
  }
}
