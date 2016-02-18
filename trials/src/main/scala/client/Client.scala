package client

import scala.io.Source

import scala.spores._

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.pickling.Defaults._
import scala.pickling.shareNothing._

import scala.util.{Success, Failure}

import com.typesafe.scalalogging.{ StrictLogging => Logging }

import silt._

object Client extends AnyRef with Logging {

  import logger._

  def populateSilo(filename: String): LocalSilo[String, List[String]] = {
    val lines = Source.fromFile(filename).mkString.split('\n').toList
    new LocalSilo(lines)
  }

  def main(args: Array[String]): Unit = {
    val system = SiloSystem()
    try {
      val host = Host("127.0.0.1", 8090)

      val filename = "./trials/src/main/scala/server/data.txt"

      val silo: Future[SiloRef[String, List[String]]] = system.fromFun(host)(() => populateSilo(filename))

      // silo.flatMap(s =>
      //   s.apply[Int, List[Int]] (spore {
      //               lines => lines.length
      //             })
      // )

      val done = silo.flatMap(_.send())

      val res = Await.result(done, 15 seconds)

      println (s"Result size: ${res.size}")
      res foreach (println)

    } catch {
      case e: Throwable =>
        println(s"An error appeared: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      println("Exiting..")
      system.waitUntilAllClosed()
      println("Done !")
    }

  }
}
