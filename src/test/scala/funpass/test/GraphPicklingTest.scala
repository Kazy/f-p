package funpass.test

import org.junit.Test

import scala.pickling._
import Defaults._
import binary._

import scala.spores._
import SporePickler._

import silt._
import silt.graph._
import Picklers._


class GraphPicklingTest {
  // need an Apply node
  // pickle a CommandEnvelope

  // @Test def testSpore2

  @Test def testPickleGraph(): Unit = {
    runtime.GlobalRegistry.picklerMap += ("silt.graph.CommandEnvelope" -> { x => silt.graph.Picklers.CommandEnvelopePU })
    runtime.GlobalRegistry.unpicklerMap += ("silt.graph.CommandEnvelope" -> silt.graph.Picklers.CommandEnvelopePU)

    val fromHost = Host("127.0.0.1", 8091)
    val destHost = Host("127.0.0.1", 8092)
    val from: Node = Materialized(1)

    val pni = PumpNodeInput[Int, Int, List[Int]](from, fromHost, spore {
      (elem: Int, emit: Emitter[Int]) => emit.emit(elem)
    }, new ListBuilderFactory[Int])

    val inputs = List(pni)
    // 3 = emitterId
    val multiInput = MultiInput(inputs, 2, destHost, 3)

    val p = multiInput.pickle

    val up = p.unpickle[Node]
    up match {
      case m: MultiInput[r] =>
        val inputs    = m.inputs
        val refId     = m.refId
        val destHost  = m.destHost
        val emitterId = m.emitterId

        for (input <- inputs) {
          val className = input.fun.getClass.getName
          println(s"input.fun: class name = $className")
          // val spore = input.fun.asInstanceOf[Spore2[Int, Emitter[Int], Unit]]
          // println(s"spore.className: ${spore.className}")
        }
    }
    // val command = CommandEnvelope
    assert(true)
  }
}
