package baby_spark
package rdd

import scala.language.higherKinds

import scala.spores._
import scala.pickling._
import SporePickler._
import Defaults._

import scala.concurrent._

import silt._


class MapRDD[K, V, S <: Traversable[(K, V)]](override val silos: Seq[SiloRef[S]])
    extends RDD[(K, V), S](silos) {

  def reduceByKey[RS[A, B] <: Traversable[(A, B)]](f: Spore2[V, V, V])
    (implicit cbf1: CanBuildTo[(K, V), RS[K, V]]): MapRDD[K, V, RS[K, V]] = {
    val resList = silos.map {
      s => s.apply[RS[K, V]](spore {
        val func = f
        val lcbf = cbf1
        c => {
          val res0 = c.groupBy(_._1)
          val res1 = res0.map(e => (e._1, e._2.map(_._2)))
          val res2 = res1.map(e => (e._1, e._2.reduce(func)))(collection.breakOut(lcbf))
          res2
        }
      })
    }
    new MapRDD(resList)
  }

  // IS: Traversable type used to store the value for one key
  // RS: Traversable type used to store the mapping key/value
  def groupByKey[IS[A] <: Traversable[A], RS[A, B] <: Traversable[(A, B)]]()
    (implicit cbf1: CanBuildTo[(K, IS[V]), RS[K, IS[V]]], cbf2: CanBuildTo[V, IS[V]]): MapRDD[K, IS[V], RS[K, IS[V]]] = {
    val resList = silos.map {
      s => s.apply[RS[K, IS[V]]](spore {
        val lcbf = cbf1
        val lcbf2 = cbf2
        c => {
          val res0 = c.groupBy(_._1)
          val res1 = res0.map(e => (e._1, e._2.map(_._2)(collection.breakOut(lcbf2))))(collection.breakOut(lcbf))
          res1
        }
      })
    }
    new MapRDD[K, IS[V], RS[K, IS[V]]](resList)
  }

  def mapValues[W, RS <: Traversable[(K, W)]](f: Spore[V, W])
    (implicit cbt: CanBuildTo[(K, W), RS]): MapRDD[K, W, RS] = {
    map[(K, W), RS](spore {
      val lf = f
      e => (e._1, lf(e._2))
    })
  }

  def join[IS[A] <: Traversable[A], RS[A, B] <: Traversable[(A, B)]]
    (other: MapRDD[K, V, S])
    (implicit cbf1: CanBuildTo[(K, IS[V]), RS[K, IS[V]]],
      cbf2: CanBuildTo[V, IS[V]]): MapRDD[K, IS[V], RS[K, IS[V]]] = {

    val rdd1 = groupByKey[IS, RS]()
    val rdd2 = other.groupByKey[IS, RS]()
    new MapRDD(rdd1.silos ++ rdd2.silos)
  }

  def join[W, S2 <: Traversable[(K, W)], FS <: Traversable[(K, (V, W))]]
    (other: MapRDD[K, W, S2])
    (implicit ec: ExecutionContext,
      cbf1: CanBuildTo[(K, (V, W)), FS],
      cbf2: CanBuildTo[(K, W), S2]): MapRDD[K, Tuple2[V, W], FS] = {

    flatMap[(K, Tuple2[V, W]), FS](spore {
      val rdd = other
      val lcbf1 = cbf1
      val lcbf2 = cbf2
      val lec = ec
      c1 => {
        val k1 = c1._1
        val v1 = c1._2
        val res0 = rdd.filter(spore {
          val lk1 = k1
          c2 => c2._1 == lk1
        })(lcbf2)
        val res1 = res0.map(spore {
          val lv1 = v1
          c2 => (c2._1, (lv1, c2._2))
        })(lcbf1)
        res1.collect()(lec)
      }
    })
  }

  def union(other: MapRDD[K, V, S]): MapRDD[K, V, S] = new MapRDD(silos ++ other.silos)
}
