package baby_spark
package rdd

import scala.language.higherKinds

import scala.reflect.runtime.universe._

import scala.spores._
import scala.pickling._
import SporePickler._
import Defaults._

import scala.concurrent._

import silt._

object MapRDD {
  def apply[K, V, S <: Traversable[(K, V)], A](silos: Seq[SiloRef[S]], hosts:
      Seq[Host], partitioner: Option[Partitioner[A]]): MapRDD[K, V, S] = {
    val localPartitioner = partitioner
    new MapRDD[K, V, S](silos, hosts){
      type P = A
      override val partitioner = localPartitioner
    }
  }

  def apply[K, V, S <: Traversable[(K, V)]](silos: Seq[SiloRef[S]], hosts:
  Seq[Host]): MapRDD[K, V, S] = {
    new MapRDD(silos, hosts)
  }
}

class MapRDD[K, V, S <: Traversable[(K, V)]]
  (override val silos: Seq[SiloRef[S]],
    override val hosts: Seq[Host])
    extends RDD[(K, V), S](silos, hosts) {

  import RDD._

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
    MapRDD(resList, hosts)
  }

  // IS: Traversable type used to store the value for one key
  // RS: Traversable type used to store the mapping key/value
  def groupByKey[IS[A] <: Traversable[A], RS[A, B] <: Traversable[(A, B)]]()
    (implicit cbf1: CanBuildTo[(K, IS[V]), RS[K, IS[V]]],
      cbf2: CanBuildTo[V, IS[V]]): MapRDD[K, IS[V], RS[K, IS[V]]] = {
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
    MapRDD[K, IS[V], RS[K, IS[V]]](resList, hosts)
  }

  def mapValues[W, RS <: Traversable[(K, W)]](f: Spore[V, W])
    (implicit cbt: CanBuildTo[(K, W), RS], ev: this.P =:= KeyPartitioner[K, V]): MapRDD[K, W, RS] = {
    val silos = map[(K, W), RS](spore {
      val lf = f
      e => (e._1, lf(e._2))
    }).silos

    val self = this
    new MapRDD[K, W, RS](silos, hosts){
      type P = self.P
      override val partitioner = self.partitioner
    }
  }

  def combine[X, IS[A] <: Traversable[A], RS[A, B] <: Traversable[(A, B)]]()(
    implicit ev: V =:= (X, X),
    cbf: CanBuildTo[(K, V), S],
    cbf1: CanBuildTo[X, IS[X]],
    cbf2: CanBuildTo[(K, IS[X]), RS[K, IS[X]]],
    tt: TypeTag[K]): MapRDD[K, IS[X], RS[K, IS[X]]] = {

    partitioner match {
      case None => {
        val keyPartitioner = KeyPartitioner[K, V](hosts.length)
        val newSilos = RDD.partition[(K, V), S](silos)(keyPartitioner)
        MapRDD[K, V, S, (K, V)](silos, hosts, Some(keyPartitioner)).combine()
      }

      case Some(_) => {
        val combineSilos = silos.map { s => {
          s.apply(spore {
            val lcbf1 = cbf1
            val lcbf2 = cbf2
            val lev = ev
            content => {
              val map = content.flatMap({ case (key, v) => {
                val value = lev(v)
                List((key, value._1), (key, value._2))
              }})
              val grouped = map.groupBy(_._1)
              val res = grouped.map({ case (key, v) => {
                (key, v.map(e => e._2)(collection.breakOut(lcbf1)))
              }})(collection.breakOut(lcbf2))
              res
            }
          })
        }}

        MapRDD(combineSilos, hosts, partitioner)
      }
    }
  }

  def join[W, S2 <: Traversable[(K, W)], FS <: Traversable[(K, (V, W))]]
    (other: MapRDD[K, W, S2])
    (implicit ec: ExecutionContext,
      cbf: CanBuildTo[(K, (V, W)), FS]): MapRDD[K, Tuple2[V, W], FS] = {

    val othersSilo = other.silos

    type X = (K, (V, W))

    def joinSilos
      (silo1: SiloRef[S], silo2: SiloRef[S2], cbf: CanBuildTo[X, FS]): SiloRef[FS] = {

      silo2.flatMap(spore {
        val lsl1 = silo1
        val lcbf1 = cbf
        sl2 => {
          lsl1.apply(spore {
            val lsl2 = sl2
            val lcbf2 = lcbf1
            sl1 => {
              lsl2.flatMap { e1 =>
                sl1.flatMap {
                  e2 => if (e1._1 == e2._1) List((e1._1, (e2._2, e1._2))) else Nil
                }
              }(collection.breakOut(lcbf2))
            }
          })
        }
      })
    }

    def mergeSilos(receiverSilo: SiloRef[FS], restSilo: Seq[SiloRef[FS]], cbf: CanBuildTo[X, FS]):
        SiloRef[FS] = {
      def innerMerge(silo1: SiloRef[FS], silo2: SiloRef[FS], cbf: CanBuildTo[X, FS]): SiloRef[FS] = {

        silo2.flatMap(spore {
          val ls1 = silo1
          val lcbf = cbf
          s2 => {
            ls1.apply(spore {
              val ls2 = s2
              val llcbf = lcbf
              s1 => {
                val builder = llcbf()
                builder ++= s1
                builder ++= ls2
                builder.result()
              }
            })
          }
        })
      }

      restSilo.fold(receiverSilo) {
        (currentSilo, otherSilo) => innerMerge(currentSilo, otherSilo, cbf)
      }
    }

    val part = (partitioner, other.partitioner) match {
      case (Some(p1), Some(p2)) if p1 == p2 => partitioner
      case _ => None
    }

    val res = part match {
      case Some(p) => {
        silos.zip(othersSilo).map({ case (s1, s2) => joinSilos(s1, s2, cbf) })
      }
      case _ => {
        silos.map(s1 => {
        val joined = othersSilo.map(s2 =>
          joinSilos(s1, s2, cbf)
        )
        mergeSilos(joined.head, joined.tail, cbf)
      })}
    }

    MapRDD(res, hosts, part)
  }

  // def leftJoin[W, S2 <: Traversable[(K, W)], FS <: Traversable[(K, (V, Option[W]))]]
  //   (other: MapRDD[K, W, S2])
  //   (implicit ec: ExecutionContext,
  //     cbf: CanBuildTo[(K, (V, Option[W])), FS]): MapRDD[K, Tuple2[V, Option[W]], FS] = {

  //   def extractor(e1: (K, V), e2: (K, W)): List[(K, (V, Option[W]))] = {
  //     if (e1._1 == e2._1) List((e1._1, e1._2 -> Some(e2._2)))
  //     else List((e1._1, e1._2 -> None))
  //   }

  //   join[(K, (V, Option[W])), (K, W), S2, FS](other, extractor)
  // }

  // def rightJoin[W, S2 <: Traversable[(K, W)], FS <: Traversable[(K, (Option[V], W))]]
  //   (other: MapRDD[K, W, S2])
  //   (implicit ec: ExecutionContext,
  //     cbf: CanBuildTo[(K, (Option[V], W)), FS]): MapRDD[K, Tuple2[Option[V], W], FS] = {

  //   def extractor(e1: (K, W), e2: (K, V)): List[(K, (Option[V], W))] = {
  //     if (e1._1 == e2._1) List((e1._1, Some(e2._2) -> e1._2))
  //     else List((e1._1, None -> e1._2))
  //   }

  //   other.join[(K, (Option[V], W)), (K, V), S, FS](this, extractor)
  // }

  // def fullJoin[W, S2 <: Traversable[(K, W)], FS <: Traversable[(K, (Option[V], Option[W]))]]
  //   (other: MapRDD[K, W, S2])
  //   (implicit ec: ExecutionContext,
  //     cbf: CanBuildTo[(K, (Option[V], Option[W])), FS]): MapRDD[K, Tuple2[Option[V], Option[W]], FS] = {

  //   def extractor(e1: (K, V), e2: (K, W)): List[(K, (Option[V], Option[W]))] = {
  //     if (e1._1 == e2._1) List((e1._1, Some(e1._2) -> Some(e2._2)))
  //     else List((e1._1, Some(e1._2) -> None), (e2._1, None -> Some(e2._2)))
  //   }

    // join[(K, (Option[V], Option[W])), (K, W), S2, FS](other, extractor)
  // }

  def union(other: MapRDD[K, V, S]): MapRDD[K, V, S] = MapRDD(silos ++ other.silos, hosts)
}
