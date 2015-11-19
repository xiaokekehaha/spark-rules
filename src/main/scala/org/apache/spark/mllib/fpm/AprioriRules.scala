package org.apache.spark.mllib.fpm

import org.apache.spark.storage.StorageLevel
import org.slf4j.Logger

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.{SparkException, Logging}
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.rdd.{EmptyRDD, RDD}

/**
  * Created by zrf on 11/1/15.
  */

/**
  * :: Experimental ::
  *
  * Generates association rules from a [[RDD[FreqItemset[Item]]].
  *
  */
@Since("1.5.0")
@Experimental
class AprioriRules private[fpm](
                                 private var minConfidence: Double,
                                 private var maxConsequent: Int,
                                 private var numPartitions: Int)
  extends Logging with Serializable {


  /**
    * Constructs a default instance with default parameters {minConfidence = 0.8}.
    */
  @Since("1.5.0")
  def this() = this(0.8, Int.MaxValue, -1)

  /**
    * Sets the minimal confidence (default: `0.8`).
    */
  @Since("1.5.0")
  def setMinConfidence(minConfidence: Double): this.type = {
    require(minConfidence >= 0.0 && minConfidence <= 1.0)
    this.minConfidence = minConfidence
    this
  }

  /**
    * Sets the maximum size of consequents used by AprioriRules (default: Int.MaxValue).
    *
    */
  @Since("1.5.0")
  def setMaxConsequent(maxConsequent: Int): this.type = {
    this.maxConsequent = maxConsequent
    this
  }

  /**
    * Sets the number of partitions used by AprioriRules (default: same as input data).
    *
    */
  @Since("1.5.0")
  def setNumPartitions(numPartitions: Int): this.type = {
    this.numPartitions = numPartitions
    this
  }


  /**
    * Computes the association rules with confidence above [[minConfidence]].
    * @param freqItemsets frequent itemset model obtained from [[FPGrowth]]
    * @return a [[Set[Rule[Item]]] containing the assocation rules.
    *
    */
  @Since("1.5.0")
  def run[Item: ClassTag](freqItemsets: RDD[FreqItemset[Item]]): RDD[Rule[Item]] = {

    val sc = freqItemsets.sparkContext

    val freqItems = freqItemsets.filter(_.items.length == 1).flatMap(_.items).collect()

    val freqItemIndices = freqItemsets.mapPartitions {
      it =>
        val itemToRank = freqItems.zipWithIndex.toMap

        it.map {
          itemset =>
            val indices = itemset.items.flatMap(itemToRank.get).sorted.toSeq
            (indices, itemset.freq)
        }
    }.persist()

    val rules = AprioriRules.genRules(freqItemIndices, minConfidence, maxConsequent, numPartitions, log)

    freqItemIndices.unpersist()

    rules.mapPartitions {
      it =>
        it.map {
          case (antecendent, consequent, freqUnion, freqAntecedent) =>
            new Rule(antecendent.map(i => freqItems(i)).toArray,
              consequent.map(i => freqItems(i)).toArray,
              freqUnion, freqAntecedent)
        }
    }
  }


  /** Java-friendly version of [[run]]. */
  @Since("1.5.0")
  def run[Item](freqItemsets: JavaRDD[FreqItemset[Item]]): JavaRDD[Rule[Item]] = {
    val tag = fakeClassTag[Item]
    run(freqItemsets.rdd)(tag)
  }
}


object AprioriRules {

  /**
    * Computes the union seq.
    * @param freqItemIndices Frequent Items with Integer Indices.
    * @param minConfidence minConfidence.
    * @param maxConsequent maxConsequent.
    * @param numPartitions numPartitions.
    * @param log logger to output rule-generation information.
    * @return an ordered union Seq of s1 and s2.
    *
    */
  @Since("1.5.0")
  def genRules(freqItemIndices: RDD[(Seq[Int], Long)],
               minConfidence: Double,
               maxConsequent: Int,
               numPartitions: Int,
               log: Logger
              ): RDD[(Seq[Int], Seq[Int], Long, Long)] = {

    val sc = freqItemIndices.sparkContext

    val initCandidates = freqItemIndices.flatMap {
      case (indices, freq) =>
        indices.flatMap {
          index =>
            indices.partition(_ == index) match {
              case (consequent, antecendent) if antecendent.nonEmpty =>
                Some((antecendent, (consequent, freq)))
              case _ => None
            }
        }
    }

    val initRules = sc.emptyRDD[(Seq[Int], Seq[Int], Long, Long)]

    @tailrec
    def loop(candidates: RDD[(Seq[Int], (Seq[Int], Long))],
             lenConsequent: Int,
             rules: RDD[(Seq[Int], Seq[Int], Long, Long)]
            ): RDD[(Seq[Int], Seq[Int], Long, Long)] = {

      val numCandidates = candidates.count()

      log.info(s"Candidates for $lenConsequent-consequent rules : $numCandidates")

      if (numCandidates == 0 || lenConsequent > maxConsequent) rules
      else {
        val newRules = candidates.join(freqItemIndices).flatMap {
          case (antecendent, ((consequent, freqUnion), freqAntecedent))
            if freqUnion >= minConfidence * freqAntecedent =>
            Some(antecendent, consequent, freqUnion, freqAntecedent)

          case _ => None
        }.persist()

        val numNewRules = newRules.count()
        log.info(s"Generated $lenConsequent-consequent rules : $numNewRules")

        if (lenConsequent == maxConsequent) sc.union(rules, newRules)
        else {

          val newCandidates = newRules.flatMap {
            case (antecendent, consequent, freqUnion, freqAntecedent) if antecendent.size > 1 =>
              val union = seqAdd(antecendent, consequent)
              Some((union, freqUnion), consequent)

            case _ => None

          }.groupByKey(numPartitions).flatMap {

            case ((union, freqUnion), consequents) if consequents.size > 1 =>
              val array = consequents.toArray
              val newConsequents = collection.mutable.Set[Seq[Int]]()
              for (i <- 0 until array.length; j <- i + 1 until array.length) {
                val newConsequent = seqAdd(array(i), array(j))
                if (newConsequent.length == lenConsequent + 1) {
                  newConsequents.add(newConsequent)
                }
              }
              newConsequents.map {
                newConsequent =>
                  val newAntecendent = seqMinus(union, newConsequent)
                  (newAntecendent, (newConsequent, freqUnion))
              }

            case _ => None
          }

          loop(newCandidates, lenConsequent + 1, sc.union(rules, newRules))
        }
      }
    }

    loop(initCandidates, 1, initRules)
  }

  /**
    * Computes the union seq.
    * @param s1 ordered Seq1
    * @param s2 ordered Seq2
    * @return an ordered union Seq of s1 and s2.
    *
    */
  @Since("1.5.0")
  def seqAdd(s1: Seq[Int], s2: Seq[Int]): Seq[Int] = {
    var i1 = 0
    var i2 = 0

    val buffer = ArrayBuffer[Int]()

    while (i1 < s1.length && i2 < s2.length) {
      val e1 = s1(i1)
      val e2 = s2(i2)

      if (e1 == e2) {
        buffer.append(e1)
        i1 += 1
        i2 += 1
      } else if (e1 > e2) {
        buffer.append(e2)
        i2 += 1
      } else {
        buffer.append(e1)
        i1 += 1
      }
    }

    if (i1 < s1.length) {
      for (i <- i1 until s1.length)
        buffer.append(s1(i))
    } else if (i2 < s2.length) {
      for (i <- i2 until s2.length)
        buffer.append(s2(i))
    }

    buffer
  }

  /**
    * Computes the complementary seq.
    * @param s1 ordered Seq1
    * @param s2 ordered Seq2, must be a sub-sequence of s1
    * @return an ordered Seq, which equals to s1 -- s2.
    *
    */
  @Since("1.5.0")
  def seqMinus(s1: Seq[Int], s2: Seq[Int]): Seq[Int] = {
    var i1 = 0
    var i2 = 0

    val buffer = ArrayBuffer[Int]()

    while (i1 < s1.length && i2 < s2.length) {
      val e1 = s1(i1)
      val e2 = s2(i2)

      if (e1 == e2) {
        i1 += 1
        i2 += 1
      } else if (e1 < e2) {
        buffer.append(e1)
        i1 += 1
      } else {
        throw new SparkException(s"AprioriRules.seqMinus : ${s1.mkString(",")} is not a superset of ${s2.mkString(",")}")
      }
    }

    if (i1 < s1.length) {
      for (i <- i1 until s1.length)
        buffer.append(s1(i))
    } else if (i2 < s2.length) {
      throw new SparkException(s"AprioriRules.seqMinus : ${s1.mkString(",")} is not a superset of ${s2.mkString(",")}")
    }

    buffer
  }
}



