package org.apache.spark.mllib.fpm

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.rdd.{EmptyRDD, RDD}

/**
  * Created by zrf on 16/2/18.
  */
class AprioriRulesII private[fpm](
                                   private var minConfidence: Double,
                                   private var maxConsequent: Int = 1)
  extends Serializable with Logging {


  /**
    * Constructs a default instance with default parameters {minConfidence = 0.8}.
    */
  @Since("1.5.0")
  def this() = this(0.8, 1)

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
    * Sets the maximum size of consequents used by AprioriRules (default: 1).
    */
  @Since("1.5.0")
  def setMaxConsequent(maxConsequent: Int): this.type = {
    this.maxConsequent = maxConsequent
    this
  }


  /**
    * Computes the association rules with confidence above [[minConfidence]].
    *
    * @param freqItemsets frequent itemset model obtained from [[FPGrowth]]
    * @return a [[Set[Rule[Item]]] containing the assocation rules.
    *
    */
  @Since("1.5.0")
  def run[Item: ClassTag](freqItemsets: RDD[FreqItemset[Item]]): RDD[Rule[Item]] = {

    val sc = freqItemsets.sparkContext

    val initCandidates = freqItemsets.flatMap {
      itemset =>
        itemset.items.flatMap {
          item =>
            itemset.items.partition(_ == item) match {
              case (consequent, antecendent) if antecendent.nonEmpty =>
                Some((antecendent.toSet, (consequent.toSet, itemset.freq)))
              case _ => None
            }
        }
    }

    val initRules = sc.emptyRDD[(Set[Item], Set[Item], Long, Long)]

    val setAndFreq = freqItemsets.map {
      itemset =>
        (itemset.items.toSet, itemset.freq)
    }

    @tailrec
    def genRules(candidates: RDD[(Set[Item], (Set[Item], Long))],
                 lenConsequent: Int,
                 rules: RDD[(Set[Item], Set[Item], Long, Long)]
                ): RDD[(Set[Item], Set[Item], Long, Long)] = {

      val numCandidates = candidates.count()
      log.info(s"Candidates for ${lenConsequent}-consequent rules : ${numCandidates}")

      if (numCandidates == 0 || lenConsequent > maxConsequent) {
        rules
      } else {
        val newRules = candidates.join(setAndFreq).flatMap {
          case (antecendent, ((consequent, freqUnion), freqAntecedent))
            if freqUnion >= minConfidence * freqAntecedent =>
            Some(antecendent, consequent, freqUnion, freqAntecedent)

          case _ => None
        }.cache()

        val numNewRules = newRules.count()
        log.info(s"Generated ${lenConsequent}-consequent rules : ${numNewRules}")

        if (lenConsequent == maxConsequent) {
          sc.union(rules, newRules)
        } else {
          val newCandidates = newRules.flatMap {
            case (antecendent, consequent, freqUnion, freqAntecedent) if antecendent.size > 1 =>
              val union = antecendent ++ consequent
              Some((union, freqUnion), consequent)
            case _ => None
          }.groupByKey().flatMap {
            case ((union, freqUnion), consequents) if consequents.size > 1 =>
              val arr = consequents.toArray
              val newConsequents = collection.mutable.Set[Set[Item]]()
              for (i <- 0 until arr.length; j <- i + 1 until arr.length) {
                val newConsequent = arr(i) ++ arr(j)
                if (newConsequent.size == lenConsequent + 1) {
                  newConsequents.add(newConsequent)
                }
              }

              newConsequents.map {
                newConsequent =>
                  val newAntecendent = union -- newConsequent
                  (newAntecendent, (newConsequent, freqUnion))
              }

            case _ => None
          }

          genRules(newCandidates, lenConsequent + 1, sc.union(rules, newRules))
        }
      }
    }


    val rules = genRules(initCandidates, 1, initRules)

    rules.map {
      case (antecendent, consequent, freqUnion, freqAntecedent) =>
        new Rule(antecendent.toArray,
          consequent.toArray,
          freqUnion.toDouble,
          freqAntecedent.toDouble)
    }
  }


  /** Java-friendly version of [[run]]. */
  @Since("1.5.0")
  def run[Item](freqItemsets: JavaRDD[FreqItemset[Item]]): JavaRDD[Rule[Item]] = {
    val tag = fakeClassTag[Item]
    run(freqItemsets.rdd)(tag)
  }
}



