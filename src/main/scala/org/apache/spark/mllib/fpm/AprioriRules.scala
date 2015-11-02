package org.apache.spark.mllib.fpm

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.Logging
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
 * Generates association rules from a [[RDD[FreqItemset[Item]]]. This method only generates
 * association rules which have a single item as the consequent.
 *
 */
@Since("1.5.0")
@Experimental
class AprioriRules private[fpm](private var minConfidence: Double)
  extends AssociationRules {

  /**
   * Computes the association rules with confidence above [[minConfidence]].
   * @param freqItemsets frequent itemset model obtained from [[FPGrowth]]
   * @return a [[Set[Rule[Item]]] containing the assocation rules.
   *
   */
  @Since("1.5.0")
  override def run[Item: ClassTag](freqItemsets: RDD[FreqItemset[Item]]): RDD[Rule[Item]] = {

    val sc = freqItemsets.sparkContext

    var rules: RDD[(Set[Item], Set[Item], Long, Long)] = sc.emptyRDD[(Set[Item], Set[Item], Long, Long)]

    val freqs: RDD[(Set[Item], Long)] =
      freqItemsets.map {
        itemset =>
          (itemset.items.toSet, itemset.freq)
      }

    //antecedent, (consequent, freqUnion)
    var candidates: RDD[(Set[Item], (Set[Item], Long))] =
      freqItemsets.flatMap {
        itemset =>
          val items = itemset.items.toSet
          items.flatMap {
            item =>
              items.partition(_ == item) match {
                case (consequent, antecendent) if antecendent.nonEmpty =>
                  Some((antecendent.toSet, (consequent.toSet, itemset.freq)))
                case _ => None
              }
          }
      }

    var lenConsequent = 1

    var hasCandidates = !candidates.isEmpty()

    while (hasCandidates) {

      val tmpRules = candidates.join(freqs).flatMap {
        case (antecendent, ((consequent, freqUnion), freqAntecedent)) =>
          if (freqUnion >= minConfidence * freqAntecedent)
            Some(antecendent, consequent, freqUnion, freqAntecedent)
          else None
      }

      rules = sc.union(rules, tmpRules)

      candidates = tmpRules.map {
        case (antecendent, consequent, freqUnion, freqAntecedent) =>
          ((antecendent ++ consequent, freqUnion), consequent)
      }.groupByKey().flatMap {
        case ((union: Set[Item], freqUnion: Long), consequents: Iterable[Set[Item]]) =>
          val array = consequents.toArray
          if (array.length > 1) {
            val buffer = ArrayBuffer[(Set[Item], (Set[Item], Long))]()
            for (i <- 0 until array.length; j <- i + 1 until array.length) {
              val intersect = array(i) & array(j)
              if (intersect.size == lenConsequent - 1) {
                val newConsequent = array(i) ++ array(j)
                val newAntecendent = union -- newConsequent
                if (newAntecendent.nonEmpty)
                  buffer.append((newAntecendent, (newConsequent, freqUnion)))
              }
            }
            buffer
          } else None
      }

      hasCandidates = !candidates.isEmpty()

      lenConsequent += 1
    }

    rules.map {
      case (antecendent, consequent, freqUnion, freqAntecedent) =>
        new Rule(antecendent.toArray, consequent.toArray, freqUnion, freqAntecedent)
    }
  }
}
