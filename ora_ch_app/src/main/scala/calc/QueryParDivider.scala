package calc

import scala.collection.mutable

object QueryParDivider {

  private def listToQueue[A](l: List[A]): mutable.Queue[A] = new mutable.Queue[A] ++= l

  def mapOfQueues(queries: List[Query]): Map[Int, mutable.Queue[Query]] =
    queries.groupBy(_.query_id).map { case (k: Int, lst: List[Query]) =>
      (k, listToQueue(lst))
    }

  def listOfListsQuery(
                        m: Map[Int, mutable.Queue[Query]],
                        acc: List[List[Query]] = List.empty[List[Query]]
                      ): List[List[Query]] =
    if (m.exists(_._2.nonEmpty)) {
      if (m.count(_._2.nonEmpty) == 1) {
        val nonemptyQueue = m.find(_._2.nonEmpty)
        nonemptyQueue match {
          case Some((key, queue)) =>
            val element: Query = queue.dequeue()
            listOfListsQuery(Map(key -> queue), acc :+ List(element))
          case None               => acc
        }
      } else {
        val keys: List[Int]          = m.filter(_._2.nonEmpty).keys.toList
        val k1: Int                  = keys.head
        val k2: Int                  = keys.tail.head
        val q1: mutable.Queue[Query] = m.getOrElse(k1, mutable.Queue.empty[Query])
        val q2: mutable.Queue[Query] = m.getOrElse(k2, mutable.Queue.empty[Query])
        val query1                   = q1.dequeue()
        val query2                   = q2.dequeue()
        listOfListsQuery(m.updated(k1, q1).updated(k2, q2), acc :+ List(query1, query2))
      }
    } else
      acc

}
