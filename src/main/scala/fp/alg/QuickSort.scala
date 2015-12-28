package fp.alg

/**
 * Created by jude on 15-11-25.
 */
import util.Random

object QuickSort extends App{
  def quick[A <% Ordered[A]](lst:Seq[A]):Seq[A] =lst match{
    // in case of a Seq with head and tail
    case (head) +: tail => {
      val (le, gt) = tail.partition( _ <= head)
      quick(le) ++ (head +: quick(gt))
    }
    // in case of only one element or empty
    case i => i
    }
  def quick2(lst:Seq[Int]):Seq[Int] ={
    val p = lst.head
    val (lt,gt) = lst.tail.partition(_ <= p)
    quick2(lt) ++ (p+: quick2(gt))
  }
  val shuffles = Random.shuffle( (1 to 50).toList)
  shuffles.foreach(i => print(s"${i}, "))
  println()
  assert(quick(shuffles) equals (1 to 50).toList , println("Error !"))
//  quick(shuffles).foreach(i => print(s"${i}, "))
}
