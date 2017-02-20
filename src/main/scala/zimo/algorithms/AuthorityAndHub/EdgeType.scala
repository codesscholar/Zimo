package zimo.algorithms.AuthorityAndHub

/**
  * Created by wangzhiping on 16-11-18.
  */
class EdgeType(val src: Any, val dst: Any) extends Serializable{
  override def toString: String = src + "\t" + dst
}
