package zimo.algorithms.AuthorityAndHub

/**
  * Created by wzp on 17-02-20.
  */
/**
  *  srcId:节点id
  *  authority:authority值
  *  hub:hub值
  *  lastAuth:上一轮迭代authority值
  *  lastHub: 上一轮迭代hub值
  */
class VertexType(val srcId: Any, val value: (Double, Double, Double, Double)) extends Serializable{
  override def toString: String = srcId + "\t" + value.toString()
}
