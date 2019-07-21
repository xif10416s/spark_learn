package org.fxi.test.spark.sql.catalyst.trees

import org.apache.spark.sql.catalyst.trees.TreeNode

/**
  * Created by xifei on 16-9-1.
  */
object TreeNodeTest {
  def main(args: Array[String]): Unit = {
    val c1 =  DemoTreeNode("b",1)
    val c2 =  DemoTreeNode("c",1)
    val p1 = DemoTreeNode("a",1,Seq(c1,c2))

    p1.foreach( f=>{
      println(f.name)
    })

    p1.foreachUp( f=>{
      println(f.name)
    })

    p1.collect(new PartialFunction[DemoTreeNode,DemoTreeNode] {
      override def isDefinedAt(x: DemoTreeNode): Boolean = ???

      override def apply(v1: DemoTreeNode): DemoTreeNode = ???
    })
  }
}

case class DemoTreeNode(name:String,age:Int ,childs:Seq[DemoTreeNode] = Seq.empty) extends TreeNode[DemoTreeNode]{
  override def children: Seq[DemoTreeNode] = childs



  override def verboseString: String = super.simpleString
}
