package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}

object SparkTransformer {

  def apply (session:SparkSession,sql:String)={
    val ast: LogicalPlan = session.sessionState.sqlParser.parsePlan(sql)


    matchAndPrint(ast)


    // Transform the AST.

  }
  def matchAndPrint(ast:LogicalPlan):Unit={
      ast match {
      case node: Project => {
        node.projectList.foreach(x => println(x))
      }
      case filt :Filter => {
        filt.condition.foreach(x => println("asdsa" + x))
        filt.constraints.foreach(println)
      }
      case x => println(x)

    }
    ast.children.foreach(x=>matchAndPrint(x))
  }
}
