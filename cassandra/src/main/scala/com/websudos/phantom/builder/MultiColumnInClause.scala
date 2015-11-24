package com.websudos.phantom.builder

import com.websudos.phantom.builder.clauses.WhereClause

case class MultiColumnInClause(columns: List[String], values: List[List[_]]) {

  private val col = "(" + columns.mkString(",") + ")"
  private val valueStr = "(" + values.map("(" + _.mkString(",") + "_").mkString(",") + ")"

  val clause = new WhereClause.Condition(QueryBuilder.Where.in(col, valueStr))

}
