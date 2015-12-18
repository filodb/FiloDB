package filodb.cli

object Dummy {

  val orders = "tenant_id: string, year: int, month: int, week_of_month: int, ticket_id: string, date_sent: int, avg_timebw_inbnord: bigint, basketid: string, destination: string, inbound_ticketid: string, order_id: string, order_price: double, order_quantity: double, order_sent_date: string, order_sent_date_df: bigint, order_type: double, parent_quantity: double, side: int, stargate: string, symbol: string, tif: string, time_sent: string, trader_id: string, segmentkey: string, partkey: string"
  val executions = "tenant_id: string, year: int, month: int, week_of_month: int, ticket_id: string, execution_id: int, date_recvd: int, exe_quantity: double, execution_price: double, execution_type: int, inbound_ticketid: string, received_date: string, received_date_df: bigint, stargate: string, time_recvd: string, segmentkey: string, partkey: string"

  def main(args: Array[String]) {

    println("val orderSchema = \n Seq("+makeSchemaString("orders",orders)+")")
    println("\n")
    println("val executionsSchema = \n Seq("+makeSchemaString("executions",executions)+")")
  }

  def makeSchemaString(t: String, str: String) = {
    val all = str.split(",")
    all.map { c =>
      val colAndTypeStr = c.split(": ")
      val colName = colAndTypeStr(0)
      val colTypeStr = colAndTypeStr(1)
      val colType = colTypeStr.toLowerCase match {
        case "int" => "Column.ColumnType.IntColumn"
        case "bigint" => "Column.ColumnType.LongColumn"
        case "double" => "Column.ColumnType.DoubleColumn"
        case "string" => "Column.ColumnType.StringColumn"
      }
      s"""Column(\"${colName.trim}\", \"${t}\", 0, $colType)"""
    }.mkString(",\n")
  }
}
